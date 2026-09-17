import os
from concurrent import futures

import grpc

import schema_registry_pb2
import schema_registry_pb2_grpc


class SchemaRegistryService(schema_registry_pb2_grpc.SchemaRegistryServicer):
    def __init__(self):
        # service_name -> list of schemas by version
        self.schemas = {}

    @staticmethod
    def _issue(code, message, struct_name="", field_id=0):
        return schema_registry_pb2.CompatibilityIssue(
            code=code,
            message=message,
            struct_name=struct_name,
            field_id=field_id,
        )

    @staticmethod
    def _validate_schema(schema):
        issues = []

        struct_names = set()

        for struct in schema.structs:
            if struct.name in struct_names:
                issues.append(
                    SchemaRegistryService._issue(
                        schema_registry_pb2.DUPLICATE_STRUCT_NAME,
                        f"Duplicate struct name: {struct.name}",
                        struct.name,
                    )
                )
            struct_names.add(struct.name)

            field_ids = set()
            field_names = set()

            for field in struct.fields:
                if field.type == schema_registry_pb2.FIELD_TYPE_UNSPECIFIED:
                    issues.append(
                        SchemaRegistryService._issue(
                            schema_registry_pb2.FIELD_TYPE_CHANGED,
                            f"Field {field.name} has unspecified type",
                            struct.name,
                            field.id,
                        )
                    )

                if field.id in field_ids:
                    issues.append(
                        SchemaRegistryService._issue(
                            schema_registry_pb2.DUPLICATE_FIELD_ID,
                            f"Duplicate field id: {field.id}",
                            struct.name,
                            field.id,
                        )
                    )
                field_ids.add(field.id)

                if field.name in field_names:
                    issues.append(
                        SchemaRegistryService._issue(
                            schema_registry_pb2.DUPLICATE_FIELD_NAME,
                            f"Duplicate field name: {field.name}",
                            struct.name,
                            field.id,
                        )
                    )
                field_names.add(field.name)

        return issues

    @staticmethod
    def _check_compatibility(old_schema, new_schema):
        issues = []

        old_structs = {struct.name: struct for struct in old_schema.structs}
        new_structs = {struct.name: struct for struct in new_schema.structs}

        for struct_name, old_struct in old_structs.items():
            new_struct = new_structs.get(struct_name)

            if new_struct is None:
                continue

            old_by_id = {field.id: field for field in old_struct.fields}
            new_by_id = {field.id: field for field in new_struct.fields}

            old_by_name = {field.name: field for field in old_struct.fields}
            new_by_name = {field.name: field for field in new_struct.fields}

            # Проверяем изменение ID поля по его имени.
            for field_name, old_field in old_by_name.items():
                new_field = new_by_name.get(field_name)

                if new_field is not None and old_field.id != new_field.id:
                    issues.append(
                        SchemaRegistryService._issue(
                            schema_registry_pb2.FIELD_ID_CHANGED,
                            (
                                f"Field '{field_name}' changed id "
                                f"from {old_field.id} to {new_field.id}"
                            ),
                            struct_name,
                            new_field.id,
                        )
                    )

            # Проверяем поля, существующие в обеих версиях.
            for field_id, old_field in old_by_id.items():
                new_field = new_by_id.get(field_id)

                if new_field is None:
                    # Удаление required-поля запрещено.
                    if old_field.required:
                        issues.append(
                            SchemaRegistryService._issue(
                                schema_registry_pb2.REMOVED_REQUIRED_FIELD,
                                f"Required field {field_id} was removed",
                                struct_name,
                                field_id,
                            )
                        )
                    continue

                # Изменение типа запрещено.
                if old_field.type != new_field.type:
                    issues.append(
                        SchemaRegistryService._issue(
                            schema_registry_pb2.FIELD_TYPE_CHANGED,
                            (
                                f"Field {field_id} type changed "
                                f"from {old_field.type} to {new_field.type}"
                            ),
                            struct_name,
                            field_id,
                        )
                    )

                # optional -> required запрещено.
                if not old_field.required and new_field.required:
                    issues.append(
                        SchemaRegistryService._issue(
                            schema_registry_pb2.OPTIONAL_TO_REQUIRED,
                            f"Field {field_id} changed from optional to required",
                            struct_name,
                            field_id,
                        )
                    )

            # Проверяем новые поля.
            for field_id, new_field in new_by_id.items():
                if field_id not in old_by_id:
                    # Добавление required-поля запрещено.
                    if new_field.required:
                        issues.append(
                            SchemaRegistryService._issue(
                                schema_registry_pb2.ADDED_REQUIRED_FIELD,
                                f"Required field {field_id} was added",
                                struct_name,
                                field_id,
                            )
                        )

        return issues

    def RegisterSchema(self, request, context):
        service_name = request.service_name
        candidate = request.schema

        if not service_name.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("service_name must not be empty")
            return schema_registry_pb2.RegisterSchemaResponse()

        validation_issues = self._validate_schema(candidate)

        if validation_issues:
            current_version = len(self.schemas.get(service_name, []))

            return schema_registry_pb2.RegisterSchemaResponse(
                accepted=False,
                version=current_version,
                issues=validation_issues,
            )

        versions = self.schemas.setdefault(service_name, [])

        # Первая версия.
        if not versions:
            versions.append(candidate)

            return schema_registry_pb2.RegisterSchemaResponse(
                accepted=True,
                version=1,
                issues=[],
            )

        # Проверяем совместимость с последней версией.
        latest = versions[-1]
        issues = self._check_compatibility(latest, candidate)

        if issues:
            return schema_registry_pb2.RegisterSchemaResponse(
                accepted=False,
                version=len(versions),
                issues=issues,
            )

        versions.append(candidate)

        return schema_registry_pb2.RegisterSchemaResponse(
            accepted=True,
            version=len(versions),
            issues=[],
        )

    def CheckCompatibility(self, request, context):
        service_name = request.service_name
        base_version = request.base_version
        candidate = request.candidate_schema

        if not service_name.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("service_name must not be empty")
            return schema_registry_pb2.CheckCompatibilityResponse()

        if base_version <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("base_version must be positive")
            return schema_registry_pb2.CheckCompatibilityResponse()

        versions = self.schemas.get(service_name)

        if not versions:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(
                f"Service '{service_name}' does not exist"
            )
            return schema_registry_pb2.CheckCompatibilityResponse()

        if base_version > len(versions):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(
                f"Version {base_version} does not exist"
            )
            return schema_registry_pb2.CheckCompatibilityResponse()

        issues = self._validate_schema(candidate)

        if not issues:
            base_schema = versions[base_version - 1]
            issues = self._check_compatibility(
                base_schema,
                candidate,
            )

        return schema_registry_pb2.CheckCompatibilityResponse(
            compatible=len(issues) == 0,
            issues=issues,
        )

    def GetSchema(self, request, context):
        service_name = request.service_name
        version = request.version

        if not service_name.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("service_name must not be empty")
            return schema_registry_pb2.GetSchemaResponse()

        if version < 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("version must be non-negative")
            return schema_registry_pb2.GetSchemaResponse()

        versions = self.schemas.get(service_name)

        if not versions:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(
                f"Service '{service_name}' does not exist"
            )
            return schema_registry_pb2.GetSchemaResponse()

        # version=0 означает последнюю версию.
        if version == 0:
            version = len(versions)

        if version > len(versions):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Version {version} does not exist"
            )
            return schema_registry_pb2.GetSchemaResponse()

        return schema_registry_pb2.GetSchemaResponse(
            version=version,
            schema=versions[version - 1],
        )

    def GetLatestVersion(self, request, context):
        service_name = request.service_name

        if not service_name.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("service_name must not be empty")
            return schema_registry_pb2.GetLatestVersionResponse()

        versions = self.schemas.get(service_name)

        if not versions:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(
                f"Service '{service_name}' does not exist"
            )
            return schema_registry_pb2.GetLatestVersionResponse()

        return schema_registry_pb2.GetLatestVersionResponse(
            version=len(versions)
        )


def serve():
    port = int(os.getenv("PORT", "50051"))

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    schema_registry_pb2_grpc.add_SchemaRegistryServicer_to_server(
        SchemaRegistryService(),
        server,
    )

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Schema Registry server started on port {port}")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()