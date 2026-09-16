import os
from concurrent import futures
from dataclasses import dataclass

import grpc

import schema_registry_pb2
import schema_registry_pb2_grpc


class SchemaRegistryService(schema_registry_pb2_grpc.SchemaRegistryServicer):
    def __init__(self):
        pass

    def RegisterSchema(self, request, context):
        pass

    def CheckCompatibility(self, request, context):
        pass

    def GetSchema(self, request, context):
        pass

    def GetLatestVersion(self, request, context):
        pass


def serve():
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    schema_registry_pb2_grpc.add_SchemaRegistryServicer_to_server(
        SchemaRegistryService(),
        server,
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()