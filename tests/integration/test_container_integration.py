import importlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import grpc
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
PROTO_FILE = REPO_ROOT / "proto" / "schema_registry.proto"


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def generated_modules():
    if shutil.which("docker") is None:
        pytest.skip("docker is required for integration tests")

    with tempfile.TemporaryDirectory(prefix="grpc_proto_gen_") as tmp_dir:
        out_dir = Path(tmp_dir)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{REPO_ROOT / 'proto'}",
                f"--python_out={out_dir}",
                f"--grpc_python_out={out_dir}",
                str(PROTO_FILE),
            ],
            check=True,
            cwd=REPO_ROOT,
        )

        sys.path.insert(0, str(out_dir))
        try:
            pb2 = importlib.import_module("schema_registry_pb2")
            pb2_grpc = importlib.import_module("schema_registry_pb2_grpc")
            yield pb2, pb2_grpc
        finally:
            sys.path.remove(str(out_dir))


@pytest.fixture(scope="session")
def running_container():
    image_tag = "schema-registry-hw:local"
    container_name = f"schema-registry-hw-{int(time.time())}"
    host_port = _free_tcp_port()

    subprocess.run(
        ["docker", "build", "-t", image_tag, "."],
        check=True,
        cwd=REPO_ROOT,
    )

    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "-p",
            f"{host_port}:50051",
            image_tag,
        ],
        check=True,
        cwd=REPO_ROOT,
    )

    target = f"127.0.0.1:{host_port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        channel = grpc.insecure_channel(target)
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
            channel.close()
            break
        except grpc.FutureTimeoutError:
            channel.close()
            time.sleep(0.5)
    else:
        logs = subprocess.run(
            ["docker", "logs", container_name],
            check=False,
            capture_output=True,
            text=True,
        )
        pytest.fail(f"container did not become ready in time\n{logs.stdout}\n{logs.stderr}")

    try:
        yield target
    finally:
        subprocess.run(["docker", "stop", container_name], check=False)


def _schema(pb2, fields):
    return pb2.Schema(
        structs=[
            pb2.Struct(
                name="User",
                fields=fields,
            )
        ]
    )


def test_register_initial_and_optional_field_addition(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="name", type=pb2.FIELD_TYPE_STRING, required=False),
        ],
    )
    res1 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="user-service", schema=v1))
    assert res1.accepted is True
    assert res1.version == 1

    v2 = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="name", type=pb2.FIELD_TYPE_STRING, required=False),
            pb2.Field(id=3, name="email", type=pb2.FIELD_TYPE_STRING, required=False),
        ],
    )
    res2 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="user-service", schema=v2))
    assert res2.accepted is True
    assert res2.version == 2

    latest = stub.GetLatestVersion(pb2.GetLatestVersionRequest(service_name="user-service"))
    assert latest.version == 2


def test_reject_added_required_field(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    res1 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="order-service", schema=v1))
    assert res1.accepted is True
    assert res1.version == 1

    incompatible = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="status", type=pb2.FIELD_TYPE_STRING, required=True),
        ],
    )
    res2 = stub.RegisterSchema(
        pb2.RegisterSchemaRequest(service_name="order-service", schema=incompatible)
    )
    assert res2.accepted is False
    assert res2.version == 1
    assert any(issue.code == pb2.ADDED_REQUIRED_FIELD for issue in res2.issues)


def test_reject_field_type_change(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="counter", type=pb2.FIELD_TYPE_I32, required=True)],
    )
    res1 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="counter-service", schema=v1))
    assert res1.accepted is True

    incompatible = _schema(
        pb2,
        [pb2.Field(id=1, name="counter", type=pb2.FIELD_TYPE_STRING, required=True)],
    )
    check = stub.CheckCompatibility(
        pb2.CheckCompatibilityRequest(
            service_name="counter-service",
            base_version=1,
            candidate_schema=incompatible,
        )
    )
    assert check.compatible is False
    assert any(issue.code == pb2.FIELD_TYPE_CHANGED for issue in check.issues)


def test_reject_optional_to_required(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="nickname", type=pb2.FIELD_TYPE_STRING, required=False)],
    )
    res1 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="profile-service", schema=v1))
    assert res1.accepted is True

    incompatible = _schema(
        pb2,
        [pb2.Field(id=1, name="nickname", type=pb2.FIELD_TYPE_STRING, required=True)],
    )
    res2 = stub.RegisterSchema(
        pb2.RegisterSchemaRequest(service_name="profile-service", schema=incompatible)
    )
    assert res2.accepted is False
    assert any(issue.code == pb2.OPTIONAL_TO_REQUIRED for issue in res2.issues)


def test_allow_add_new_struct_with_required_fields(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = pb2.Schema(
        structs=[
            pb2.Struct(
                name="User",
                fields=[pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
            )
        ]
    )
    res1 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="multi-struct-service", schema=v1))
    assert res1.accepted is True
    assert res1.version == 1

    v2 = pb2.Schema(
        structs=[
            pb2.Struct(
                name="User",
                fields=[pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
            ),
            pb2.Struct(
                name="Order",
                fields=[pb2.Field(id=1, name="order_id", type=pb2.FIELD_TYPE_I64, required=True)],
            ),
        ]
    )
    res2 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="multi-struct-service", schema=v2))
    assert res2.accepted is True
    assert res2.version == 2


def test_allow_remove_optional_field(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="nickname", type=pb2.FIELD_TYPE_STRING, required=False),
        ],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="remove-optional", schema=v1)).accepted

    v2 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    res2 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="remove-optional", schema=v2))
    assert res2.accepted is True
    assert res2.version == 2


def test_reject_removed_required_field(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="status", type=pb2.FIELD_TYPE_STRING, required=True),
        ],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="remove-required", schema=v1)).accepted

    v2 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    res2 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="remove-required", schema=v2))
    assert res2.accepted is False
    assert any(issue.code == pb2.REMOVED_REQUIRED_FIELD for issue in res2.issues)


def test_allow_required_to_optional(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="nickname", type=pb2.FIELD_TYPE_STRING, required=True)],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="relax-required", schema=v1)).accepted

    v2 = _schema(
        pb2,
        [pb2.Field(id=1, name="nickname", type=pb2.FIELD_TYPE_STRING, required=False)],
    )
    res2 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="relax-required", schema=v2))
    assert res2.accepted is True
    assert res2.version == 2


def test_allow_reordered_fields(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="name", type=pb2.FIELD_TYPE_STRING, required=False),
        ],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="reorder-fields", schema=v1)).accepted

    v2 = _schema(
        pb2,
        [
            pb2.Field(id=2, name="name", type=pb2.FIELD_TYPE_STRING, required=False),
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
        ],
    )
    res2 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="reorder-fields", schema=v2))
    assert res2.accepted is True
    assert res2.version == 2


def test_reject_field_id_changed(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="nickname", type=pb2.FIELD_TYPE_STRING, required=False)],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="field-id-change", schema=v1)).accepted

    v2 = _schema(
        pb2,
        [pb2.Field(id=7, name="nickname", type=pb2.FIELD_TYPE_STRING, required=False)],
    )
    res2 = stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="field-id-change", schema=v2))
    assert res2.accepted is False
    assert any(issue.code == pb2.FIELD_ID_CHANGED for issue in res2.issues)


def test_reject_duplicate_field_id(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="dup-field-id", schema=v1)).accepted

    v2 = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=1, name="id2", type=pb2.FIELD_TYPE_STRING, required=False),
        ],
    )
    check = stub.CheckCompatibility(
        pb2.CheckCompatibilityRequest(
            service_name="dup-field-id",
            base_version=1,
            candidate_schema=v2,
        )
    )
    assert check.compatible is False
    assert any(issue.code == pb2.DUPLICATE_FIELD_ID for issue in check.issues)


def test_reject_duplicate_field_name(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="dup-field-name", schema=v1)).accepted

    v2 = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="id", type=pb2.FIELD_TYPE_STRING, required=False),
        ],
    )
    check = stub.CheckCompatibility(
        pb2.CheckCompatibilityRequest(
            service_name="dup-field-name",
            base_version=1,
            candidate_schema=v2,
        )
    )
    assert check.compatible is False
    assert any(issue.code == pb2.DUPLICATE_FIELD_NAME for issue in check.issues)


def test_reject_duplicate_struct_name(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="dup-struct-name", schema=v1)).accepted

    v2 = pb2.Schema(
        structs=[
            pb2.Struct(
                name="User",
                fields=[pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
            ),
            pb2.Struct(
                name="User",
                fields=[pb2.Field(id=2, name="name", type=pb2.FIELD_TYPE_STRING, required=False)],
            ),
        ]
    )
    check = stub.CheckCompatibility(
        pb2.CheckCompatibilityRequest(
            service_name="dup-struct-name",
            base_version=1,
            candidate_schema=v2,
        )
    )
    assert check.compatible is False
    assert any(issue.code == pb2.DUPLICATE_STRUCT_NAME for issue in check.issues)


def test_rpc_errors_for_invalid_arguments(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    with pytest.raises(grpc.RpcError) as err_empty_name:
        stub.GetLatestVersion(pb2.GetLatestVersionRequest(service_name="   "))
    assert err_empty_name.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as err_not_found:
        stub.GetSchema(pb2.GetSchemaRequest(service_name="no-such-service", version=1))
    assert err_not_found.value.code() == grpc.StatusCode.NOT_FOUND

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="invalid-base-version", schema=v1)).accepted

    with pytest.raises(grpc.RpcError) as err_bad_base:
        stub.CheckCompatibility(
            pb2.CheckCompatibilityRequest(
                service_name="invalid-base-version",
                base_version=0,
                candidate_schema=v1,
            )
        )
    assert err_bad_base.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_get_schema_zero_returns_latest(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    v2 = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="tag", type=pb2.FIELD_TYPE_STRING, required=False),
        ],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="latest-schema", schema=v1)).accepted
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="latest-schema", schema=v2)).accepted

    latest = stub.GetSchema(pb2.GetSchemaRequest(service_name="latest-schema", version=0))
    assert latest.version == 2
    assert len(latest.schema.structs[0].fields) == 2


def test_get_schema_invalid_version_errors(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="schema-version-errors", schema=v1)).accepted

    with pytest.raises(grpc.RpcError) as err_negative:
        stub.GetSchema(pb2.GetSchemaRequest(service_name="schema-version-errors", version=-1))
    assert err_negative.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as err_too_large:
        stub.GetSchema(pb2.GetSchemaRequest(service_name="schema-version-errors", version=9))
    assert err_too_large.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_check_compatibility_does_not_persist(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="check-no-persist", schema=v1)).accepted

    compatible_candidate = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="note", type=pb2.FIELD_TYPE_STRING, required=False),
        ],
    )
    check = stub.CheckCompatibility(
        pb2.CheckCompatibilityRequest(
            service_name="check-no-persist",
            base_version=1,
            candidate_schema=compatible_candidate,
        )
    )
    assert check.compatible is True

    latest = stub.GetLatestVersion(pb2.GetLatestVersionRequest(service_name="check-no-persist"))
    assert latest.version == 1


def test_register_rejection_keeps_latest_version(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True)],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="reject-keeps-version", schema=v1)).accepted

    incompatible = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="status", type=pb2.FIELD_TYPE_STRING, required=True),
        ],
    )
    rejected = stub.RegisterSchema(
        pb2.RegisterSchemaRequest(service_name="reject-keeps-version", schema=incompatible)
    )
    assert rejected.accepted is False
    assert rejected.version == 1

    latest = stub.GetLatestVersion(pb2.GetLatestVersionRequest(service_name="reject-keeps-version"))
    assert latest.version == 1


def test_multiple_issues_reported_together(generated_modules, running_container):
    pb2, pb2_grpc = generated_modules
    channel = grpc.insecure_channel(running_container)
    stub = pb2_grpc.SchemaRegistryStub(channel)

    v1 = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_I64, required=True),
            pb2.Field(id=2, name="name", type=pb2.FIELD_TYPE_STRING, required=False),
        ],
    )
    assert stub.RegisterSchema(pb2.RegisterSchemaRequest(service_name="multi-issues", schema=v1)).accepted

    incompatible = _schema(
        pb2,
        [
            pb2.Field(id=1, name="id", type=pb2.FIELD_TYPE_STRING, required=True),
            pb2.Field(id=5, name="name", type=pb2.FIELD_TYPE_STRING, required=True),
        ],
    )
    check = stub.CheckCompatibility(
        pb2.CheckCompatibilityRequest(
            service_name="multi-issues",
            base_version=1,
            candidate_schema=incompatible,
        )
    )
    assert check.compatible is False
    codes = {issue.code for issue in check.issues}
    assert pb2.FIELD_TYPE_CHANGED in codes
    assert pb2.FIELD_ID_CHANGED in codes
    assert pb2.ADDED_REQUIRED_FIELD in codes
