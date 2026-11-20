from pathlib import Path

from grpc_tools import protoc

ROOT = Path(__file__).resolve().parent.parent
PROTO_ROOT = ROOT / "protos"


def main() -> int:
    protos = [str(path) for path in sorted(PROTO_ROOT.rglob("*.proto"))]
    return protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{PROTO_ROOT}",
            f"--python_out={ROOT}",
            f"--pyi_out={ROOT}",
            f"--grpc_python_out={ROOT}",
            *protos,
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
