#!/usr/bin/env python3
"""
Genera los stubs gRPC de catalogo en los servicios que los necesitan.

Ejecutar desde la raíz del proyecto (mpi-microservicios/):
    pip install grpcio-tools
    python generate_stubs.py
"""
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).parent
PROTO_FILE = ROOT / "proto" / "catalogo.proto"

# Servicios que importan stubs de catálogo
TARGETS = [
    ROOT / "catalogo",
    ROOT / "pedidos",
]

if not PROTO_FILE.exists():
    print(f"ERROR: no se encontró {PROTO_FILE}", file=sys.stderr)
    sys.exit(1)

for target in TARGETS:
    target.mkdir(exist_ok=True)
    print(f"Generando stubs en {target} ...")
    subprocess.run(
        [
            sys.executable, "-m", "grpc_tools.protoc",
            f"--python_out={target}",
            f"--grpc_python_out={target}",
            f"-I{PROTO_FILE.parent}",
            str(PROTO_FILE),
        ],
        check=True,
    )
    print(f"  ✓ {target / 'catalogo_pb2.py'}")
    print(f"  ✓ {target / 'catalogo_pb2_grpc.py'}")

print("\nStubs generados. Para regenerar: python generate_stubs.py")
