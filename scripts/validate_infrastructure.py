"""Valida Compose sin motor, builds, red ni servicios. Solo usa biblioteca estandar."""
import ast
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SERVICES = {
    "mongodb", "spark-master", "spark-worker", "dask-scheduler",
    "dask-worker-1", "dask-worker-2", "api", "jenkins",
}
FORBIDDEN_HOST_PORTS = {27017, 27018, 5000, 8080, 8081, 8787}


def load_model(root=ROOT, include_e2=False, include_streaming=False):
    # config analiza archivos; no consulta ni inicia el motor Docker.
    environment = os.environ.copy()
    for key in (
        "API_PORT", "SPARK_UI_PORT", "DASK_UI_PORT", "JENKINS_PORT",
        "MONGO_ROOT_PASSWORD_FILE", "MONGO_WRITER_PASSWORD_FILE",
        "MONGO_READER_PASSWORD_FILE", "DEPLOYED_COMMIT", "COMPOSE_PROJECT_NAME",
    ):
        environment.pop(key, None)
    result = subprocess.run(
        ["docker", "compose", *( ['--profile', 'e2'] if include_e2 or include_streaming else [] ),
         *( ['--profile', 'streaming'] if include_streaming else [] ),
         "--project-name", "lifemap", "--env-file",
         str(root / ".env.example"), "-f", str(root / "docker-compose.yml"),
         "config", "--format", "json"],
        cwd=root, env=environment, text=True, capture_output=True,
        encoding="utf-8", timeout=30, check=True,
    )
    return json.loads(result.stdout)


def is_inside(path, parent):
    return path.resolve().is_relative_to(parent.resolve())


def validate_model(model, root=ROOT):
    errors = []
    services = model.get("services", {})
    if model.get("name") != "lifemap" or set(services) not in (SERVICES, SERVICES | {'kafka'}, SERVICES | {'kafka', 'streaming'}):
        errors.append("Expected eight E1 services and optional E2 Kafka/streaming")
    total_memory = 0
    published = []
    for name, service in services.items():
        if not service.get("image", "").startswith("lifemap-") and not (
            name == 'kafka' and re.fullmatch(r'apache/kafka:4\.1\.2@sha256:[0-9a-f]{64}', service.get('image', ''))
        ):
            errors.append(f"{name}: image must belong to LifeMap")
        if "container_name" in service:
            errors.append(f"{name}: fixed global container names are forbidden")
        if not service.get("healthcheck", {}).get("test"):
            errors.append(f"{name}: missing healthcheck")
        memory = int(service.get("mem_limit", 0))
        total_memory += memory
        if memory <= 0 or float(service.get("cpus", 0)) <= 0:
            errors.append(f"{name}: explicit memory and CPU limits required")
        for port in service.get("ports", []):
            number = int(port["published"])
            published.append(number)
            if port.get("host_ip") != "127.0.0.1" or number in FORBIDDEN_HOST_PORTS:
                errors.append(f"{name}: non-loopback or conflicting published port")
        for volume in service.get("volumes", []):
            if volume["type"] == "bind" and not (
                name == "jenkins" and volume["source"] == "/var/run/docker.sock"
                and volume["target"] == "/var/run/docker.sock"
            ):
                errors.append(f"{name}: unexpected host bind mount")
        if name == 'kafka':
            if service.get('ports') or set(service.get('networks', {})) != {'backend'}:
                errors.append('Kafka must have no host port and remain on internal backend')
            if service.get('environment', {}).get('KAFKA_PROCESS_ROLES') != 'broker,controller':
                errors.append('Kafka must use KRaft')
            continue
        build = service.get("build", {})
        context = Path(build.get("context", "."))
        dockerfile = context / build.get("dockerfile", "Dockerfile")
        if context.resolve() != root.resolve() or not is_inside(dockerfile, root):
            errors.append(f"{name}: build outside LifeMap")
        elif not dockerfile.is_file():
            errors.append(f"{name}: missing Dockerfile")
    if len(published) != len(set(published)):
        errors.append("Duplicate published host ports")
    if total_memory > (9 if 'streaming' in services else 8 if 'kafka' in services else 7) * 1024 ** 3:
        errors.append("Memory limits exceed E1 plus 1 GiB each for Kafka and streaming")
    mongo = services.get("mongodb", {})
    if mongo.get("ports") or "--auth" not in mongo.get("command", []):
        errors.append("MongoDB must authenticate and have no host port")
    if set(mongo.get("networks", {})) != {"backend"}:
        errors.append("MongoDB must remain on the internal backend network")
    if mongo.get("environment", {}).get("MONGO_DATABASE") != "lifemap":
        errors.append("MongoDB database must be lifemap")
    for kind in ("networks", "volumes"):
        for name, config in model.get(kind, {}).items():
            if config.get("external") or not config.get("name", "").startswith("lifemap_"):
                errors.append(f"{kind}/{name}: resource is external or not scoped to LifeMap")
    if not model.get("networks", {}).get("backend", {}).get("internal"):
        errors.append("Backend network must be internal")
    for name in ('dask-scheduler','dask-worker-1','dask-worker-2'):
        if not any(m.get('type')=='volume' and m.get('source')=='dataset_cache' and m.get('target')=='/data' for m in services.get(name,{}).get('volumes',[])):
            errors.append(f'{name}: persistent shared dataset cache is required')
    secrets = model.get("secrets", {})
    if set(secrets) != {"mongo_root_password", "mongo_writer_password", "mongo_reader_password"}:
        errors.append("Expected three independent file secrets")
    secret_paths = []
    for name, config in secrets.items():
        path = Path(config.get("file", "."))
        secret_paths.append(path.resolve())
        if not is_inside(path, root / ".secrets"):
            errors.append(f"{name}: secret path must stay inside LifeMap/.secrets")
    if len(secret_paths) != len(set(secret_paths)):
        errors.append("Secret file paths must be distinct")
    api_secrets = {secret["source"] for secret in services.get("api", {}).get("secrets", [])}
    if api_secrets != {"mongo_reader_password"}:
        errors.append("API may mount only the read-only MongoDB credential")
    stream = services.get('streaming')
    if stream:
        if stream.get('ports') or set(stream.get('networks', {})) != {'backend'}:
            errors.append('Streaming must remain internal without host ports')
        if {s['source'] for s in stream.get('secrets', [])} != {'mongo_writer_password'}:
            errors.append('Streaming must use only the LifeMap writer credential')
        if not any(v['source'] == 'stream_checkpoints' and v['target'] == '/checkpoints' for v in stream.get('volumes', [])):
            errors.append('Streaming requires independent persistent checkpoints')
        if not any(v['source'] == 'model_store' and v.get('read_only') for v in stream.get('volumes', [])):
            errors.append('Streaming model volume must be read-only')
    for name, service in services.items():
        mounted = {secret["source"] for secret in service.get("secrets", [])}
        if name != "mongodb" and "mongo_root_password" in mounted:
            errors.append(f"{name}: MongoDB administrator credential must not be shared")
    return errors


def validate_sources(root=ROOT):
    errors = []
    # Compilar a memoria no importa los modulos ni ejecuta instaladores/sondas.
    for directory in ("api", "scripts", "docker", "tests", "streaming"):
        for path in (root / directory).rglob("*.py"):
            try:
                ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except SyntaxError as error:
                errors.append(f"Python syntax: {path.relative_to(root)}: {error.msg}")
    for dockerfile in (root / "docker").glob("*/Dockerfile"):
        stages=set()
        for line in dockerfile.read_text(encoding="utf-8").splitlines():
            if line.startswith("FROM "):
                parts=shlex.split(line)
                if parts[1] not in stages and not re.search(r"@sha256:[0-9a-f]{64}(?:\s|$)",line):
                    errors.append(f"{dockerfile.relative_to(root)}: base image without digest")
                if len(parts)>=4 and parts[2].lower()=='as': stages.add(parts[3])
            if line.startswith("COPY ") and not line.startswith("COPY --from="):
                for source in shlex.split(line)[1:-1]:
                    path = root / source
                    if not is_inside(path, root) or not path.exists():
                        errors.append(f"{dockerfile.relative_to(root)}: invalid COPY source {source}")
                    if source.split("/")[0] in {".venv", ".env", ".secrets", "Insumos"}:
                        errors.append(f"{dockerfile.relative_to(root)}: private build input")
    for lock in (root / "requirements").glob("*.lock.txt"):
        for line in lock.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("-r "):
                if not (lock.parent / line[3:].strip()).is_file():
                    errors.append(f"{lock.name}: missing included lock")
            elif not re.match(r"^[A-Za-z0-9_.-]+==[^\s;]+", line):
                errors.append(f"{lock.name}: unpinned dependency")
    return errors


def main():
    if sys.version_info[:2] != (3, 10) or Path(sys.prefix).resolve() != (ROOT / ".venv").resolve():
        raise SystemExit("Use D:\\Proyects\\LifeMap\\.venv\\Scripts\\python.exe")
    model = load_model()
    errors = validate_model(model) + validate_sources()
    missing_secrets = [
        name for name, value in model["secrets"].items()
        if not Path(value["file"]).is_file()
    ]
    print(json.dumps({
        "status": "failed" if errors else "static_configuration_valid",
        "services": sorted(model["services"]),
        "memory_limit_total_mib": sum(int(s["mem_limit"]) for s in model["services"].values()) // 1024 ** 2,
        "cpu_limit_total": sum(float(s["cpus"]) for s in model["services"].values()),
        "networks": sorted(value["name"] for value in model["networks"].values()),
        "volumes": sorted(value["name"] for value in model["volumes"].values()),
        "pending_secret_files": missing_secrets,
        "runtime_verified": False,
        "errors": errors,
    }, ensure_ascii=False, indent=2))
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
