"""Extracción de datos desde APIs REST hacia la capa Landing del Data Lake.

Uso:
    python extract_from_api.py <ruta_al_config.yml>

El script lee la configuración del pipeline desde el YAML indicado,
realiza la llamada HTTP correspondiente y persiste el JSON crudo
en el bucket 'landing' de MinIO, sin aplicar ninguna transformación.

Estructura del objeto resultante en MinIO:
    s3://landing/<pipeline_name>/YYYY/MM/DD/<pipeline_name>_YYYYMMDD_HHMMSS.json
"""

import sys
import yaml
import requests
import logging
import json
import os
import boto3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# --- VARIABLES DE ENTORNO ---
# En Docker, las variables son inyectadas por docker-compose (env_file + environment).
# En ejecución local, se cargan desde el archivo .env del proyecto.
load_dotenv()

# --- LOGGER ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN S3 / MINIO ---
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("MINIO_ROOT_USER") or os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("AWS_SECRET_ACCESS_KEY")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise RuntimeError(
        "Credenciales de MinIO no configuradas. "
        "Definir MINIO_ROOT_USER y MINIO_ROOT_PASSWORD en el archivo .env "
        "o como variables de entorno del contenedor."
    )

s3_client = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

LANDING_BUCKET = "landing"


# ---------------------------------------------------------------------------
# Funciones
# ---------------------------------------------------------------------------

def ensure_bucket_exists() -> None:
    """Crea el bucket Landing en MinIO si aún no existe."""
    try:
        s3_client.head_bucket(Bucket=LANDING_BUCKET)
    except Exception:
        s3_client.create_bucket(Bucket=LANDING_BUCKET)
        logger.info(f"Bucket '{LANDING_BUCKET}' creado en MinIO.")


def extract_data(config: dict) -> list[dict]:
    """Realiza la solicitud HTTP GET y retorna los registros como lista de dicts."""
    url = config["url"]
    params = config.get("params", {})
    headers = config.get("headers", {})

    headers.setdefault("User-Agent", "DataEngPlatform/1.0 (manmayer@gmail.com)")

    logger.info(f"Extrayendo datos desde: {url}")
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    raw = response.json()
    data_key = config.get("data_key")

    if data_key:
        return raw.get(data_key, [])
    return raw if isinstance(raw, list) else [raw]


def save_to_landing(raw_data: list[dict], pipeline_name: str) -> str:
    """Persiste el JSON crudo en MinIO bajo s3://landing/<pipeline>/ sin transformar."""
    now = datetime.now()
    file_name = f"{pipeline_name}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    s3_key = f"{pipeline_name}/{now.strftime('%Y/%m/%d')}/{file_name}"

    payload = json.dumps(raw_data, ensure_ascii=False, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=LANDING_BUCKET,
        Key=s3_key,
        Body=payload,
        ContentType="application/json",
    )
    logger.info(f"[LANDING] {len(raw_data)} registros → s3://{LANDING_BUCKET}/{s3_key}")
    return s3_key


def run(config_path: str) -> None:
    """Punto de entrada principal: extrae y carga a Landing según la configuración."""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    pipeline_name = config.get("pipeline_name") or config.get("dag_id", "unnamed")
    logger.info(f"=== Iniciando pipeline: {pipeline_name} ===")

    ensure_bucket_exists()
    raw_data = extract_data(config)
    save_to_landing(raw_data, pipeline_name)

    logger.info(f"=== Pipeline '{pipeline_name}' finalizado correctamente ===")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Uso: python {sys.argv[0]} <ruta_al_config.yml>")
        sys.exit(1)

    run(sys.argv[1])
