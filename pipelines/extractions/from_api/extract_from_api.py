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

# --- CARGAR VARIABLES DE ENTORNO ---
load_dotenv()

# --- CONFIGURACIÓN DE LOGGER ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DE RUTAS ---
CURRENT_DIR = Path(__file__).resolve().parent
CONFIG_DIR = CURRENT_DIR / "configs"

# --- CONFIGURACIÓN S3 / MINIO ---
# Las credenciales se leen del ambiente (inyectadas por docker-compose) o del .env local.
# Si faltan, el pipeline falla con un error explícito en lugar de usar valores por defecto.
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

AWS_ACCESS_KEY_ID = os.getenv("MINIO_ROOT_USER") or os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("AWS_SECRET_ACCESS_KEY")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise RuntimeError(
        "Faltan credenciales de MinIO. Definí MINIO_ROOT_USER y MINIO_ROOT_PASSWORD "
        "en el archivo .env (o AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)."
    )

s3_client = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

LANDING_BUCKET = "landing"


def ensure_bucket_exists() -> None:
    """Crea el bucket Landing en MinIO si no existe."""
    try:
        s3_client.head_bucket(Bucket=LANDING_BUCKET)
    except Exception:
        try:
            s3_client.create_bucket(Bucket=LANDING_BUCKET)
            logger.info(f"Bucket '{LANDING_BUCKET}' creado exitosamente en MinIO.")
        except Exception as e:
            logger.error(f"Error creando bucket '{LANDING_BUCKET}': {e}")
            raise


def extract_data(config: dict) -> list[dict]:
    """Realiza la llamada HTTP y retorna los datos como lista de registros."""
    url = config["url"]
    params = config.get("params", {})
    headers = config.get("headers", {})

    if "User-Agent" not in headers:
        headers["User-Agent"] = "DataEngPortfolio/1.0 (manmayer@gmail.com)"

    logger.info(f"Extrayendo datos desde: {url}")
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    raw_json = response.json()
    data_key = config.get("data_key")

    if data_key and data_key in raw_json:
        return raw_json[data_key]
    return raw_json if isinstance(raw_json, list) else [raw_json]


def save_to_landing(raw_data: list[dict], config: dict) -> str:
    """Persiste el JSON crudo en la capa Landing de MinIO sin ninguna transformación.

    Estructura del objeto en MinIO:
        s3://landing/<pipeline_name>/YYYY/MM/DD/<pipeline_name>_YYYYMMDD_HHMMSS.json
    """
    pipeline_name = config.get("pipeline_name", "unnamed")
    now = datetime.now()
    file_name = f"{pipeline_name}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    s3_key = f"{pipeline_name}/{now.strftime('%Y/%m/%d')}/{file_name}"

    json_bytes = json.dumps(raw_data, ensure_ascii=False, indent=2).encode("utf-8")

    s3_client.put_object(
        Bucket=LANDING_BUCKET,
        Key=s3_key,
        Body=json_bytes,
        ContentType="application/json"
    )
    logger.info(f"[LANDING] s3://{LANDING_BUCKET}/{s3_key} — {len(raw_data)} registros")
    return s3_key


def process_config(config_path: Path) -> None:
    """Procesa un archivo de configuración específico: extrae y guarda en Landing."""
    logger.info(f"--- Pipeline: {config_path.stem} ---")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    raw_data = extract_data(config)
    save_to_landing(raw_data, config)


def main():
    ensure_bucket_exists()

    # Si se pasa la ruta de un YAML por argumento CLI, se ejecuta solo ese pipeline
    if len(sys.argv) > 1:
        target_path = Path(sys.argv[1])
        if not target_path.exists():
            logger.error(f"Archivo de configuración no encontrado: {target_path}")
            sys.exit(1)
        process_config(target_path)
        return

    # Si no hay argumentos, se procesan todos los YAMLs presentes en CONFIG_DIR
    if not CONFIG_DIR.exists():
        logger.error(f"Directorio de configuraciones no encontrado: {CONFIG_DIR}")
        return

    for config_file in CONFIG_DIR.glob("*.yml"):
        try:
            process_config(config_file)
        except Exception as e:
            logger.exception(f"Error en pipeline '{config_file.name}': {e}")


if __name__ == "__main__":
    main()
