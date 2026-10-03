import yaml
import requests
import polars as pl
import logging
from datetime import datetime
from pathlib import Path

# --- CONFIGURACIÓN DE LOGGER ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# --- ACTUALIZACIÓN DE RUTAS ---
# __file__ es: data-eng/extract/from_api/extract_from_apis.py
CURRENT_DIR = Path(__file__).resolve().parent
# Subimos dos niveles para llegar a la raíz (data-eng)
BASE_DIR = CURRENT_DIR.parent.parent 
# La carpeta configs ahora está al lado del script
CONFIG_DIR = CURRENT_DIR / "configs"

POLARS_TYPE_MAP = {
    "string": pl.String,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "float32": pl.Float32,
    "float64": pl.Float64,
    "boolean": pl.Boolean
}

def extract_data(config: dict) -> list[dict]:
    url = config["url"]
    params = config.get("params", {})
    
    logger.info(f"Obteniendo datos desde {url}...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    raw_json = response.json()
    data_key = config.get("data_key")
    
    if data_key and data_key in raw_json:
        return raw_json[data_key]
    return raw_json if isinstance(raw_json, list) else [raw_json]

def process_and_save(raw_data: list[dict], config: dict):
    pipeline_name = config.get("pipeline_name", "unnamed_pipeline")
    
    if not raw_data:
        logger.warning(f"No hay datos para procesar en {pipeline_name}.")
        return

    schema_config = config.get("schema", {})
    output_path = config["output_path"]
    
    df = pl.DataFrame(raw_data)
    
    select_exprs = []
    for col_name, type_str in schema_config.items():
        if col_name in df.columns:
            pl_type = POLARS_TYPE_MAP.get(type_str.lower(), pl.String)
            select_exprs.append(pl.col(col_name).cast(pl_type))
        else:
            logger.warning(f"La columna '{col_name}' falta en el origen.")

    df_clean = df.select(select_exprs).with_columns(
        pl.lit(datetime.now()).alias("extracted_at")
    )
    
    now = datetime.now()
    # BASE_DIR asegura que la carpeta 'data' se cree en la raíz del repo
    partition_dir = BASE_DIR / "data" / output_path / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = partition_dir / f"{pipeline_name}_snapshot.parquet"
    df_clean.write_parquet(file_path)
    
    logger.info(f"{pipeline_name}: {df_clean.height} filas guardadas en {file_path}")

def main():
    if not CONFIG_DIR.exists():
        logger.error(f"El directorio {CONFIG_DIR} no existe.")
        return

    for config_file in CONFIG_DIR.glob("*.yml"):
        logger.info(f"--- Iniciando pipeline desde: {config_file.name} ---")
        try:
            with open(config_file, "r") as file:
                config = yaml.safe_load(file)
            
            raw_data = extract_data(config)
            process_and_save(raw_data, config)
            
        except Exception as e:
            # Usar logger.exception incluye automáticamente el stacktrace completo (ideal para debuggear)
            logger.exception(f"Error procesando {config_file.name}: {e}")

if __name__ == "__main__":
    main()