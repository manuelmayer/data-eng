"""DAG Generator para la Capa Landing — Apache Airflow.

Lee los archivos de configuración YAML ubicados en:
    pipelines/extractions/<origen>/configs/*.yml
(por ejemplo: pipelines/extractions/from_api/configs/*.yml)

Por cada archivo de configuración genera dinámicamente un DAG en Airflow:
- Asigna dag_id, periodicidad (schedule), retries y tags definidos en el YAML.
- Discierne el tipo de origen ('source_type') para invocar el script correspondiente:
    - 'api' -> pipelines/extractions/from_api/extract_from_api.py
    - 'db'  -> pipelines/extractions/from_db/extract_from_db.py
- Pasa la ruta del YAML al script como argumento de CLI para aislar cada ejecución.
"""

import glob
import logging
import os
from datetime import datetime
import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

# Ruta base del proyecto montada en el contenedor de Airflow
PROJECT_CODE = "/opt/airflow/project_code"
EXTRACTIONS_DIR = os.path.join(PROJECT_CODE, "pipelines", "extractions")

# Mapeo de source_type a script de extracción
SOURCE_SCRIPTS: dict[str, str] = {
    "api": os.path.join(EXTRACTIONS_DIR, "from_api", "extract_from_api.py"),
    "db": os.path.join(EXTRACTIONS_DIR, "from_db", "extract_from_db.py"),
}


def load_config(config_path: str) -> dict:
    """Carga y valida el archivo de configuración YAML."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_script_path(source_type: str) -> str:
    """Determina la ruta del script de extracción según el source_type."""
    if source_type in SOURCE_SCRIPTS:
        return SOURCE_SCRIPTS[source_type]

    # Convención fallback: from_<source_type>/extract_from_<source_type>.py
    fallback_script = os.path.join(
        EXTRACTIONS_DIR, f"from_{source_type}", f"extract_from_{source_type}.py"
    )
    if os.path.exists(fallback_script):
        return fallback_script

    raise ValueError(
        f"source_type '{source_type}' no soportado y no se encontró el script en: {fallback_script}"
    )


def create_landing_dag(config: dict, config_path: str) -> DAG:
    """Crea una instancia de DAG de Airflow a partir de la configuración."""
    dag_id = config.get("dag_id")
    if not dag_id:
        raise ValueError(f"Falta 'dag_id' en {config_path}")

    source_type = config.get("source_type", "api")
    script_path = get_script_path(source_type)

    schedule = config.get("schedule", None)
    catchup = config.get("catchup", False)
    retries = config.get("retries", 1)
    tags = config.get("tags", ["landing", source_type])

    raw_start_date = str(config.get("start_date", "2025-01-01"))
    start_date = datetime.strptime(raw_start_date, "%Y-%m-%d")

    config_filename = os.path.basename(config_path)

    with DAG(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        default_args={"retries": retries},
        tags=tags,
        doc_md=f"### Pipeline Landing: `{dag_id}`\n\nGenerado dinámicamente desde `{config_filename}`.",
    ) as dag:

        BashOperator(
            task_id="extract_to_landing",
            bash_command=f"python {script_path} {config_path}",
        )

    return dag


# ------------------------------------------------------------------------------
# Descubrimiento dinámico de configuraciones (Opción B: por carpeta de origen)
# Busca en pipelines/extractions/*/configs/*.yml
# ------------------------------------------------------------------------------
config_pattern = os.path.join(EXTRACTIONS_DIR, "*", "configs", "*.yml")
discovered_configs = glob.glob(config_pattern)

for cfg_path in discovered_configs:
    try:
        cfg = load_config(cfg_path)
        if not isinstance(cfg, dict):
            continue

        dag_instance = create_landing_dag(cfg, cfg_path)
        globals()[cfg["dag_id"]] = dag_instance

    except Exception as e:
        logger.error(f"[to_landing_dag_generator] Error procesando '{cfg_path}': {e}")
