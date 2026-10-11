"""DAG Generator — Plataforma de Ingeniería de Datos.

Genera un DAG de Airflow por cada archivo de configuración YAML encontrado
en pipelines/configs/. El YAML define el dag_id, la periodicidad, el tipo
de origen (source_type) y los parámetros propios de la extracción.

Mapeo source_type → script de extracción:
    api  → pipelines/extract/api/extract_from_api.py

Para agregar un nuevo origen, basta con:
  1. Crear el script correspondiente en pipelines/extract/<nuevo_origen>/
  2. Añadir la entrada en SOURCE_SCRIPTS
  3. Crear un YAML de configuración en pipelines/configs/
"""

import os
import glob
import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Ruta base del repositorio montada en el contenedor de Airflow
PROJECT_CODE = "/opt/airflow/project_code"
CONFIGS_DIR = os.path.join(PROJECT_CODE, "pipelines", "configs")

# Mapeo de source_type al script de extracción correspondiente
SOURCE_SCRIPTS: dict[str, str] = {
    "api": os.path.join(PROJECT_CODE, "pipelines", "extract", "api", "extract_from_api.py"),
}


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def make_dag(config: dict, config_path: str) -> DAG:
    """Construye un DAG de Airflow a partir de un diccionario de configuración."""

    dag_id = config["dag_id"]
    source_type = config["source_type"]
    schedule = config.get("schedule", None)
    catchup = config.get("catchup", False)
    retries = config.get("retries", 1)
    tags = config.get("tags", [source_type, "landing"])
    start_date = datetime.strptime(config.get("start_date", "2025-01-01"), "%Y-%m-%d")

    if source_type not in SOURCE_SCRIPTS:
        raise ValueError(
            f"[{dag_id}] source_type '{source_type}' no está soportado. "
            f"Opciones válidas: {list(SOURCE_SCRIPTS.keys())}"
        )

    script_path = SOURCE_SCRIPTS[source_type]

    with DAG(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        default_args={"retries": retries},
        tags=tags,
        doc_md=f"Pipeline generado automáticamente desde `{os.path.basename(config_path)}`.",
    ) as dag:

        BashOperator(
            task_id="extract_to_landing",
            bash_command=f"python {script_path} {config_path}",
        )

    return dag


# ---------------------------------------------------------------------------
# Generación dinámica: Airflow ejecuta este módulo al iniciar el Scheduler;
# cada DAG se registra en el namespace global del módulo para ser descubierto.
# ---------------------------------------------------------------------------
for _config_path in glob.glob(os.path.join(CONFIGS_DIR, "*.yml")):
    try:
        _config = load_config(_config_path)
        _dag = make_dag(_config, _config_path)
        globals()[_config["dag_id"]] = _dag
    except Exception as _e:
        import logging
        logging.getLogger(__name__).error(
            f"Error generando DAG desde '{_config_path}': {_e}"
        )
