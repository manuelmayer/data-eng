# ==============================================================================
# Apache Airflow
#
# Base: apache/airflow:slim-latest-python3.14
# Incluye: boto3, polars, pyarrow y demás librerías listadas en requirements.txt
# ==============================================================================
FROM apache/airflow:slim-latest-python3.14

# Instalación de dependencias del proyecto con privilegios de administrador
USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Restaurar el usuario sin privilegios para la ejecución de servicios y tareas
USER airflow
