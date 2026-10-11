# Data Engineering Platform

Plataforma personal de ingeniería de datos para la orquestación de pipelines ELT. Diseñada para correr localmente con Docker Compose, utilizando Apache Airflow como orquestador y MinIO como capa de almacenamiento de objetos compatible con S3.

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────┐
│                      data-eng-net                        │
│                                                          │
│   ┌─────────────┐    ┌─────────────┐   ┌─────────────┐  │
│   │   Airflow   │───▶│  PostgreSQL │   │    MinIO    │  │
│   │  :8080      │    │  (interno)  │   │  :9000/:9001│  │
│   └─────────────┘    └─────────────┘   └─────────────┘  │
│        │  (orquesta DAGs ELT)               ▲            │
│        └───────────────────────────────────-┘            │
│              (lectura/escritura S3 interna)               │
└──────────────────────────────────────────────────────────┘
```

| Componente   | Rol                                         | Imagen                                    |
|--------------|---------------------------------------------|-------------------------------------------|
| **Airflow**  | Orquestador de pipelines ELT                | `apache/airflow:slim-latest-python3.14`   |
| **MinIO**    | Data Lake — almacenamiento de objetos (S3)  | `bitnamilegacy/minio:2025.7.23-debian-12-r5` |
| **PostgreSQL** | Metastore de Airflow                      | `postgres:15-alpine`                      |

---

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/) >= 24
- [Docker Compose](https://docs.docker.com/compose/) >= 2.20

---

## Configuración inicial

### 1. Clonar el repositorio

```bash
git clone https://github.com/<tu-usuario>/data-eng.git
cd data-eng
```

### 2. Configurar variables de entorno

Copiar la plantilla y completar con los valores reales:

```bash
cp .env.example .env
```

Editar `.env` con un editor de texto. **Este archivo nunca debe ser commiteado** (está en `.gitignore`).

```env
# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=<contraseña-segura>

# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=<contraseña-segura>
POSTGRES_DB=airflow

# Airflow
AIRFLOW__WEBSERVER__SECRET_KEY=<clave-aleatoria-larga>
AIRFLOW_WWW_USER_USERNAME=admin
AIRFLOW_WWW_USER_PASSWORD=<contraseña-segura>
```

> **Tip:** Para generar claves seguras:
> ```bash
> python3 -c "import secrets; print(secrets.token_hex(32))"
> ```

---

## Entorno de desarrollo

### pre-commit (obligatorio antes del primer commit)

```bash
pip install -r requirements-dev.txt
pre-commit install
```

Generar el baseline inicial para `detect-secrets` (solo la primera vez):

```bash
detect-secrets scan > .secrets.baseline
git add .secrets.baseline
```

> **Nota:** `.secrets.baseline` debe commitearse. Registra los falsos positivos conocidos
> para que `detect-secrets` no bloquee commits legítimos.

Hooks activos en cada `git commit`:

| Hook | Acción |
|---|---|
| `trailing-whitespace` | Elimina espacios al final de línea |
| `end-of-file-fixer` | Asegura newline al final de archivo |
| `check-yaml` | Valida sintaxis de todos los YMLs |
| `check-added-large-files` | Bloquea archivos > 500 KB |
| `debug-statements` | Detecta `pdb` / `breakpoint()` olvidados |
| `ruff` | Linting Python con correcciones automáticas |
| `ruff-format` | Formateo Python (equivalente a black) |
| `detect-secrets` | Bloquea commits con credenciales o tokens |

---

## Levantar la plataforma

```bash
docker compose up --build -d
```

La primera vez, Airflow ejecuta `airflow db migrate` y luego inicia en modo `standalone` (Webserver + Scheduler + Triggerer en un único proceso). Esto puede tardar ~1-2 minutos.

Para ver los logs en tiempo real:

```bash
docker compose logs -f airflow
```

---

## Interfaces de usuario

| Servicio     | URL                      | Credenciales                      |
|--------------|--------------------------|-----------------------------------|
| **Airflow**  | http://localhost:8080    | `AIRFLOW_WWW_USER_*` del `.env`   |
| **MinIO UI** | http://localhost:9001    | `MINIO_ROOT_*` del `.env`         |
| **MinIO API (S3)** | http://localhost:9000 | (uso programático / clientes S3) |

---

## Estructura del proyecto

```
data-eng/
├── airflow/
│   ├── Dockerfile                       # Imagen personalizada de Airflow
│   ├── dags/
│   │   └── to_landing_dag_generator.py  # Generador dinámico de DAGs a Landing
│   ├── logs/                            # Logs de ejecución (generados en runtime)
│   └── plugins/                         # Plugins y operadores custom
├── pipelines/
│   └── extractions/                     # Módulos de extracción organizados por origen
│       ├── from_api/
│       │   ├── configs/                 # Configuraciones YAML de APIs
│       │   │   └── example_api.yml      # Plantilla de configuración
│       │   └── extract_from_api.py      # Extracción desde APIs REST → Landing (MinIO)
│       └── from_db/                     # (Próximo) Extracciones desde bases de datos
│           ├── configs/
│           └── extract_from_db.py
├── docker-compose.yml                   # Orquestación de servicios
├── requirements.txt                     # Dependencias Python del pipeline
├── .env.example                         # Plantilla de variables de entorno
└── .gitignore
```

## Cómo agregar un nuevo pipeline

### Pipeline desde API REST

1. Copiar la plantilla de configuración:
   ```bash
   cp pipelines/extractions/from_api/configs/example_api.yml pipelines/extractions/from_api/configs/mi_pipeline.yml
   ```

2. Editar `mi_pipeline.yml` con los valores del nuevo origen:
   ```yaml
   dag_id: landing_mi_pipeline
   pipeline_name: mi_pipeline
   schedule: "@daily"
   start_date: "2025-01-01"
   source_type: api
   url: https://api.ejemplo.com/v1/endpoint
   data_key: results   # o null si la respuesta ya es una lista
   ```

3. Airflow detectará el archivo automáticamente en `to_landing_dag_generator.py` y creará el DAG en la UI.

### Agregar un nuevo tipo de origen (ej. base de datos)

1. Crear la carpeta del origen con su script y directorio de configs:
   ```text
   pipelines/extractions/from_db/
   ├── configs/
   └── extract_from_db.py
   ```
2. Asegurar que el script acepte la ruta del YAML como argumento: `python extract_from_db.py <config.yml>`.
3. Registrar el `source_type: "db"` en `to_landing_dag_generator.py` (o seguir la convención `from_<source_type>/extract_from_<source_type>.py`).

---

## Dependencias Python del pipeline

Definidas en [`requirements.txt`](./requirements.txt) e instaladas en la imagen de Airflow:

| Librería        | Uso                                      |
|-----------------|------------------------------------------|
| `boto3`         | Interacción con MinIO / S3               |
| `s3fs`          | Sistema de archivos S3 para Polars       |
| `polars`        | Transformación de datos (DataFrame)      |
| `pyarrow`       | Serialización Parquet                    |
| `PyYAML`        | Lectura de configuraciones               |
| `requests`      | Llamadas HTTP en tareas de extracción    |
| `python-dotenv` | Carga de variables `.env` en local       |

---

## Seguridad

- Las credenciales se gestionan exclusivamente mediante el archivo `.env`, excluido del control de versiones.
- El metastore PostgreSQL no expone puertos al host por defecto; es accesible únicamente dentro de la red interna `data-eng-net`.
- La comunicación entre Airflow y MinIO se realiza internamente mediante el hostname `minio` (sin pasar por la interfaz de red del host).

---

## Detener la plataforma

```bash
# Detener sin eliminar volúmenes (conserva datos)
docker compose down

# Detener y eliminar todos los datos persistidos
docker compose down -v
```

---

## Licencia

Proyecto personal — uso libre.