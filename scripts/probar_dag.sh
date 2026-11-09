#!/usr/bin/env bash
# Crea una base de metadatos aislada y ejecuta las cuatro tareas reales.
set -euo pipefail
export AIRFLOW_HOME="$(mktemp -d /tmp/airflow-ventas-test.XXXXXX)"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$AIRFLOW_HOME/logs"
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="/opt/airflow/dags${PYTHONPATH:+:$PYTHONPATH}"
airflow db migrate
python /opt/airflow/dags/ventas_diarias.py
python /opt/airflow/scripts/verificar_salida.py
