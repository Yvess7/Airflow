"""DAG de ejemplo para Airflow 3 mediante su interfaz pública Task SDK."""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task
from ventas.etl import RAIZ, extraer, publicar, transformar, validar


@dag(
    dag_id="ventas_diarias",
    description="Valida un CSV de ventas y publica sus ingresos por producto en MXN",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "alumno", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["ejemplo", "etl", "ventas"],
    doc_md="""
    ## Resumen de ventas
    Procesa el CSV de muestra completo en cada ejecución. La moneda es MXN.
    Flujo: extraer → validar → transformar → publicar.
    Los resultados se guardan en `output/ejecucion_<hash del run_id>/`.
    Consulta el reporte y las instrucciones en el repositorio Yvess7/Airflow.
    """,
)
def ventas_diarias():
    @task
    def extraer_ventas() -> list[dict[str, str]]:
        return extraer(os.environ.get("VENTAS_INPUT_PATH", str(RAIZ / "data/ventas.csv")))

    @task
    def validar_ventas(filas: list[dict[str, str]]) -> list[dict]:
        return validar(filas)

    @task(multiple_outputs=False)
    def transformar_ventas(ventas: list[dict]) -> dict:
        return transformar(ventas)

    @task(multiple_outputs=False)
    def publicar_reporte(resumen: dict) -> dict[str, str]:
        contexto = get_current_context()
        rutas = publicar(
            resumen,
            os.environ.get("VENTAS_OUTPUT_DIR", str(RAIZ / "output")),
            contexto["run_id"],
        )
        logging.info("Resumen de ventas: %s", json.dumps(resumen, ensure_ascii=False))
        logging.info("Archivos generados: %s", rutas)
        return rutas

    publicar_reporte(transformar_ventas(validar_ventas(extraer_ventas())))


dag = ventas_diarias()

if __name__ == "__main__":
    resultado = dag.test()
    if resultado.state != "success":
        raise SystemExit(f"El DAG terminó con estado {resultado.state}")
