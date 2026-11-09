"""Ejecuta sólo la lógica ETL. No sustituye la ejecución del DAG en Airflow."""

import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))

from ventas.etl import extraer, publicar, transformar, validar  # noqa: E402

if __name__ == "__main__":
    resumen = transformar(validar(extraer(RAIZ / "data/ventas.csv")))
    rutas = publicar(resumen, RAIZ / "output", "demostracion_local")
    print(json.dumps({"resumen": resumen, "archivos": rutas}, ensure_ascii=False, indent=2))
