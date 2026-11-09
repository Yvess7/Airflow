"""Comprueba el resultado real del DAG contra el fixture esperado."""

import csv
import json
import os
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
salida = Path(os.environ.get("VENTAS_OUTPUT_DIR", str(RAIZ / "output")))
archivos = list(salida.glob("ejecucion_*/resumen.json"))
if not archivos:
    raise SystemExit("No se generó resumen.json")
esperado = json.loads((RAIZ / "evidencias/resumen_esperado.json").read_text(encoding="utf-8"))
for archivo in archivos:
    obtenido = json.loads(archivo.read_text(encoding="utf-8"))
    obtenido.pop("ejecucion")
    if obtenido != esperado:
        raise SystemExit(f"El resumen no coincide con el esperado: {archivo}")
    with (archivo.parent / "ventas_por_producto.csv").open(encoding="utf-8", newline="") as csv_file:
        filas = list(csv.DictReader(csv_file))
    filas_esperadas = [
        {clave: str(valor) for clave, valor in fila.items()}
        for fila in esperado["productos"]
    ]
    if filas != filas_esperadas:
        raise SystemExit(f"El CSV no coincide con el esperado: {archivo.parent}")
print(f"Resultados verificados: {len(archivos)} ejecución(es), 10 ventas, 21 unidades, 2105.00 MXN")
