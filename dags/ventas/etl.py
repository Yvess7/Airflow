"""Extracción, validación, agregación y publicación de un lote de ventas."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path

COLUMNAS = ["venta_id", "fecha", "producto", "cantidad", "precio_unitario"]
RAIZ = Path(__file__).resolve().parents[2]


def extraer(ruta: str | Path) -> list[dict[str, str]]:
    """Lee un CSV UTF-8 con cabecera conocida. No se ejecuta al importar el DAG."""
    with Path(ruta).open(newline="", encoding="utf-8-sig") as archivo:
        lector = csv.DictReader(archivo)
        if lector.fieldnames != COLUMNAS:
            raise ValueError(f"Cabecera inválida; se requiere: {','.join(COLUMNAS)}")
        filas = list(lector)
    if not filas:
        raise ValueError("El archivo no contiene ventas")
    return filas


def validar(filas: list[dict[str, str]]) -> list[dict]:
    """Rechaza el lote ante un error y convierte el dinero a centavos enteros."""
    if not filas:
        raise ValueError("El lote no contiene ventas")
    resultado = []
    identificadores = set()
    for numero, fila in enumerate(filas, start=2):
        try:
            if set(fila) != set(COLUMNAS) or any(
                not isinstance(valor, str) or not valor.strip()
                for valor in fila.values()
            ):
                raise ValueError("faltan campos o hay columnas adicionales")
            limpia = {clave: valor.strip() for clave, valor in fila.items()}
            venta_id = limpia["venta_id"]
            if venta_id in identificadores:
                raise ValueError(f"venta_id duplicado: {venta_id}")
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", limpia["fecha"]):
                raise ValueError("fecha debe tener formato AAAA-MM-DD")
            date.fromisoformat(limpia["fecha"])
            if not re.fullmatch(r"[0-9]+", limpia["cantidad"]):
                raise ValueError("cantidad debe ser un entero positivo")
            cantidad = int(limpia["cantidad"])
            if cantidad <= 0:
                raise ValueError("cantidad debe ser mayor que cero")
            precio = limpia["precio_unitario"]
            if not re.fullmatch(r"[0-9]+(?:\.[0-9]{1,2})?", precio):
                raise ValueError("precio debe ser positivo y tener hasta dos decimales")
            centavos = int(Decimal(precio) * 100)
            if centavos <= 0:
                raise ValueError("precio debe ser mayor que cero")
            resultado.append({
                "venta_id": venta_id,
                "fecha": limpia["fecha"],
                "producto": limpia["producto"],
                "cantidad": cantidad,
                "precio_centavos": centavos,
            })
            identificadores.add(venta_id)
        except (ValueError, TypeError) as error:
            raise ValueError(f"Fila {numero}: {error}") from error
    return resultado


def importe(centavos: int) -> str:
    """Representa pesos con dos decimales sin usar punto flotante."""
    return f"{centavos // 100}.{centavos % 100:02d}"


def transformar(ventas: list[dict]) -> dict:
    """Agrupa por producto y calcula los ingresos del lote validado en MXN."""
    productos = {}
    for venta in ventas:
        grupo = productos.setdefault(venta["producto"], {
            "unidades": 0, "ingresos_centavos": 0,
        })
        grupo["unidades"] += venta["cantidad"]
        grupo["ingresos_centavos"] += venta["cantidad"] * venta["precio_centavos"]
    return {
        "moneda": "MXN",
        "ventas": len(ventas),
        "unidades": sum(v["cantidad"] for v in ventas),
        "ingresos": importe(sum(p["ingresos_centavos"] for p in productos.values())),
        "productos": [
            {"producto": nombre, "unidades": datos["unidades"],
             "ingresos": importe(datos["ingresos_centavos"])}
            for nombre, datos in sorted(productos.items())
        ],
    }


def _escribir_atomico(ruta: Path, contenido: str) -> None:
    """Evita archivos parcialmente escritos si una tarea se interrumpe."""
    temporal = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=ruta.parent, delete=False
        ) as archivo:
            temporal = Path(archivo.name)
            archivo.write(contenido)
        os.replace(temporal, ruta)
    finally:
        if temporal is not None:
            temporal.unlink(missing_ok=True)


def publicar(resumen: dict, directorio: str | Path, ejecucion: str) -> dict[str, str]:
    """Sobrescribe el mismo resultado al reintentar una ejecución determinada."""
    identificador = hashlib.sha256(ejecucion.encode("utf-8")).hexdigest()
    destino = Path(directorio) / f"ejecucion_{identificador}"
    destino.mkdir(parents=True, exist_ok=True)
    csv_buffer = io.StringIO(newline="")
    escritor = csv.DictWriter(csv_buffer, fieldnames=["producto", "unidades", "ingresos"])
    escritor.writeheader()
    escritor.writerows(resumen["productos"])
    _escribir_atomico(destino / "ventas_por_producto.csv", csv_buffer.getvalue())
    _escribir_atomico(
        destino / "resumen.json",
        json.dumps({"ejecucion": ejecucion, **resumen}, ensure_ascii=False, indent=2) + "\n",
    )
    return {"csv": str(destino / "ventas_por_producto.csv"),
            "json": str(destino / "resumen.json")}
