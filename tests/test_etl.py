"""Pruebas de calidad de datos, importes y repetición segura de la publicación."""

import copy
import csv
import json
import tempfile
import unittest
from pathlib import Path

from dags.ventas.etl import extraer, publicar, transformar, validar

RAIZ = Path(__file__).resolve().parents[1]


class VentasTests(unittest.TestCase):
    def setUp(self):
        self.filas = extraer(RAIZ / "data/ventas.csv")

    def test_resumen_completo(self):
        esperado = json.loads((RAIZ / "evidencias/resumen_esperado.json").read_text(encoding="utf-8"))
        self.assertEqual(transformar(validar(self.filas)), esperado)

    def test_datos_invalidos(self):
        casos = [
            ("cantidad", "0"), ("cantidad", "-1"), ("cantidad", "1.5"),
            ("precio_unitario", "-10"), ("precio_unitario", "NaN"),
            ("precio_unitario", "Infinity"), ("precio_unitario", "1.001"),
            ("fecha", "2026-02-30"), ("fecha", "20261101"), ("producto", " "),
        ]
        for campo, valor in casos:
            with self.subTest(campo=campo, valor=valor):
                filas = copy.deepcopy(self.filas)
                filas[0][campo] = valor
                with self.assertRaisesRegex(ValueError, "Fila 2"):
                    validar(filas)

    def test_rechaza_duplicados(self):
        with self.assertRaisesRegex(ValueError, "duplicado"):
            validar(self.filas + [self.filas[0]])

    def test_rechaza_lote_vacio(self):
        with self.assertRaises(ValueError):
            validar([])

    def test_rechaza_cabecera_y_archivo_vacio(self):
        with tempfile.TemporaryDirectory() as temporal:
            ruta = Path(temporal) / "entrada.csv"
            for contenido in ("otra_columna\nvalor\n", ",".join(self.filas[0]) + "\n"):
                with self.subTest(contenido=contenido):
                    ruta.write_text(contenido, encoding="utf-8")
                    with self.assertRaises(ValueError):
                        extraer(ruta)

    def test_rechaza_columnas_incompletas_y_adicionales(self):
        for linea in ("V011,2026-11-01,Café,1", "V011,2026-11-01,Café,1,120.00,sobra"):
            with self.subTest(linea=linea), tempfile.TemporaryDirectory() as temporal:
                ruta = Path(temporal) / "entrada.csv"
                ruta.write_text(",".join(self.filas[0]) + "\n" + linea + "\n", encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "Fila 2"):
                    validar(extraer(ruta))

    def test_importes_exactos(self):
        fila = dict(self.filas[0], cantidad="3", precio_unitario="0.10")
        self.assertEqual(transformar(validar([fila]))["ingresos"], "0.30")

    def test_publicacion_idempotente_y_ejecuciones_separadas(self):
        resumen = transformar(validar(self.filas))
        with tempfile.TemporaryDirectory() as temporal:
            rutas = publicar(resumen, temporal, "manual__1")
            contenido = Path(rutas["json"]).read_bytes()
            self.assertEqual(publicar(resumen, temporal, "manual__1"), rutas)
            self.assertEqual(Path(rutas["json"]).read_bytes(), contenido)
            self.assertNotEqual(publicar(resumen, temporal, "manual__2"), rutas)
            self.assertEqual(len(list(Path(temporal).iterdir())), 2)
            with Path(rutas["csv"]).open(encoding="utf-8", newline="") as archivo:
                filas = list(csv.DictReader(archivo))
            self.assertEqual(sum(int(f["unidades"]) for f in filas), 21)


if __name__ == "__main__":
    unittest.main()
