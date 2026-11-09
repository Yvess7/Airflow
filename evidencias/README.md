# Evidencia de ejecución del ejemplo de ventas

Pruebas realizadas el 9 de noviembre de 2026 con la imagen oficial `apache/airflow:3.3.2-python3.12`.

| Archivo | Procedencia |
| --- | --- |
| `resumen_esperado.json` | Valores calculados para comprobar las diez ventas de muestra |
| `resumen_obtenido.json` | Archivo generado por una ejecución real con `dag.test()` |
| `ventas_por_producto_obtenido.csv` | CSV generado en la misma ejecución |
| `ejecucion_airflow.log` | Salida real de la prueba de Airflow descrita en el README |
| `estados_interfaz.txt` | Consulta de los estados de las cuatro tareas de `demostracion_reporte` en la instancia local |

La prueba con `dag.test()` usa una base de metadatos temporal. La ejecución `demostracion_reporte` se comprobó adicionalmente con el scheduler de la instancia que ofrece la interfaz web.

Los resultados se comparan con el fixture mediante `scripts/verificar_salida.py`. La ejecución de la lógica sin Airflow tiene un identificador diferente y no se usa como evidencia de ejecución del DAG.

Las carpetas de trabajo de `output/` están excluidas de Git. Estos archivos son una muestra conservada para acompañar el reporte. Las contraseñas y la base de metadatos permanecen en el volumen local de Docker.
