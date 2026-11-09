# Proyecto de ventas con Airflow

## Qué hace

Este proyecto usa Airflow para leer un archivo de ventas, revisar que los datos estén completos y calcular cuánto se vendió de cada producto.

El ejemplo incluye ventas de café, chocolate y té. Al ejecutarlo, genera un resumen con **10 ventas, 21 unidades y $2,105.00 MXN**.

Los datos están en `data/ventas.csv` y los resultados se guardan en la carpeta `output/`, en archivos CSV y JSON.

## Cómo correrlo

1. Abre **Docker Desktop** y espera a que termine de iniciar.
2. Abre una terminal en la carpeta **Airflow** y ejecuta:

   ```bash
   docker compose up -d --wait
   ```

   La primera vez puede tardar unos minutos.

3. Entra a **http://localhost:8080** desde tu navegador.
4. Usa el usuario **`alumno`**. Para ver tu contraseña, ejecuta:

   ```bash
   docker compose exec airflow cat /opt/airflow/runtime/passwords.json
   ```

   La contraseña es el texto que aparece junto a `alumno`.

5. Busca **`ventas_diarias`**, activa su interruptor y pulsa **Trigger** o el botón de ejecución.
6. Cuando las cuatro tareas estén en verde, abre la carpeta **`output/`** para ver los resultados.

Para detener Airflow:

```bash
docker compose down
```

Para usarlo otro día, abre Docker Desktop y repite los pasos desde el comando de inicio. Tus datos se conservan.

Puedes consultar el [reporte completo](docs/reporte.md) para conocer más sobre el ejemplo.
