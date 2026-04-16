import azure.functions as func
import logging
import json
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()

@app.route(route="weather_medallion", auth_level=func.AuthLevel.FUNCTION)
def weather_medallion(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Iniciando Pipeline Global Weather Medallion")

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            body=json.dumps({"error": "JSON inválido en el body de la petición"}),
            status_code=400,
            mimetype="application/json"
        )

    # ====================== 1. Leer parámetros de entrada ======================
    days_back = req_body.get("days_back")
    start_date_str = req_body.get("start_date")
    end_date_str = req_body.get("end_date")
    cities = req_body.get("cities")

    if not cities or not isinstance(cities, list) or len(cities) == 0:
        return func.HttpResponse(
            body=json.dumps({"error": "El campo 'cities' es obligatorio y debe ser una lista"}),
            status_code=400,
            mimetype="application/json"
        )

    # Calcular rango de fechas
    if days_back is not None:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=int(days_back))
    elif start_date_str and end_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    else:
        return func.HttpResponse(
            body=json.dumps({"error": "Debe enviar 'days_back' o ambos 'start_date' y 'end_date' (formato YYYY-MM-DD)"}),
            status_code=400,
            mimetype="application/json"
        )

    logging.info(f"Procesando {len(cities)} ciudades desde {start_date} hasta {end_date}")

    # ====================== 2. Ingesta de datos desde Open-Meteo (Bronze) ======================
    all_data = []
    for city in cities:
        name = city.get("name")
        lat = city.get("lat")
        lon = city.get("lon")

        if not name or lat is None or lon is None:
            logging.warning(f"Ciudad incompleta ignorada: {city}")
            continue

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "daily": "temperature_2m_max,temperature_2m_min",
            "timezone": "auto"
        }

        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()

            times = data["daily"]["time"]
            tmax = data["daily"]["temperature_2m_max"]
            tmin = data["daily"]["temperature_2m_min"]

            for i, date_str in enumerate(times):
                all_data.append({
                    "fecha": date_str,
                    "ciudad": name,
                    "temp_max": float(tmax[i]),
                    "temp_min": float(tmin[i])
                })

        except Exception as e:
            logging.error(f"Error al obtener datos de {name}: {str(e)}")
            continue

    if not all_data:
        return func.HttpResponse(
            body=json.dumps({"error": "No se pudieron obtener datos de ninguna ciudad"}),
            status_code=500,
            mimetype="application/json"
        )

    df_bronze = pd.DataFrame(all_data)

    # ====================== 3. Conexión a Azure Data Lake ======================
    connect_str = os.getenv("STORAGE_CONNECTION_STRING")
    container_name = os.getenv("CONTAINER_NAME", "weather-lake")

    if not connect_str:
        return func.HttpResponse(
            body=json.dumps({"error": "Falta la variable STORAGE_CONNECTION_STRING"}),
            status_code=500,
            mimetype="application/json"
        )

    try:
        blob_service = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service.get_container_client(container_name)

        # Crear contenedor si no existe
        if not container_client.exists():
            container_client.create_container()
            logging.info(f"Contenedor '{container_name}' creado.")

    except Exception as e:
        logging.error(f"Error conectando a Azure Storage: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": "Error al conectar con Azure Data Lake"}),
            status_code=500,
            mimetype="application/json"
        )

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ====================== BRONZE LAYER ======================
    bronze_path = f"bronze/{run_id}/weather_bronze.csv"
    bronze_blob = container_client.get_blob_client(bronze_path)
    bronze_blob.upload_blob(df_bronze.to_csv(index=False), overwrite=True)
    logging.info(f"✅ Bronze guardado: {bronze_path} ({len(df_bronze)} registros)")

    # ====================== SILVER LAYER ======================
    df_silver = df_bronze.copy()
    df_silver["temp_avg"] = (df_silver["temp_max"] + df_silver["temp_min"]) / 2.0
    df_silver = df_silver.sort_values(by=["ciudad", "fecha"]).reset_index(drop=True)
    df_silver = df_silver.dropna()

    silver_path = f"silver/{run_id}/weather_silver.parquet"
    silver_blob = container_client.get_blob_client(silver_path)
    silver_blob.upload_blob(df_silver.to_parquet(index=False), overwrite=True)
    logging.info(f"✅ Silver guardado: {silver_path}")

    # ====================== GOLD LAYER ======================
    df_gold = df_silver.groupby("ciudad").agg({
        "temp_max": "mean",
        "temp_min": "mean",
        "temp_avg": "mean"
    }).round(2).reset_index()

    df_gold.rename(columns={
        "temp_max": "avg_temp_max",
        "temp_min": "avg_temp_min",
        "temp_avg": "avg_temp"
    }, inplace=True)

    df_gold["period_start"] = start_date.strftime("%Y-%m-%d")
    df_gold["period_end"] = end_date.strftime("%Y-%m-%d")
    df_gold["days_processed"] = len(df_silver) // len(df_gold) if len(df_gold) > 0 else 0

    gold_path = f"gold/{run_id}/weather_gold.parquet"
    gold_blob = container_client.get_blob_client(gold_path)
    gold_blob.upload_blob(df_gold.to_parquet(index=False), overwrite=True)
    logging.info(f"✅ Gold guardado: {gold_path}")

    # ====================== Respuesta final ======================
    result = {
        "status": "success",
        "run_id": run_id,
        "records_bronze": len(df_bronze),
        "records_silver": len(df_silver),
        "cities_processed": len(df_gold),
        "period": f"{start_date} a {end_date}",
        "paths": {
            "bronze": bronze_path,
            "silver": silver_path,
            "gold": gold_path
        }
    }

    return func.HttpResponse(
        body=json.dumps(result, ensure_ascii=False),
        mimetype="application/json",
        status_code=200
    )