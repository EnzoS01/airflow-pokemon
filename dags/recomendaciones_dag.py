# dags/energy_demand_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import json
import os
import logging
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 9),  # Empezar desde hoy
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

def get_data_dir():
    data_dir = os.getenv("AIRFLOW_DATA_DIR", "/tmp/data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def download_cammesa_data(**kwargs):
    ti = kwargs['ti']
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start') or ti.execution_date
    file_date_str = execution_date.strftime('%Y%m%d')

    data_dir = get_data_dir()
    out_csv = f"{data_dir}/cammesa_data_{file_date_str}.csv"

    url = "https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegion"
    params = {"id_region": "425"}  # id correspondiente al Gran Buenos Aires
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://api.cammesa.com/",
    } # headers necesarios

    r = requests.get(url, params=params, headers=headers, timeout=60, verify=False)
    r.raise_for_status()

    max_retries = 5
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=60, verify=False)
            r.raise_for_status()
            break
        except requests.exceptions.SSLError as e:
            logging.warning(f"SSL error en intento {attempt+1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt+1))  # backoff exponencial
            else:
                raise

    # La API devuelve JSON, no CSV directo
    data = r.json()

    # Convertir JSON a DataFrame
    df = pd.DataFrame(data)

    # Renombrar columnas para que coincidan con el formato del CSV manual
    column_mapping = {
        'fecha': 'fecha',
        'demPrevista': 'Prevista',
        'demSemanaAnt': 'Semana Ant',
        'demAyer': 'Ayer',
        'demHoy': 'Hoy',
        'tempPrevista': 'Tem. Prevista',
        'tempSemanaAnt': 'Tem. Semana Ant.',
        'tempAyer': 'Tem. Ayer',
        'tempHoy': 'Tem. Hoy'
    }

    df = df.rename(columns=column_mapping)
    # Asegurar que tengamos todas las columnas esperadas
    expected_columns = ['fecha', 'Prevista', 'Semana Ant', 'Ayer', 'Hoy', 
                       'Tem. Prevista', 'Tem. Semana Ant.', 'Tem. Ayer', 'Tem. Hoy']
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None  # Añadir columnas faltantes con valores nulos
    
    # Reordenar columnas
    df = df[expected_columns]
    
    # Formatear fecha correctamente (de ISO a formato esperado)
    df['fecha'] = pd.to_datetime(df['fecha']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Guardar como CSV con el formato correcto
    df.to_csv(out_csv, index=False, sep=';', encoding='utf-8')
    
    logging.info(f"Guardado CSV formateado en {out_csv}")
    logging.info(f"Registros: {len(df)}, Columnas: {list(df.columns)}")
    ti.xcom_push(key='cammesa_file_path', value=out_csv)
    ti.xcom_push(key='download_date', value=execution_date.strftime('%Y-%m-%d'))
    return f"Datos CAMMESA: {out_csv}"

def extract_weather_data(**kwargs):
    """
    Extrae datos climáticos de Open-Meteo (forecast) para Buenos Aires.
    """
    ti = kwargs['ti']
    execution_date = (
        kwargs.get('execution_date')
        or kwargs.get('data_interval_start')
        or ti.execution_date
    )
    date_str = execution_date.strftime('%Y-%m-%d')  # Hoy
    
    logging.info(f"Descargando datos climáticos para: {date_str}")
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -34.61,
        "longitude": -58.37,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,pressure_msl,cloudcover",
        "timezone": "auto"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Variables por defecto si no vienen en el JSON
        hourly = data.get("hourly", {})
        keys = ["time", "temperature_2m", "relative_humidity_2m",
                "precipitation", "wind_speed_10m", "wind_direction_10m",
                "pressure_msl", "cloudcover"]
        
        # Asegurar que existan todas las claves
        for key in keys:
            if key not in hourly:
                logging.warning(f"'{key}' no está en la respuesta, se llena con NaN")
                hourly[key] = [np.nan] * len(hourly.get("time", []))
        
        # Crear DataFrame con DatetimeIndex
        weather_df = pd.DataFrame(hourly)
        weather_df['time'] = pd.to_datetime(weather_df['time'])
        weather_df = weather_df.set_index('time')
        
        registros = len(weather_df)
        logging.info(f"Datos climáticos descargados: {registros} registros")
        
        # Guardar JSON
        data_dir = get_data_dir()
        weather_file_path = f"{data_dir}/weather_data_{execution_date.strftime('%Y%m%d')}.json"
        with open(weather_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # XCom
        ti.xcom_push(key="weather_data", value=hourly)
        ti.xcom_push(key="weather_file_path", value=weather_file_path)
        
        return f"Datos climáticos obtenidos: {weather_file_path}"
    
    except Exception as e:
        logging.error(f"Error al obtener datos climáticos: {str(e)}")
        raise

def process_and_merge_data(**kwargs):
    import pandas as pd
    import os

    ti = kwargs["ti"]
    weather_data = ti.xcom_pull(task_ids="extract_weather_data",key="weather_data")
    if not weather_data:
        raise ValueError("No se encontraron datos climáticos en XCom.")

    # --- Procesar datos de CAMMESA ---
    cammesa_file = "/tmp/data/cammesa_data.csv"
    cammesa_df = pd.read_csv(cammesa_file)

    # Solo quedarnos con fecha + Hoy
    cammesa_df = cammesa_df[["fecha", "Hoy"]]
    cammesa_df["fecha_dt"] = pd.to_datetime(cammesa_df["fecha"], errors="coerce")
    cammesa_df = cammesa_df.dropna(subset=["fecha_dt"])
    cammesa_df["hour_only"] = cammesa_df["fecha_dt"].dt.floor("H")

    # --- Procesar datos de Open-Meteo ---
    weather_df = pd.DataFrame(weather_data)
    weather_df["datetime"] = pd.to_datetime(weather_df["time"], utc=True)
    weather_df["datetime"] = weather_df["datetime"].dt.tz_convert("America/Argentina/Buenos_Aires")
    weather_df["hour_only"] = weather_df["datetime"].dt.floor("H")

    # Quitar timezone para que coincida con CAMMESA
    weather_df["hour_only"] = weather_df["hour_only"].dt.tz_localize(None)

    # --- Merge ---
    merged_df = pd.merge(
        cammesa_df[["hour_only", "Hoy"]],
        weather_df.drop(columns=["time"]),
        left_on="hour_only",
        right_on="hour_only",
        how="inner"
    )

    # Renombrar fecha final
    merged_df = merged_df.rename(columns={"hour_only": "fecha"})

    # --- Guardar dataset final ---
    output_file = "/tmp/data/energy_weather_dataset.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    merged_df.to_csv(output_file, index=False, encoding="utf-8")

    print(f"Dataset final guardado en {output_file}")
    return output_file



def save_to_master_dataset(**kwargs):
    """
    Agrega los datos procesados al dataset maestro histórico
    """
    ti = kwargs['ti']
    daily_dataset_path = ti.xcom_pull(key='final_dataset_path', task_ids='process_merge_data')
    
    # Leer datos del día
    daily_df = pd.read_csv(daily_dataset_path)
    
    # Ruta del dataset maestro
    master_dataset_path = "/opt/airflow/data/master_energy_dataset.csv"
    
    if os.path.exists(master_dataset_path):
        # Cargar dataset existente
        master_df = pd.read_csv(master_dataset_path)
        
        # Combinar y eliminar duplicados
        combined_df = pd.concat([master_df, daily_df], ignore_index=True)
        if 'fecha_dt' in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['fecha_dt'])
        
        # Ordenar por fecha
        combined_df = combined_df.sort_values('fecha_dt')
        combined_df.to_csv(master_dataset_path, index=False)
        
        logging.info(f"Dataset maestro actualizado: {len(combined_df)} registros totales")
    else:
        # Crear nuevo dataset maestro
        daily_df.to_csv(master_dataset_path, index=False)
        logging.info(f"Dataset maestro creado: {len(daily_df)} registros")
    
    return f"Dataset maestro actualizado: {master_dataset_path}"

# DAG principal
with DAG(
    'energy_demand_pipeline',
    default_args=default_args,
    description='Pipeline completo para datos de demanda energética y clima',
    schedule=None,  # Sin programación automática por ahora
    catchup=False,
    max_active_runs=1,
    tags=['cammesa', 'energy', 'weather', 'ml']
) as dag:
    
    # Descargar datos de CAMMESA
    download_cammesa_task = PythonOperator(
        task_id='download_cammesa',
        python_callable=download_cammesa_data,
        execution_timeout=timedelta(minutes=10)
    )
    
    # Extraer datos climáticos
    extract_weather_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_weather_data,
        execution_timeout=timedelta(minutes=5)
    )
    
    # Procesar y combinar datos
    process_merge_task = PythonOperator(
        task_id='process_merge_data',
        python_callable=process_and_merge_data,
        execution_timeout=timedelta(minutes=10)
    )
    
    # Guardar en dataset maestro
    save_master_task = PythonOperator(
        task_id='save_master_dataset',
        python_callable=save_to_master_dataset,
        execution_timeout=timedelta(minutes=5)
    )
    
    # Definir dependencias
    [download_cammesa_task, extract_weather_task] >> process_merge_task >> save_master_task