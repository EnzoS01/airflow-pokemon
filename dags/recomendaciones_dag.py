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
from urllib.parse import urlencode

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

    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()

    #devuelve CSV directo
    with open(out_csv, "wb") as f:
        f.write(r.content)
    logging.info(f"Guardado CSV directo en {out_csv}")

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
        data_dir = "/tmp/data"
        os.makedirs(data_dir, exist_ok=True)
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
    """
    Procesa y combina los datos de CAMMESA con los datos climáticos,
    interpolando datos horarios a intervalos de 5 minutos y agregando
    características temporales.
    """
    import pandas as pd
    import numpy as np
    import logging
    import os
    from datetime import timedelta

    ti = kwargs['ti']
    execution_date = execution_date.tz_convert(None) if execution_date.tzinfo else execution_date
    date_str = execution_date.strftime('%Y%m%d')

    # Obtener rutas de archivos y datos desde XCom
    cammesa_file_path = ti.xcom_pull(key='cammesa_file_path', task_ids='download_cammesa')
    weather_data = ti.xcom_pull(key='weather_data', task_ids='extract_weather')

    logging.info("Procesando datos de CAMMESA...")

    # Leer CSV de CAMMESA
    try:
        cammesa_df = pd.read_csv(cammesa_file_path, sep=';', encoding='utf-8')
    except Exception:
        cammesa_df = pd.read_csv(cammesa_file_path, sep=',', encoding='utf-8')
    cammesa_df = cammesa_df.dropna(subset=['fecha'])
    cammesa_df['fecha_dt'] = pd.to_datetime(cammesa_df['fecha'], format='%d/%m/%Y %H:%M', errors='coerce')
    cammesa_df = cammesa_df.dropna(subset=['fecha_dt'])
    cammesa_df = cammesa_df.sort_values('fecha_dt')

    # Limpiar columnas numéricas
    numeric_cols = ['Prevista', 'Semana Ant', 'Ayer', 'Hoy',
                    'Tem. Prevista', 'Tem. Semana Ant.', 'Tem. Ayer', 'Tem. Hoy']
    for col in numeric_cols:
        if col in cammesa_df.columns:
            cammesa_df[col] = cammesa_df[col].astype(str).str.replace(',', '.')
            cammesa_df[col] = pd.to_numeric(cammesa_df[col], errors='coerce')

    logging.info(f"Datos CAMMESA cargados: {len(cammesa_df)} filas, columnas: {list(cammesa_df.columns)}")

    # Asegurarse que las fechas sean tz-naive para evitar errores
    cammesa_df['fecha_dt'] = cammesa_df['fecha_dt'].dt.tz_localize(None)

    logging.info("Procesando datos climáticos...")

    # Procesar datos climáticos
    weather_df = pd.DataFrame({
        'datetime': pd.to_datetime(weather_data['time']).tz_localize(None),
        'temperature': weather_data['temperature_2m'],
        'humidity': weather_data['relative_humidity_2m'],
        'precipitation': weather_data['precipitation'],
        'wind_speed': weather_data['wind_speed_10m'],
        'wind_direction': weather_data['wind_direction_10m'],
        'pressure': weather_data['pressure_msl'],
        'cloudcover': weather_data['cloudcover']
    }).set_index('datetime')

    # Crear índice cada 5 minutos para el día
    start_time = execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
    time_index = pd.date_range(start=start_time, end=start_time + timedelta(days=1) - timedelta(minutes=5), freq='5min')

    # Interpolar datos climáticos
    weather_interp = weather_df.reindex(weather_df.index.union(time_index))
    weather_interp = weather_interp.sort_index()
    weather_interp = weather_interp.interpolate(method='time')
    weather_interp = weather_interp.reindex(time_index).reset_index().rename(columns={'index': 'datetime'})

    # Agregar características temporales a CAMMESA
    cammesa_df['year'] = cammesa_df['fecha_dt'].dt.year
    cammesa_df['month'] = cammesa_df['fecha_dt'].dt.month
    cammesa_df['day'] = cammesa_df['fecha_dt'].dt.day
    cammesa_df['hour'] = cammesa_df['fecha_dt'].dt.hour
    cammesa_df['minute'] = cammesa_df['fecha_dt'].dt.minute
    cammesa_df['day_of_week'] = cammesa_df['fecha_dt'].dt.dayofweek
    cammesa_df['is_weekend'] = cammesa_df['day_of_week'].isin([5, 6]).astype(int)
    cammesa_df['hour_sin'] = np.sin(2 * np.pi * cammesa_df['hour'] / 24)
    cammesa_df['hour_cos'] = np.cos(2 * np.pi * cammesa_df['hour'] / 24)
    cammesa_df['month_sin'] = np.sin(2 * np.pi * cammesa_df['month'] / 12)
    cammesa_df['month_cos'] = np.cos(2 * np.pi * cammesa_df['month'] / 12)

    # Función para encontrar el clima más cercano
    def find_closest_weather(dt):
        idx = weather_interp['datetime'].sub(dt).abs().idxmin()
        return weather_interp.iloc[idx]

    weather_matches = cammesa_df['fecha_dt'].apply(find_closest_weather)
    weather_matches_df = pd.DataFrame(list(weather_matches))
    weather_matches_df.index = cammesa_df.index

    # Combinar datasets
    final_df = pd.concat([cammesa_df, weather_matches_df.drop('datetime', axis=1)], axis=1)

    # Guardar dataset final
    output_path = f"/opt/airflow/data/energy_weather_dataset_{date_str}.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.to_csv(output_path, index=False)

    logging.info(f"Dataset final creado: {output_path}")
    logging.info(f"Registros: {len(final_df)}, Columnas: {len(final_df.columns)}")
    logging.info(f"Columnas finales: {list(final_df.columns)}")

    # XCom
    ti.xcom_push(key='final_dataset_path', value=output_path)
    return f"Dataset procesado: {output_path}"



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