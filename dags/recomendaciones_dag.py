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
    'start_date': datetime(2025, 9, 8),  # Empezar desde hoy
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
    params = {"id_region": "425"}  # ajustá con el id de región que necesites
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://api.cammesa.com/",
    }

    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()

    try:
        # Caso A: devuelve JSON
        data = r.json()
        df = pd.DataFrame(data)   # según estructura real, capaz necesites data['series'] o similar
        df.to_csv(out_csv, sep=";", index=False, encoding="utf-8")
        logging.info(f"Guardado CSV a partir de JSON en {out_csv}")
    except ValueError:
        # Caso B: devuelve CSV directo
        with open(out_csv, "wb") as f:
            f.write(r.content)
        logging.info(f"Guardado CSV directo en {out_csv}")

    ti.xcom_push(key='cammesa_file_path', value=out_csv)
    ti.xcom_push(key='download_date', value=execution_date.strftime('%Y-%m-%d'))
    return f"Datos CAMMESA: {out_csv}"

def create_sample_cammesa_data(execution_date, ti, file_path):
    """
    Crea datos de muestra realistas basados en el formato de CAMMESA
    """
    date_formatted = execution_date.strftime('%d/%m/%Y')
    
    # Generar datos cada 5 minutos para un día completo
    times = []
    demands = []
    temps = []
    
    base_demand = 20000  # MW base
    base_temp = 15  # Temperatura base
    
    for hour in range(24):
        for minute in range(0, 60, 5):  # cada 5 minutos
            time_str = f"{date_formatted} {hour:02d}:{minute:02d}"
            times.append(time_str)
            
            # Simular demanda con patrón diario
            hour_factor = 0.8 + 0.4 * np.sin(2 * np.pi * (hour - 6) / 24)
            demand = int(base_demand * hour_factor + np.random.normal(0, 500))
            demands.append(demand)
            
            # Simular temperatura
            temp_variation = 5 * np.sin(2 * np.pi * (hour - 12) / 24)
            temp = round(base_temp + temp_variation + np.random.normal(0, 1), 1)
            temps.append(temp)
    
    # Crear DataFrame con formato CAMMESA
    df = pd.DataFrame({
        'fecha': times,
        'Prevista': [demands[i] if i % 4 == 0 else '' for i in range(len(demands))],
        'Semana Ant': [demands[i] - np.random.randint(-500, 500) if i % 3 == 0 else '' for i in range(len(demands))],
        'Ayer': [demands[i] - np.random.randint(-300, 300) if i % 2 == 0 else '' for i in range(len(demands))],
        'Hoy': demands,
        'Tem. Prevista': [temps[i] if i % 4 == 0 else '' for i in range(len(temps))],
        'Tem. Semana Ant.': [temps[i] + np.random.normal(0, 2) if i % 3 == 0 else '' for i in range(len(temps))],
        'Tem. Ayer': [temps[i] + np.random.normal(0, 1) if i % 2 == 0 else '' for i in range(len(temps))],
        'Tem. Hoy': temps
    })
    
    # Guardar como CSV con formato correcto
    df.to_csv(file_path, sep=';', index=False, encoding='utf-8')
    logging.info(f"Datos de muestra creados con {len(df)} registros")

def extract_weather_data(**kwargs):
    """
    Extrae datos climáticos de Open-Meteo para Buenos Aires
    """
    ti = kwargs['ti']
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start') or ti.execution_date
    date_str = execution_date.strftime('%Y-%m-%d')
    
    logging.info(f"Descargando datos climáticos para: {date_str}")
    
    # API de Open-Meteo para Buenos Aires
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': -34.6132,  # Buenos Aires
        'longitude': -58.3772,
        'start_date': date_str,
        'end_date': date_str,
        'hourly': 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,pressure_msl,cloudcover',
        'timezone': 'America/Argentina/Buenos_Aires'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        logging.info(f"Datos climáticos descargados: {len(data['hourly']['time'])} registros")
        
        # Crear directorio si no existe
        data_dir = "/tmp/data"
        os.makedirs(data_dir, exist_ok=True)

        weather_file_path = f"{data_dir}/weather_data_{execution_date.strftime('%Y%m%d')}.json"
        with open(weather_file_path, 'w') as f:
            json.dump(data, f)
        
        ti.xcom_push(key='weather_data', value=data)
        ti.xcom_push(key='weather_file_path', value=weather_file_path)
        
        return f"Datos climáticos obtenidos: {weather_file_path}"
        
    except Exception as e:
        logging.error(f"Error al obtener datos climáticos: {str(e)}")
        raise

def process_and_merge_data(**kwargs):
    """
    Procesa y combina los datos de CAMMESA con los datos climáticos
    """
    ti = kwargs['ti']
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start') or ti.execution_date
    date_str = execution_date.strftime('%Y%m%d')
    
    # Obtener rutas de archivos
    cammesa_file_path = ti.xcom_pull(key='cammesa_file_path', task_ids='download_cammesa')
    weather_data = ti.xcom_pull(key='weather_data', task_ids='extract_weather')
    
    logging.info("Procesando datos de CAMMESA...")
    
    # Leer datos de CAMMESA
    try:
        cammesa_df = pd.read_csv(cammesa_file_path, sep=';', encoding='utf-8')
        logging.info(f"Datos CAMMESA cargados: {len(cammesa_df)} filas, columnas: {list(cammesa_df.columns)}")
    except Exception as e:
        logging.error(f"Error leyendo CSV de CAMMESA: {e}")
        # Intentar con otros separadores
        try:
            cammesa_df = pd.read_csv(cammesa_file_path, sep=',', encoding='utf-8')
            logging.info("Datos cargados con separador ','")
        except:
            cammesa_df = pd.read_csv(cammesa_file_path, encoding='utf-8')
            logging.info("Datos cargados con separador automático")
    
    # Procesar fechas de CAMMESA
    cammesa_df['fecha_dt'] = pd.to_datetime(cammesa_df['fecha'], format='%d/%m/%Y %H:%M', errors='coerce')
    cammesa_df = cammesa_df.dropna(subset=['fecha_dt'])
    cammesa_df = cammesa_df.sort_values('fecha_dt')
    
    # Limpiar columnas numéricas (reemplazar comas por puntos)
    numeric_cols = ['Prevista', 'Semana Ant', 'Ayer', 'Hoy', 'Tem. Prevista', 'Tem. Semana Ant.', 'Tem. Ayer', 'Tem. Hoy']
    for col in numeric_cols:
        if col in cammesa_df.columns:
            cammesa_df[col] = cammesa_df[col].astype(str).str.replace(',', '.')
            cammesa_df[col] = pd.to_numeric(cammesa_df[col], errors='coerce')
    
    logging.info("Procesando datos climáticos...")
    
    # Procesar datos climáticos
    weather_df = pd.DataFrame({
        'datetime': pd.to_datetime(weather_data['hourly']['time']),
        'temperature': weather_data['hourly']['temperature_2m'],
        'humidity': weather_data['hourly']['relative_humidity_2m'],
        'precipitation': weather_data['hourly']['precipitation'],
        'wind_speed': weather_data['hourly']['wind_speed_10m'],
        'wind_direction': weather_data['hourly']['wind_direction_10m'],
        'pressure': weather_data['hourly']['pressure_msl'],
        'cloudcover': weather_data['hourly']['cloudcover']
    })
    
    logging.info(f"Datos climáticos procesados: {len(weather_df)} registros")
    
    # Agregar características temporales a ambos datasets
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
    
    # Interpolar datos climáticos horarios a intervalos de 5 minutos
    weather_df = weather_df.set_index('datetime')
    
    # Crear índice cada 5 minutos para el día
    start_time = execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=1)
    time_index = pd.date_range(start=start_time, end=end_time, freq='5min')[:-1]  # Excluir último punto
    
    # Interpolar datos climáticos
    weather_interp = weather_df.reindex(weather_df.index.union(time_index)).interpolate(method='time')
    weather_interp = weather_interp.reindex(time_index)
    weather_interp = weather_interp.reset_index()
    weather_interp.rename(columns={'index': 'datetime'}, inplace=True)
    
    # Combinar datos basándose en timestamp más cercano
    def find_closest_weather(dt):
        idx = weather_interp['datetime'].sub(dt).abs().idxmin()
        return weather_interp.iloc[idx]
    
    # Aplicar merge
    weather_matches = cammesa_df['fecha_dt'].apply(find_closest_weather)
    weather_matches_df = pd.DataFrame(list(weather_matches))
    weather_matches_df.index = cammesa_df.index
    
    # Combinar todos los datos
    final_df = pd.concat([cammesa_df, weather_matches_df.drop('datetime', axis=1)], axis=1)
    
    # Guardar dataset final
    output_path = f"/opt/airflow/data/energy_weather_dataset_{date_str}.csv"
    final_df.to_csv(output_path, index=False)
    
    logging.info(f"Dataset final creado: {output_path}")
    logging.info(f"Registros: {len(final_df)}, Columnas: {len(final_df.columns)}")
    logging.info(f"Columnas finales: {list(final_df.columns)}")
    
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