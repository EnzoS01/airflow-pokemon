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
from pytz import timezone

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 9),  # Empezar desde demhoy
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

def get_data_dir():
    """
    Carpeta local mapeada para guardar CSV y JSON dentro del contenedor
    que será reflejada en Windows en C:/Users/dell/Desktop/proyecto-cd-grupo-20/data
    """
    data_dir = "/usr/local/airflow/data"  # Cambiado de /opt/airflow/data
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def download_cammesa_data(region_id, region_name, **kwargs):
    ti = kwargs['ti']
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start') or ti.execution_date
    file_date_str = execution_date.strftime('%Y%m%d')

    data_dir = get_data_dir()
    out_csv = f"{data_dir}/cammesa_data_{region_name}_{file_date_str}.csv"

    url = "https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegion"
    params = {"id_region": str(region_id)}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://api.cammesa.com/",
    }

    r = requests.get(url, params=params, headers=headers, timeout=60, verify=False)
    r.raise_for_status()

    df = pd.DataFrame(r.json())
    # acá podés aplicar el mismo procesamiento de columnas que ya hiciste
    df.to_csv(out_csv, index=False, sep=';', encoding='utf-8')

    logging.info(f"Guardado CSV {region_name} en {out_csv}")
    ti.xcom_push(key=f'cammesa_file_path_{region_name}', value=out_csv)
    return f"Datos CAMMESA {region_name}: {out_csv}"

def extract_weather_data(region_name, latitude, longitude, **kwargs):
    """
    Extrae datos climáticos de Open-Meteo para una región específica.
    """
    ti = kwargs['ti']
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start') or ti.execution_date
    
    argentina_tz = timezone('America/Argentina/Buenos_Aires')
    execution_date_arg = execution_date.astimezone(argentina_tz)
    date_str = execution_date_arg.strftime('%Y-%m-%d')
    
    logging.info(f"[{region_name}] Descargando datos climáticos para: {date_str} (huso Argentina)")
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,pressure_msl,cloudcover",
        "timezone": "auto"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Crear DataFrame
        hourly = data.get("hourly", {})
        for key in ["time","temperature_2m","relative_humidity_2m","precipitation",
                    "wind_speed_10m","wind_direction_10m","pressure_msl","cloudcover"]:
            if key not in hourly:
                hourly[key] = [np.nan] * len(hourly.get("time", []))
        weather_df = pd.DataFrame(hourly)
        weather_df['time'] = pd.to_datetime(weather_df['time'])
        weather_df = weather_df.set_index('time')
        
        # Guardar JSON
        data_dir = get_data_dir()
        weather_file_path = f"{data_dir}/weather_data_{region_name}_{execution_date_arg.strftime('%Y%m%d')}.json"
        with open(weather_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # XCom
        ti.xcom_push(key=f"weather_data_{region_name}", value=hourly)
        ti.xcom_push(key=f"weather_file_path_{region_name}", value=weather_file_path)
        
        return f"[{region_name}] Datos climáticos obtenidos: {weather_file_path}"
    
    except Exception as e:
        logging.error(f"[{region_name}] Error al obtener datos climáticos: {str(e)}")
        raise


def process_and_merge_data(**kwargs):
    ti = kwargs["ti"]
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start') or ti.execution_date
    file_date_str = execution_date.strftime('%Y%m%d')

    argentina_tz = timezone('America/Argentina/Buenos_Aires')

    regions = [
        {"name": "edenor", "cammesa_task": "download_cammesa_edenor", "weather_key": "weather_data_edenor"},
        {"name": "edesur", "cammesa_task": "download_cammesa_edesur", "weather_key": "weather_data_edesur"},
        {"name": "edelap", "cammesa_task": "download_cammesa_edelap", "weather_key": "weather_data_edelap"},
    ]

    output_files = []

    for region in regions:
        region_name = region["name"]

        # --- Recuperar XComs ---
        weather_data = ti.xcom_pull(task_ids=f"extract_weather_{region_name}", key=region["weather_key"])
        if not weather_data:
            raise ValueError(f"No se encontraron datos climáticos para {region_name} en XCom.")

        cammesa_file_path = ti.xcom_pull(task_ids=region["cammesa_task"], key=f"cammesa_file_path_{region_name}")
        if not cammesa_file_path:
            raise ValueError(f"No se encontró archivo CAMMESA para {region_name} en XCom.")

        # --- Procesar CAMMESA ---
        cammesa_df = pd.read_csv(cammesa_file_path, sep=';')
        cammesa_df.columns = cammesa_df.columns.str.strip()  # limpiar espacios invisibles

        if 'demHoy' not in cammesa_df.columns:
            raise KeyError(f"La columna 'demHoy' no existe en {cammesa_file_path}. Columnas: {cammesa_df.columns.tolist()}")

        cammesa_df['demHoy'] = (
            cammesa_df['demHoy'].astype(str)
            .str.replace(',', '.', regex=False)
            .str.replace(' ', '', regex=False)
        )
        cammesa_df['demHoy'] = pd.to_numeric(cammesa_df['demHoy'], errors='coerce')

        cammesa_df["fecha_dt"] = pd.to_datetime(cammesa_df["fecha"], errors="coerce")
        cammesa_df = cammesa_df.dropna(subset=["fecha_dt"])
        cammesa_df = cammesa_df[cammesa_df["fecha_dt"].dt.minute == 0]
        cammesa_df = cammesa_df.sort_values("fecha_dt").head(24)

        # --- Asegurar tz-aware ---
        cammesa_df["hour_only"] = cammesa_df["fecha_dt"].dt.floor("H")
        if cammesa_df["hour_only"].dt.tz is None:
            cammesa_df["hour_only"] = cammesa_df["hour_only"].dt.tz_localize(argentina_tz)

        # --- Procesar Weather ---
        weather_df = pd.DataFrame(weather_data)
        weather_df["datetime_arg"] = pd.to_datetime(weather_df["time"], errors="coerce")
        weather_df["hour_only"] = weather_df["datetime_arg"].dt.floor("H")
        if weather_df["hour_only"].dt.tz is None:
            weather_df["hour_only"] = weather_df["hour_only"].dt.tz_localize(argentina_tz)
        else:
            weather_df["hour_only"] = weather_df["hour_only"].dt.tz_convert(argentina_tz)

        # --- Merge ---
        merged_df = pd.merge(
            cammesa_df[["hour_only", "demHoy", "fecha_dt"]],
            weather_df.drop(columns=["time", "datetime_arg"]),
            on="hour_only",
            how="inner"
        )

        if merged_df.empty:
            raise ValueError(f"No hubo coincidencia entre horas de CAMMESA y Weather para {region_name}")

        merged_df = merged_df.rename(columns={"hour_only": "fecha", "fecha_dt": "fecha_original_cammesa"})

        column_order = ["fecha", "fecha_original_cammesa", "demHoy"] + \
                       [c for c in merged_df.columns if c not in ["fecha", "fecha_original_cammesa", "demHoy"]]
        merged_df = merged_df[column_order]

        # --- Guardar CSV por región ---
        data_dir = get_data_dir()
        output_file = f"{data_dir}/energy_weather_dataset_{region_name}_{file_date_str}.csv"
        merged_df.to_csv(output_file, index=False, encoding="utf-8")
        logging.info(f"[{region_name}] Dataset final guardado en {output_file} con {len(merged_df)} registros")

        # XCom por región
        ti.xcom_push(key=f'final_dataset_path_{region_name}', value=output_file)
        output_files.append(output_file)

    return output_files


def save_to_master_dataset(**kwargs):
    """
    Agrega los datos procesados al dataset maestro histórico para cada región
    y guarda los 3 CSVs maestros en la carpeta mapeada al host.
    """
    import pandas as pd
    import os
    import logging

    ti = kwargs['ti']

    regions = ["edenor", "edesur", "edelap"]
    data_dir = get_data_dir()

    for region in regions:
        # Recuperar dataset diario procesado
        daily_dataset_path = ti.xcom_pull(key=f'final_dataset_path_{region}', task_ids='process_merge_data')
        if not daily_dataset_path:
            logging.warning(f"No se encontró dataset diario para {region}, se salta.")
            continue

        daily_df = pd.read_csv(daily_dataset_path)

        # Asegurar columna fecha como datetime
        if 'fecha' not in daily_df.columns:
            raise ValueError(f"No se encontró la columna 'fecha' en daily_df de {region}")
        daily_df['fecha'] = pd.to_datetime(daily_df['fecha'], errors='coerce')

        # Archivo maestro por región
        master_file_path = os.path.join(data_dir, f"master_energy_dataset_{region}.csv")

        if os.path.exists(master_file_path):
            master_df = pd.read_csv(master_file_path)
            
            # CORRECCIÓN: Siempre convertir la columna fecha a datetime
            if 'fecha' in master_df.columns:
                master_df['fecha'] = pd.to_datetime(master_df['fecha'], errors='coerce')
            else:
                raise ValueError(f"No se encontró la columna 'fecha' en master_df de {region}")

            # Combinar, eliminar duplicados y ordenar
            combined_df = pd.concat([master_df, daily_df], ignore_index=True)
            combined_df.drop_duplicates(subset=['fecha'], inplace=True)
            combined_df.sort_values('fecha', inplace=True)
            combined_df.to_csv(master_file_path, index=False)
            logging.info(f"[{region}] Dataset maestro actualizado: {len(combined_df)} registros")
        else:
            daily_df.sort_values('fecha', inplace=True)
            daily_df.to_csv(master_file_path, index=False)
            logging.info(f"[{region}] Dataset maestro creado: {len(daily_df)} registros")

    return f"Datasets maestros actualizados en {data_dir}"


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
    download_edenor = PythonOperator(
        task_id="download_cammesa_edenor",
        python_callable=download_cammesa_data,
        op_args=[1077, "edenor"],
    )

    download_edesur = PythonOperator(   
        task_id="download_cammesa_edesur",
        python_callable=download_cammesa_data,
        op_args=[1078, "edesur"],
    )

    download_edelap = PythonOperator(
        task_id="download_cammesa_edelap",
        python_callable=download_cammesa_data,
        op_args=[1943, "edelap"],
    )
    
    # Extraer datos climáticos
    extract_weather_edenor = PythonOperator(
        task_id='extract_weather_edenor',
        python_callable=extract_weather_data,
        op_args=["edenor", -34.57, -58.46]  # ejemplo de coordenadas
    )

    extract_weather_edesur = PythonOperator(
        task_id='extract_weather_edesur',
        python_callable=extract_weather_data,
        op_args=["edesur", -34.66, -58.45]
    )

    extract_weather_edelap = PythonOperator(
        task_id='extract_weather_edelap',
        python_callable=extract_weather_data,
        op_args=["edelap", -34.91, -57.95]
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
[download_edenor, download_edesur, download_edelap,
 extract_weather_edenor, extract_weather_edesur, extract_weather_edelap] >> process_merge_task >> save_master_task

