from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import time
import logging
from requests.exceptions import ConnectionError, HTTPError
logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

POKEMON_LIMIT = 1000
OUTPUT_PATH = "/tmp/pokemon_data/pokemon_base.csv"
POKEMON_DATA_PATH = "/tmp/pokemon_data/pokemon_data.json"
SPECIES_DATA_PATH = "/tmp/pokemon_data/species_data.json"
MERGED_DATA_PATH = "/tmp/pokemon_data/pokemon_merged.csv"

default_args = {
    'owner': 'pablo',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

# Tarea A: /pokemon/{id}
def download_pokemon_data(**kwargs):
    import os
    import json
    import time
    import logging
    from airflow.providers.http.hooks.http import HttpHook

    # Obtener la lista de Pokémon desde el XCom de fetch_pokemon_list
    ti = kwargs['ti']
    results = json.loads(ti.xcom_pull(task_ids='fetch_pokemon_list'))['results']
    os.makedirs(os.path.dirname(POKEMON_DATA_PATH), exist_ok=True)

    # Si el archivo pokemon_data.json ya existe, lo cargamos para evitar repetir descargas
    if os.path.exists(POKEMON_DATA_PATH):
        with open(POKEMON_DATA_PATH, 'r') as f:
            pokemon_data = json.load(f)

        # Creamos un set con los nombres ya descargados para comparación rápida
        done_names = {p['name'] for p in pokemon_data}
        logging.info(f"[INFO] Ya existen {len(done_names)} pokémon descargados.")
    else:
        pokemon_data = []
        done_names = set()

    # Inicializamos el hook HTTP para hacer las requests a la pokeapi
    hook = HttpHook(http_conn_id='pokeapi', method='GET')

    try:
        # Iteramos sobre los Pokémon disponibles
        for i, entry in enumerate(results):
            name = entry['name']

            # Si ya lo descargamos antes, lo salteamos
            if name in done_names:
                continue

            url = entry['url']
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon/{pokemon_id}/"

            # Hacemos la request a la API
            res = hook.run(endpoint)
            pokemon = res.json()

            # Guardamos el JSON crudo en la lista
            pokemon_data.append(pokemon)
            done_names.add(name)

            # Guardado parcial cada 100 Pokémon
            if (i + 1) % 100 == 0:
                with open(POKEMON_DATA_PATH, 'w') as f:
                    json.dump(pokemon_data, f)
                logging.info(f"[INFO] {i + 1} pokémon procesados (hasta ahora {len(pokemon_data)} guardados)")

            # Para no saturar la API
            time.sleep(0.5)

    except Exception as e:
        # Si hay error, guardamos lo que se pudo descargar y relanzamos el error
        logging.error(f"[ERROR] Interrupción en pokémon: {e}")
        with open(POKEMON_DATA_PATH, 'w') as f:
            json.dump(pokemon_data, f)
        raise e

    # Guardado final completo
    with open(POKEMON_DATA_PATH, 'w') as f:
        json.dump(pokemon_data, f)

    logging.info(f"[INFO] Descarga finalizada con {len(pokemon_data)} pokémon.")


# Tarea B: /pokemon-species/{id}
def download_species_data(**kwargs):
    import os
    import json
    import time
    import logging
    from airflow.providers.http.hooks.http import HttpHook

    # Obtener lista de Pokémon desde la tarea anterior (fetch_pokemon_list)
    ti = kwargs['ti']
    results = json.loads(ti.xcom_pull(task_ids='fetch_pokemon_list'))['results']
    os.makedirs(os.path.dirname(SPECIES_DATA_PATH), exist_ok=True)

    # Si el archivo species_data.json ya existe, lo cargamos para evitar repeticiones
    if os.path.exists(SPECIES_DATA_PATH):
        with open(SPECIES_DATA_PATH, 'r') as f:
            species_data = json.load(f)

        # Creamos un set con los nombres ya descargados para comparación rápida
        done_names = {s['name'] for s in species_data}
        logging.info(f"[INFO] Ya existen {len(done_names)} species descargadas.")
    else:
        species_data = []
        done_names = set()

    # Inicializamos el hook para hacer las requests a pokeapi
    hook = HttpHook(http_conn_id='pokeapi', method='GET')

    try:
        # Iteramos sobre todos los Pokémon recibidos en la lista original
        for i, entry in enumerate(results):
            name = entry['name']

            # Si ya descargamos esta species previamente, la salteamos
            if name in done_names:
                continue

            url = entry['url']
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon-species/{pokemon_id}/"

            # Hacemos la request y parseamos la respuesta
            res = hook.run(endpoint)
            species = res.json()

            # Guardamos nombre, generación e info de legendario
            species_data.append({
                'name': species['name'],
                'generation': species['generation']['name'],
                'is_legendary': species['is_legendary']
            })
            done_names.add(species['name'])

            # Cada 100 species, escribimos un backup del archivo parcial
            if (i + 1) % 100 == 0:
                with open(SPECIES_DATA_PATH, 'w') as f:
                    json.dump(species_data, f)
                logging.info(f"[INFO] {i + 1} species procesadas (hasta ahora {len(species_data)} guardadas)")

            # Dormimos medio segundo para no saturar la API
            time.sleep(0.5)

    except Exception as e:
        # Si algo falla, guardamos lo que se haya descargado hasta ahora
        logging.error(f"[ERROR] Interrupción en species: {e}")
        with open(SPECIES_DATA_PATH, 'w') as f:
            json.dump(species_data, f)
        raise e  # relanzamos el error para que Airflow marque la tarea como fallida

    # Guardado final completo por si el total no es múltiplo de 100
    with open(SPECIES_DATA_PATH, 'w') as f:
        json.dump(species_data, f)

    logging.info(f"[INFO] Descarga finalizada con {len(species_data)} species.")


# Tarea C: combinar y transformar
def merge_and_transform_data(**kwargs):
    execution_date = kwargs['ds']
    MERGED_DATA_PATH = f"/tmp/pokemon_data/output/final_{execution_date}.csv"
    with open(POKEMON_DATA_PATH, 'r') as f:
        pokemon_data = json.load(f)
    with open(SPECIES_DATA_PATH, 'r') as f:
        species_data = json.load(f)
    species_lookup = {
        s['name']: {'generation': s['generation'], 'is_legendary': s['is_legendary']}
        for s in species_data
    }
    tidy_records = []
    for p in pokemon_data:
        p_info = species_lookup.get(p['name'], {})  # puede quedar como None
        stats = {s['stat']['name']: s['base_stat'] for s in p.get('stats', [])}
        types = sorted(p.get('types', []), key=lambda t: t['slot'])
        tidy_records.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "height": p.get("height"),
            "weight": p.get("weight"),
            "base_experience": p.get("base_experience"),
            "generation": p_info.get("generation"),
            "is_legendary": p_info.get("is_legendary", False),
            "type_1": types[0]['type']['name'] if len(types) > 0 else None,
            "type_2": types[1]['type']['name'] if len(types) > 1 else None,
            "hp": stats.get("hp"),
            "attack": stats.get("attack"),
            "defense": stats.get("defense"),
            "special-attack": stats.get("special-attack"),
            "special-defense": stats.get("special-defense"),
            "speed": stats.get("speed"),
            "grupo": "Grupo 20",
        })
    df = pd.DataFrame(tidy_records)
    os.makedirs(os.path.dirname(MERGED_DATA_PATH), exist_ok=True)
    df.to_csv(MERGED_DATA_PATH, index=False)
    print(f"[INFO] CSV guardado en: {MERGED_DATA_PATH}")

# Tarea D: Nueva tarea para generar un archivo zip con los logs del DAG
def exportar_logs_reales_zip(**kwargs):
    import shutil

    execution_date= kwargs['ds']

    # Obteniendo información del dag
    dag_id= kwargs["dag"].dag_id
    ti= kwargs["ti"]
    run_id = ti.run_id
    logs_path=  f"/usr/local/airflow/logs/dag_id={dag_id}/run_id={run_id}"
    output_path= f"/tmp/pokemon_data/output/logs_{execution_date}.zip"
    
    # Crea la carpeta destino por si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Verificar si existen logs para este DAG
    if not os.path.exists(logs_path):
        print(f"[INFO] No se encontró directorio de logs: {logs_path}")
        return

    # Crea el archivo zip
    shutil.make_archive(output_path.replace(".zip",""), 'zip', logs_path)

    print(f"[INFO] Logs comprimidos en: {output_path}")

# Tarea E: Nueva tarea que va a enviar el correo

def enviar_correo_manual(**kwargs):
    import smtplib
    import os
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    from airflow.hooks.base import BaseHook

    execution_date= kwargs["ds"]
    output_dir= "/tmp/pokemon_data/output"

    archivos = [f"{output_dir}/logs_{execution_date}.zip", f"{output_dir}/final_{execution_date}.csv"]

    destinatario= "cienciadedatos.frm.utn@gmail.com"

    asunto =  f"Entrega Grupo 20 - {execution_date}"
    cuerpo =f"""
    Envío este correo como prueba
    Adjunto los archivos generados en la ejecución del DAG pokemon_base_etl_parallel del {execution_date}.

    Los archivos adjuntos son: 
    - final_{execution_date}.csv: Dataset con información de Pokémon
    - logs_{execution_date}.zip: Logs de la ejecución

    """
    
    try:
        # Obtener credenciales desde la Connection en Airflow
        conn = BaseHook.get_connection("gmail_smtp")
        remitente = conn.login
        contraseña = conn.password
        servidor = conn.host
        puerto = conn.port

        # Crear mensaje
        mensaje = MIMEMultipart()
        mensaje["From"] = remitente
        mensaje["To"] = destinatario
        mensaje["Subject"] = asunto
        mensaje.attach(MIMEText(cuerpo, "plain"))

        # Adjuntar archivos si existen
        if archivos:
            for archivo in archivos:
                if os.path.exists(archivo):
                    with open(archivo, "rb") as adj:
                        parte = MIMEBase("application", "octet-stream")
                        parte.set_payload(adj.read())
                        encoders.encode_base64(parte)
                        parte.add_header("Content-Disposition", f"attachment; filename={os.path.basename(archivo)}")
                        mensaje.attach(parte)
                else:
                    print(f"[WARN] No se encontró el archivo: {archivo}")

        # Conectar y enviar
        with smtplib.SMTP(servidor, puerto) as server:
            server.starttls()
            server.login(remitente, contraseña)
            server.sendmail(remitente, destinatario, mensaje.as_string())

        print(f"[INFO] Correo enviado con éxito a {destinatario}")

    except Exception as e:
        print(f"[ERROR] No se pudo enviar el correo: {str(e)}")

# DAG
with DAG(
    dag_id='pokemon_base_etl_parallel',
    description='DAG ETL paralelo que une data de /pokemon y /pokemon-species',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['pokemon', 'parallel', 'etl']
) as dag:

    fetch_pokemon_list = HttpOperator(
        task_id='fetch_pokemon_list',
        http_conn_id='pokeapi',
        endpoint=f'/pokemon?limit={POKEMON_LIMIT}',
        method='GET',
        log_response=True,
        response_filter=lambda response: response.text,
        do_xcom_push=True,
    )

    download_a = PythonOperator(
        task_id='download_pokemon_data',
        python_callable=download_pokemon_data,
    )

    download_b = PythonOperator(
        task_id='download_species_data',
        python_callable=download_species_data,
    )

    merge_transform = PythonOperator(
        task_id='merge_and_transform_data',
        python_callable=merge_and_transform_data,
    )

    export_logs = PythonOperator(
    task_id='exportar_logs_zip',
    python_callable=exportar_logs_reales_zip,
    )

    enviar_email = PythonOperator(
    task_id='enviar_email',
    python_callable= enviar_correo_manual,
    )

    fetch_pokemon_list >> [download_a, download_b] >> merge_transform >> export_logs >> enviar_email
