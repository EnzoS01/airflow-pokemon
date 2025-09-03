from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook  # o airflow.hooks.base_hook.BaseHook según tu versión

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import json
import time
import logging

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

import requests
from requests.exceptions import ConnectionError, HTTPError

# Configuración
logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

# Paths para almacenamiento de datos
WEATHER_DATA_PATH = "/tmp/music_weather_data/weather_data.json"
SPOTIFY_PLAYLIST_PATH = "/tmp/music_weather_data/playlist_data.json"
AUDIO_FEATURES_PATH = "/tmp/music_weather_data/audio_features.json"
FINAL_DATASET_PATH = "/tmp/music_weather_data/final_dataset.csv"
RECOMMENDATIONS_PATH = "/tmp/music_weather_data/recommendations.json"

# Ciudades para analizar
CITIES = ["London", "New York", "Tokyo", "Sydney", "Madrid", "Berlin", "Paris", "Moscow", "Mexico City", "Toronto"]

default_args = {
    'owner': 'grupo20',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

# Tarea 1: Obtener datos meteorológicos para múltiples ciudades
def fetch_weather_data(**kwargs):
    import os
    import json
    import time
    import logging 
    from airflow.providers.http.hooks.http import HttpHook
    
    os.makedirs(os.path.dirname(WEATHER_DATA_PATH), exist_ok=True)
    
    # Obtener API key desde Airflow Connections
    openweather_conn = BaseHook.get_connection("openweather_api")
    api_key = openweather_conn.password
    
    weather_data = []
    hook = HttpHook(http_conn_id='openweather_api', method='GET')
    
    try:
        for i, city in enumerate(CITIES):
            endpoint = f"/data/2.5/weather?q={city}&appid={api_key}&units=metric"
            
            try:
                response = hook.run(endpoint)
                city_weather = response.json()
                
                # Extraer información relevante
                weather_info = {
                    'city': city,
                    'timestamp': datetime.now().isoformat(),
                    'temperature': city_weather['main']['temp'],
                    'humidity': city_weather['main']['humidity'],
                    'pressure': city_weather['main']['pressure'],
                    'weather_condition': city_weather['weather'][0]['main'],
                    'weather_description': city_weather['weather'][0]['description'],
                    'wind_speed': city_weather['wind']['speed'],
                    'cloudiness': city_weather['clouds']['all'] if 'clouds' in city_weather else None
                }
                
                weather_data.append(weather_info)
                logging.info(f"Weather data fetched for {city}")
                
            except Exception as e:
                logging.error(f"Error fetching weather for {city}: {str(e)}")
                continue
            
            # Pausa para no saturar la API
            time.sleep(0.5)
            
            # Guardado parcial cada 5 ciudades
            if (i + 1) % 5 == 0:
                with open(WEATHER_DATA_PATH, 'w') as f:
                    json.dump(weather_data, f)
    
    except Exception as e:
        logging.error(f"Error in weather data fetching: {str(e)}")
        raise e
    
    # Guardado final
    with open(WEATHER_DATA_PATH, 'w') as f:
        json.dump(weather_data, f)
    
    logging.info(f"Weather data fetched for {len(weather_data)} cities")
    return weather_data

from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import logging

def fetch_spotify_playlist(**kwargs):
    spotify_conn = BaseHook.get_connection("spotify_api")
    client_id = spotify_conn.login
    client_secret = spotify_conn.password

    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    ))

    playlist_ids = [
        "2ln8x063dYgSJKb1vv7Mn1", # playlist personal
    ]

    playlist_data = []
    try:
        for playlist_id in playlist_ids:
            try:
                results = sp.playlist(playlist_id)
                tracks = sp.playlist_tracks(playlist_id)['items']
                
                playlist_info = {
                    'playlist_id': playlist_id,
                    'playlist_name': results['name'],
                    'playlist_description': results.get('description', ''),
                    'total_tracks': results['tracks']['total'],
                    'tracks': []
                }
                
                # Obtener información básica de cada track
                for track in tracks:
                    if track['track'] and track['track']['id']:
                        track_info = {
                            'track_id': track['track']['id'],
                            'track_name': track['track']['name'],
                            'artist_name': track['track']['artists'][0]['name'],
                            'artist_id': track['track']['artists'][0]['id'],
                            'album_name': track['track']['album']['name'],
                            'duration_ms': track['track']['duration_ms'],
                            'popularity': track['track']['popularity']
                        }
                        playlist_info['tracks'].append(track_info)
                
                playlist_data.append(playlist_info)
                logging.info(f"Playlist {results['name']} processed with {len(playlist_info['tracks'])} tracks")
                
            except Exception as e:
                logging.error(f"Error processing playlist {playlist_id}: {str(e)}")
                continue
        
        # Guardar datos de playlist
        with open(SPOTIFY_PLAYLIST_PATH, 'w') as f:
            json.dump(playlist_data, f)
            
    except Exception as e:
        logging.error(f"Error in Spotify data fetching: {str(e)}")
        raise e
    
    logging.info(f"Processed {len(playlist_data)} playlists")
    return playlist_data


# Tarea 3: Obtener características de audio de las canciones
def fetch_audio_features(**kwargs):
    import os
    import json
    import time
    import logging
    
    os.makedirs(os.path.dirname(AUDIO_FEATURES_PATH), exist_ok=True)
    
    # Cargar datos de playlist
    
    with open(SPOTIFY_PLAYLIST_PATH, 'r') as f:
        playlist_data = json.load(f)
    
    # Obtener credenciales Spotify
    spotify_conn = BaseHook.get_connection("spotify_api")
    client_id = spotify_conn.login
    client_secret = spotify_conn.password
    
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret))
    
    audio_features_data = []
    track_ids = []
    
    # Recopilar todos los IDs de tracks
    for playlist in playlist_data:
        for track in playlist['tracks']:
            if track['track_id'] not in track_ids:
                track_ids.append(track['track_id'])
    
    # Obtener características de audio en lotes (máximo 100 por request)
    try:
        for i in range(0, len(track_ids), 100):
            batch_ids = track_ids[i:i+100]
            features_batch = sp.audio_features(batch_ids)
            
            for j, features in enumerate(features_batch):
                if features:
                    audio_features_data.append({
                        'track_id': batch_ids[j],
                        'danceability': features['danceability'],
                        'energy': features['energy'],
                        'key': features['key'],
                        'loudness': features['loudness'],
                        'mode': features['mode'],
                        'speechiness': features['speechiness'],
                        'acousticness': features['acousticness'],
                        'instrumentalness': features['instrumentalness'],
                        'liveness': features['liveness'],
                        'valence': features['valence'],
                        'tempo': features['tempo'],
                        'duration_ms': features['duration_ms'],
                        'time_signature': features['time_signature']
                    })
            
            # Guardado parcial
            if (i + 100) % 200 == 0:
                with open(AUDIO_FEATURES_PATH, 'w') as f:
                    json.dump(audio_features_data, f)
                logging.info(f"Processed {i + 100} audio features")
            
            # Pausa para no saturar la API
            time.sleep(0.5)
    
    except Exception as e:
        logging.error(f"Error fetching audio features: {str(e)}")
        raise e
    
    # Guardado final
    with open(AUDIO_FEATURES_PATH, 'w') as f:
        json.dump(audio_features_data, f)
    
    logging.info(f"Fetched audio features for {len(audio_features_data)} tracks")
    return audio_features_data

# Tarea 4: Combinar y transformar datos
def merge_and_transform_data(**kwargs):
    import os
    import json
    import pandas as pd
    import logging
    
    execution_date = kwargs['ds']
    FINAL_DATASET_PATH = f"/tmp/music_weather_data/final_dataset_{execution_date}.csv"
    
    os.makedirs(os.path.dirname(FINAL_DATASET_PATH), exist_ok=True)
    
    # Cargar todos los datos
    with open(WEATHER_DATA_PATH, 'r') as f:
        weather_data = json.load(f)
    
    with open(SPOTIFY_PLAYLIST_PATH, 'r') as f:
        playlist_data = json.load(f)
    
    with open(AUDIO_FEATURES_PATH, 'r') as f:
        audio_features = json.load(f)
    
    # Crear DataFrame de características de audio
    audio_features_df = pd.DataFrame(audio_features)
    
    # Crear DataFrame de tracks con información de playlist
    tracks_data = []
    for playlist in playlist_data:
        for track in playlist['tracks']:
            track_info = track.copy()
            track_info['playlist_id'] = playlist['playlist_id']
            track_info['playlist_name'] = playlist['playlist_name']
            tracks_data.append(track_info)
    
    tracks_df = pd.DataFrame(tracks_data)
    
    # Combinar datos de tracks con características de audio
    merged_df = pd.merge(tracks_df, audio_features_df, on='track_id', how='left')
    
    # Función para mapear clima a estado de ánimo
    def weather_to_mood(weather_info):
        temp = weather_info['temperature']
        condition = weather_info['weather_condition']
        
        if condition in ['Rain', 'Drizzle', 'Thunderstorm']:
            if temp > 20:
                return {'mood': 'cozy', 'energy_level': 0.4, 'valence_level': 0.3}
            else:
                return {'mood': 'melancholic', 'energy_level': 0.3, 'valence_level': 0.2}
        elif condition == 'Clear':
            if temp > 25:
                return {'mood': 'happy', 'energy_level': 0.8, 'valence_level': 0.9}
            else:
                return {'mood': 'calm', 'energy_level': 0.6, 'valence_level': 0.7}
        elif condition in ['Clouds', 'Mist', 'Fog']:
            return {'mood': 'thoughtful', 'energy_level': 0.5, 'valence_level': 0.6}
        elif condition == 'Snow':
            return {'mood': 'festive', 'energy_level': 0.7, 'valence_level': 0.8}
        else:
            return {'mood': 'neutral', 'energy_level': 0.5, 'valence_level': 0.5}
    
    # Crear dataset final con recomendaciones por ciudad
    final_data = []
    for city_weather in weather_data:
        mood_profile = weather_to_mood(city_weather)
        
        # Filtrar canciones que coincidan con el perfil de ánimo
        energy_filter = merged_df['energy'].between(
            mood_profile['energy_level'] - 0.2, 
            mood_profile['energy_level'] + 0.2
        )
        valence_filter = merged_df['valence'].between(
            mood_profile['valence_level'] - 0.2, 
            mood_profile['valence_level'] + 0.2
        )
        
        matching_tracks = merged_df[energy_filter & valence_filter]
        
        # Para cada ciudad, tomar las 10 canciones más populares que coincidan
        top_tracks = matching_tracks.nlargest(10, 'popularity')
        
        for _, track in top_tracks.iterrows():
            recommendation = {
                'city': city_weather['city'],
                'weather_condition': city_weather['weather_condition'],
                'temperature': city_weather['temperature'],
                'mood_profile': mood_profile['mood'],
                'target_energy': mood_profile['energy_level'],
                'target_valence': mood_profile['valence_level'],
                'track_id': track['track_id'],
                'track_name': track['track_name'],
                'artist_name': track['artist_name'],
                'album_name': track['album_name'],
                'playlist_name': track['playlist_name'],
                'energy': track['energy'],
                'valence': track['valence'],
                'danceability': track['danceability'],
                'acousticness': track['acousticness'],
                'popularity': track['popularity'],
                'execution_date': execution_date
            }
            final_data.append(recommendation)
    
    # Crear DataFrame final y guardar como CSV
    final_df = pd.DataFrame(final_data)
    final_df.to_csv(FINAL_DATASET_PATH, index=False)
    
    logging.info(f"Final dataset created with {len(final_df)} recommendations")
    return FINAL_DATASET_PATH

# Tarea 5: Generar reporte de recomendaciones
def generate_recommendations_report(**kwargs):
    import os
    import json
    import pandas as pd
    import logging
    
    execution_date = kwargs['ds']
    ti = kwargs['ti']
    
    dataset_path = ti.xcom_pull(task_ids='merge_and_transform_data')
    RECOMMENDATIONS_PATH = f"/tmp/music_weather_data/recommendations_{execution_date}.json"
    
    if not dataset_path or not os.path.exists(dataset_path):
        logging.error("Dataset path not found")
        return
    
    # Leer dataset final
    final_df = pd.read_csv(dataset_path)
    
    # Generar resumen por ciudad
    city_summaries = []
    for city in final_df['city'].unique():
        city_data = final_df[final_df['city'] == city]
        city_weather = city_data.iloc[0]  # Tomar primera fila para datos climáticos
        
        summary = {
            'city': city,
            'weather_condition': city_weather['weather_condition'],
            'temperature': city_weather['temperature'],
            'mood_profile': city_weather['mood_profile'],
            'recommendation_count': len(city_data),
            'top_artists': city_data['artist_name'].value_counts().head(3).to_dict(),
            'avg_energy': city_data['energy'].mean(),
            'avg_valence': city_data['valence'].mean(),
            'recommendations': []
        }
        
        # Añadir top recomendaciones
        for _, track in city_data.head(5).iterrows():
            summary['recommendations'].append({
                'track_name': track['track_name'],
                'artist_name': track['artist_name'],
                'energy': track['energy'],
                'valence': track['valence']
            })
        
        city_summaries.append(summary)
    
    # Guardar reporte
    with open(RECOMMENDATIONS_PATH, 'w') as f:
        json.dump(city_summaries, f, indent=2)
    
    logging.info(f"Recommendations report generated for {len(city_summaries)} cities")
    return RECOMMENDATIONS_PATH

# Tarea 6: Enviar correo con reporte
def send_email_report(**kwargs):
    import smtplib
    import os
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    import json
    import pandas as pd
    
    execution_date = kwargs['ds']
    ti = kwargs['ti']
    
    recommendations_path = ti.xcom_pull(task_ids='generate_recommendations_report')
    dataset_path = f"/tmp/music_weather_data/final_dataset_{execution_date}.csv"
    
    if not recommendations_path or not os.path.exists(recommendations_path):
        print("Recommendations file not found")
        return
    
    # Leer recomendaciones
    with open(recommendations_path, 'r') as f:
        recommendations = json.load(f)
    
    # Configuración de email
    destinatario = "cocenzosilva@gmail.com"
    asunto = f"Music Weather Recommendations Report - {execution_date}"
    
    # Crear cuerpo del email
    cuerpo = f"""
    <h2>Music Weather Recommendations Report - {execution_date}</h2>
    <p>Se han generado recomendaciones musicales basadas en el clima para {len(recommendations)} ciudades.</p>
    
    <h3>Resumen por Ciudad:</h3>
    """
    
    for city in recommendations:
        cuerpo += f"""
        <div style="margin-bottom: 20px; padding: 10px; border: 1px solid #ddd;">
            <h4>{city['city']} - {city['weather_condition']} ({city['temperature']}°C)</h4>
            <p>Perfil de ánimo: <strong>{city['mood_profile']}</strong></p>
            <p>Recomendaciones: {city['recommendation_count']} canciones</p>
            <p>Artistas más recomendados: {', '.join(city['top_artists'].keys())}</p>
            
            <h5>Top Recomendaciones:</h5>
            <ul>
        """
        
        for rec in city['recommendations']:
            cuerpo += f"<li>{rec['track_name']} - {rec['artist_name']} (Energy: {rec['energy']:.2f}, Valence: {rec['valence']:.2f})</li>"
        
        cuerpo += """
            </ul>
        </div>
        """
    
    try:
        # Obtener credenciales desde Airflow Connections
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
        mensaje.attach(MIMEText(cuerpo, "html"))
        
        # Adjuntar archivos
        archivos = [dataset_path, recommendations_path]
        
        for archivo in archivos:
            if os.path.exists(archivo):
                with open(archivo, "rb") as adj:
                    parte = MIMEBase("application", "octet-stream")
                    parte.set_payload(adj.read())
                    encoders.encode_base64(parte)
                    parte.add_header("Content-Disposition", f"attachment; filename={os.path.basename(archivo)}")
                    mensaje.attach(parte)
        
        # Enviar email
        with smtplib.SMTP(servidor, puerto) as server:
            server.starttls()
            server.login(remitente, contraseña)
            server.sendmail(remitente, destinatario, mensaje.as_string())
        
        print(f"Email report sent successfully for {execution_date}")
        
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        raise e

# Definición del DAG
with DAG(
    dag_id='music_weather_recommendation',
    description='DAG para sistema de recomendación musical basado en condiciones climáticas',
    default_args=default_args,
    schedule=None,  
    catchup=False,
    tags=['music', 'weather', 'recommendation', 'spotify']
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    fetch_spotify = PythonOperator(
        task_id='fetch_spotify_playlist',
        python_callable=fetch_spotify_playlist,
    )

    fetch_audio = PythonOperator(
        task_id='fetch_audio_features',
        python_callable=fetch_audio_features,
    )

    merge_data = PythonOperator(
        task_id='merge_and_transform_data',
        python_callable=merge_and_transform_data,
    )

    generate_report = PythonOperator(
        task_id='generate_recommendations_report',
        python_callable=generate_recommendations_report,
    )

    send_email = PythonOperator(
        task_id='send_email_report',
        python_callable=send_email_report,
    )

    # Definir dependencias
    [fetch_weather, fetch_spotify] >> fetch_audio >> merge_data >> generate_report >> send_email