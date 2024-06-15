import json
import psycopg2
import requests
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
import logging

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las variables de entorno
API_KEY = os.getenv('API_KEY')
PWD_REDSHIFT = os.getenv('PWD_REDSHIFT')

logging.info("Variables de entorno cargadas.")

# Cabeceras para la solicitud HTTP a la API
headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': API_KEY
}

# Lista de los números de identificación de las cinco grandes ligas europeas dentro de la api-football
big_five_leagues = [61, 39, 78, 135, 140]

# Crear un DataFrame vacío para almacenar los datos agregados de todas las temporadas
df = pd.DataFrame()

# Obtener Año de la temporada dinámicamente
now = datetime.now()
year_now = now.year
month_now = now.month
year_season = year_now - 1 if month_now <= 7 else year_now

# Extraer datos para cada liga
for league_id in big_five_leagues:
    url = f"https://v3.football.api-sports.io/standings?league={league_id}&season={year_season}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if "response" in data and data["response"]:
            standings = data["response"][0]["league"]["standings"][0]
            df_normalize = pd.json_normalize(standings)
            df_normalize['league_id'] = league_id
            df = pd.concat([df, df_normalize], ignore_index=True)
            logging.info(f"Datos extraídos para la liga {league_id}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener datos de la liga {league_id}: {e}")
        
# Listar columnas antes del ETL
#logging.info("Columnas del DataFrame antes del ETL: %s", df.columns.tolist())
#print("Columnas del DataFrame antes del ETL:", df.columns.tolist())

# ETL
df = df.drop(['status', 'team.logo'], axis=1)
df.columns = [
    'rank', 'points', 'goals_diff', 'league', 'lasts_match', 'current_status', 'update', 'team_id',
    'team_name', 'played', 'win', 'draw', 'lose', 'all_goals_for', 'all_goals_against', 'home_played',
    'home_win', 'home_draw', 'home_lose', 'home_goals_for', 'home_goals_against', 'away_played',
    'away_win', 'away_draw', 'away_lose', 'away_goals_for', 'away_goals_against', 'league_id'
]

df['current_status'] = df['current_status'].fillna('Stable')
df['update'] = pd.to_datetime(df['update'], errors='coerce').dt.date
df = df[['update', 'league_id', 'league', 'rank', 'current_status', 'team_id', 'team_name', 'points', 'goals_diff', 'lasts_match', 'played', 'win', 'draw', 'lose', 'all_goals_for', 'all_goals_against', 'home_played', 'home_win', 'home_draw', 'home_lose', 'home_goals_for', 'home_goals_against', 'away_played', 'away_win', 'away_draw', 'away_lose', 'away_goals_for', 'away_goals_against']]
matches_df = df['lasts_match'].apply(lambda x: pd.Series(list(x)))
matches_df.columns = ['last_match_result', 'last_match_result_2', 'last_match_result_3', 'last_match_result_4', 'last_match_result_5']
df = pd.concat([df, matches_df], axis=1).drop('lasts_match', axis=1)
df['league_id'] = df['league_id'].astype(str).apply(lambda x: x.zfill(3))
df['rank'] = df['rank'].astype(str).apply(lambda x: x.zfill(2))
df['primary_key'] = df['league_id'] + df['rank']
df.insert(0, 'primary_key', df.pop('primary_key'))
df['primary_key'] = df['primary_key'].astype(str).str[:5]
df['league_id'] = df['league_id'].astype(int)
df['rank'] = df['rank'].astype(int)

logging.info("Transformación de datos completada.")

# Conectar a Redshift
dbname = 'data-engineer-database'
user = 'francoesalas_coderhouse'
password = PWD_REDSHIFT
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'

try:
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    logging.info("Conexión a Redshift exitosa.")
except psycopg2.Error as e:
    logging.error(f"Error al conectar a Redshift: {e}")
    raise

# Crear tabla en Redshift
table_name = 'current_standings'
try:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            primary_key VARCHAR(5),
            update DATE,
            league_id INTEGER,
            league VARCHAR(50),
            rank INTEGER,
            current_status VARCHAR(100),
            team_id INTEGER,
            team_name VARCHAR(100),
            points INTEGER,
            goals_diff INTEGER,
            played INTEGER,
            win INTEGER,
            draw INTEGER,
            lose INTEGER,
            all_goals_for INTEGER,
            all_goals_against INTEGER,
            home_played INTEGER,
            home_win INTEGER,
            home_draw INTEGER,
            home_lose INTEGER,
            home_goals_for INTEGER,
            home_goals_against INTEGER,
            away_played INTEGER,
            away_win INTEGER,
            away_draw INTEGER,
            away_lose INTEGER,
            away_goals_for INTEGER,
            away_goals_against INTEGER,
            last_match_result VARCHAR(10),
            last_match_result_2 VARCHAR(10),
            last_match_result_3 VARCHAR(10),
            last_match_result_4 VARCHAR(10),
            last_match_result_5 VARCHAR(10)
        )
    """)
    conn.commit()
    logging.info("Tabla creada o verificada exitosamente en Redshift.")
except psycopg2.Error as e:
    logging.error(f"Error al crear la tabla en Redshift: {e}")
    raise

# Llenar la tabla en Redshift
rows = df.to_dict(orient='records')
insert_query = f"""
    INSERT INTO {table_name} (
        primary_key, update, league_id, league, rank, current_status, team_id, team_name, points, goals_diff,
        played, win, draw, lose, all_goals_for, all_goals_against, home_played,
        home_win, home_draw, home_lose, home_goals_for, home_goals_against, away_played,
        away_win, away_draw, away_lose, away_goals_for, away_goals_against,
        last_match_result, last_match_result_2, last_match_result_3, last_match_result_4, last_match_result_5
    ) VALUES (
        %(primary_key)s, %(update)s, %(league_id)s, %(league)s, %(rank)s, %(current_status)s, %(team_id)s, %(team_name)s, %(points)s, %(goals_diff)s,
        %(played)s, %(win)s, %(draw)s, %(lose)s, %(all_goals_for)s, %(all_goals_against)s, %(home_played)s,
        %(home_win)s, %(home_draw)s, %(home_lose)s, %(home_goals_for)s, %(home_goals_against)s, %(away_played)s,
        %(away_win)s, %(away_draw)s, %(away_lose)s, %(away_goals_for)s, %(away_goals_against)s,
        %(last_match_result)s, %(last_match_result_2)s, %(last_match_result_3)s, %(last_match_result_4)s, %(last_match_result_5)s
    )
"""
try:
    cur.executemany(insert_query, rows)
    conn.commit()
    logging.info("Datos insertados exitosamente en Redshift.")
except psycopg2.Error as e:
    logging.error(f"Error al insertar datos: {e}")
    conn.rollback()

cur.close()
conn.close()
logging.info("Conexión a Redshift cerrada.")