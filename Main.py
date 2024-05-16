# Se modifico el nombre de los campos de la tabla a snake case
# Se agrego una columna update para dejar sentado la fecha de la ultima actualizacion
# Traer Season dinamicamente tomando la fecha actual
# Se aislaron las contraseñas en un archivo config.py

import json
import psycopg2
import requests
from datetime import datetime
import pandas as pd
import config

#################### Extraer Datos de la API ########################################
# Cabeceras para la solicitud HTTP a la API
headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': config.API_KEY
}

# Lista de los números de identificación de las cinco grandes ligas europeas dentro de la api-football
big_five_leagues = [61, 39, 78, 135, 140]

# Crear un DataFrame vacío para almacenar los datos agregados de todas las temporadas
df = pd.DataFrame()

# Obtener Año de la temporada dinámicamente
now = datetime.now()
year_now = now.year
month_now = now.month
if month_now <= 7:
    year_season = year_now - 1
else:
    year_season = year_now

# Extraigo datos para una liga para elultimo año
for league_id in big_five_leagues:
    url = f"https://v3.football.api-sports.io/standings?league={league_id}&season={year_season}"
    response = requests.get(url, headers=headers)
    data = response.json()
    
    if "response" in data and data["response"]:
        standings = data["response"][0]["league"]["standings"][0]
        df_normalize = pd.json_normalize(standings)
        # Agregar una columna league_id con el ID de la liga correspondiente
        df_normalize['league_id'] = league_id
        df = pd.concat([df, df_normalize])  

########################### ETL ####################################
#1 Eliminando columnas innecesarias
df = df.drop(['status', 'team.logo'], axis=1)
#2 Cambiar el nombre de las columnas
rename = ['rank','points','goals_diff','league','lasts_match','current_status','update','team_id','team_name','played','win','draw','lose','all_goals_for','all_goals_against','home_played','home_win','home_draw','home_lose','home_goals_for','home_goals_against','away_played','away_win','away_draw','away_lose','away_goals_for','away_goals_against','league_id']
df.columns = rename
#3 Completando NaN de algunas columnas
df['current_status'] = df['current_status'].fillna('Stable')
#4 Convertir la columna 'update' a tipo datetime
df['update'] = pd.to_datetime(df['update'], errors='coerce').dt.date
#5 Reordenar Columnas
df = df[['update','league_id','league','rank','current_status','team_id','team_name','points','goals_diff','lasts_match','played','win','draw','lose','all_goals_for','all_goals_against','home_played','home_win','home_draw','home_lose','home_goals_for','home_goals_against','away_played','away_win','away_draw','away_lose','away_goals_for','away_goals_against']]
#6 Dividir la columna 'lasts_matches' en letras y expandir en nuevas columnas
matches_df = df['lasts_match'].apply(lambda x: pd.Series(list(x)))
#7 Renombrar las nuevas columnas
matches_df.columns = ['last_match_result', '2_last_match_result', '3_last_match_result', '4_last_match_result', '5_last_match_result']
#8 Concateno Tablas
df = pd.concat([df, matches_df], axis=1)
#9 Elimino Last matches
df = df.drop('lasts_match', axis=1)
# Agregar cero a la izquierda a 'league_id' si tiene dos dígitos
df['league_id'] = df['league_id'].astype(str).apply(lambda x: x.zfill(3))
# Agregar cero a la izquierda a 'rank' si tiene un solo dígito
df['rank'] = df['rank'].astype(str).apply(lambda x: x.zfill(2))
# Concatenar 'league_id' y 'rank' en una nueva columna llamada 'primary_key'
df['primary_key'] = df['league_id'] + df['rank']
# Mover la columna 'league_id' al principio del DataFrame
df.insert(0, 'primary_key', df.pop('primary_key'))
# Convertir la columna 'primary_key' a tipo VARCHAR(5)
df['primary_key'] = df['primary_key'].astype(str).str[:5]
# Volver a convertir 'league_id' y 'rank' a tipo de datos entero
df['league_id'] = df['league_id'].astype(int)
df['rank'] = df['rank'].astype(int)

######################## Conectar a Redshift #######################################
# Datos de conexión a Redshift
dbname = 'data-engineer-database'
user = 'francoesalas_coderhouse'
password = config.PWD_REDSHIFT
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'

#Conexion y Cursor
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()

######################### Crear Tabla ###############################################
# Nombre de la tabla en Redshift
table_name = 'current_standings'

# Crear la tabla en Redshift
cur.execute("""
    CREATE TABLE IF NOT EXISTS {} (
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

    """.format(table_name))

# Hacer commit para guardar los cambios
conn.commit()

################################ Llenar Tabla #########################################

# Iterar sobre cada fila del DataFrame y ejecutar un INSERT INTO para cada una
for index, row in df.iterrows():
    cur.execute("""
        INSERT INTO francoesalas_coderhouse.current_standings (
            primary_key,update,league_id ,league, rank, current_status, team_id, team_name, points, goals_diff,
            played, win, draw, lose, all_goals_for, all_goals_against, home_played,
            home_win, home_draw, home_lose, home_goals_for, home_goals_against, away_played,
            away_win, away_draw, away_lose, away_goals_for, away_goals_against,
            last_match_result, last_match_result_2, last_match_result_3, last_match_result_4, last_match_result_5
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, row.tolist())

# Confirmar la transacción y cerrar la conexión
conn.commit()
cur.close()
conn.close()
