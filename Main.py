# Se modifico el nombre de los campos de la tabla a snake case
# Se agrego una columna update para dejar sentado la fecha de la ultima actualizacion
# Traer Season dinamicamente tomando la fecha actual
# Se aislaron las contraseñas en un archivo config.py

import json  # Importar el módulo json para manejar datos JSON
import psycopg2  # Importar la biblioteca psycopg2 para interactuar con PostgreSQL (Redshift)
import requests  # Importar la biblioteca requests para hacer solicitudes HTTP 
from datetime import datetime #Importar datetime para obtener la fecha actual
import config

# Datos de conexión a Redshift
dbname = 'data-engineer-database'
user = 'francoesalas_coderhouse'
password = config.PWD_REDSHIFT
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'

# Lista de los números de identificación de las cinco grandes ligas europeas dentro de la api-football
big_five_leagues = [61, 39, 78, 135, 140]

# Crear la conexión a Redshift
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()

# Cabeceras para la solicitud HTTP a la API
headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': config.API_KEY
}

try:
    # Crear la tabla en Redshift si no existe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS current_standings (
            rank INT,
            team VARCHAR(255),
            points INT,
            goals_difference INT,
            league_id VARCHAR(255),
            form VARCHAR(255),
            updated DATE
        )
    """)
    #Obtener Fecha y hora
    now = datetime.now()
    date_updated = now.date()
    #Obtener Año de la temporada dinamicamente
    year_now = now.year
    month_now = now.month

    if month_now<=7:
        year_season = year_now-1
    else:
        year_season = year_now
        
    # Iteramos e Insertamos los datos de las ligas en la tabla
    for league_id in big_five_leagues:
        # Eliminar datos previos de esa liga en la tabla
        cur.execute(f"DELETE FROM current_standings WHERE league_id = '{league_id}'")
        
        # Hacer la solicitud HTTP a la API para obtener los datos de la tabla de posiciones
        url = f"https://v3.football.api-sports.io/standings?league={league_id}&season={year_season}"
        response = requests.get(url, headers=headers)
        result_dict = response.json()
        
        # Verificar si la respuesta contiene datos válidos
        if "response" in result_dict and result_dict["response"]:
            standings = result_dict["response"][0]["league"]["standings"][0]
            # Iterar sobre cada equipo en la tabla de posiciones
            for team in standings:
                # Extraer los datos de cada equipo
                rank = team["rank"]
                team_name = team["team"]["name"]
                points = team["points"]
                goals_diff = team["goalsDiff"]
                league_id = team["group"]
                form = team["form"]
                # Insertar los datos en la tabla current_standings en Redshift
                cur.execute("""
                    INSERT INTO current_standings (rank, team, points, goals_difference, league_id, form,updated)
                    VALUES (%s, %s, %s, %s, %s, %s, TO_DATE(%s, 'YYYY-MM-DD'))
                """, (rank, team_name, points, goals_diff, league_id, form,date_updated))
    
    # Confirmar la transacción
    conn.commit()
    print("Los datos se han cargado exitosamente en la tabla current_standings en Redshift.")

except Exception as e:
    print("Ocurrió un error:", e)
    conn.rollback()

finally:
    # Cerrar la conexión y el cursor
    cur.close()
    conn.close()
