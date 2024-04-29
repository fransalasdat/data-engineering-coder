import http.client  # Importar el módulo http.client para hacer solicitudes HTTP
import json  # Importar el módulo json para manejar datos JSON
import psycopg2  # Importar la biblioteca psycopg2 para interactuar con PostgreSQL (Redshift)

# Datos de conexión a Redshift
dbname = 'data-engineer-database'
user = 'francoesalas_coderhouse'
password = 'eHgW2401vJ'
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
    'x-rapidapi-key': "bbf04fb6aac9b84535835019b45f55a2"
}

try:
    # Crear la tabla en Redshift si no existe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS current_standings (
            Rank INT,
            Team VARCHAR(255),
            Points INT,
            Goals_Difference INT,
            League_id VARCHAR(255),
            Form VARCHAR(255)
        )
    """)

    # Insertar los datos de las ligas en la tabla
    for league_id in big_five_leagues:
        # Eliminar datos previos de la liga en la tabla
        cur.execute(f"DELETE FROM current_standings WHERE League_id = '{league_id}'")
        
        # Hacer la solicitud HTTP a la API para obtener los datos de la tabla de posiciones
        conn_api = http.client.HTTPSConnection("v3.football.api-sports.io")
        conn_api.request("GET", f"/standings?league={league_id}&season=2023", headers=headers)
        res = conn_api.getresponse()
        data = res.read()
        decoded_data = data.decode("utf-8")
        result_dict = json.loads(decoded_data)
        conn_api.close()
        
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
                    INSERT INTO current_standings (Rank, Team, Points, Goals_Difference, League_id, Form)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (rank, team_name, points, goals_diff, league_id, form))

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