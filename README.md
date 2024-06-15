# Coder Project Salas

Este proyecto extrae datos de una API de fútbol y los inserta en una base de datos Redshift usando Docker.

## Archivos del Proyecto

- `.env`: Contiene las variables de entorno (API keys, contraseñas).
- `.gitignore`: Archivos que no deben ser incluidos en el repositorio.
- `docker-compose.yml`: Configuración para Docker Compose.
- `Dockerfile`: Configuración para construir la imagen Docker.
- `main.py`: Script principal que realiza la extracción y carga de datos.
- `requirements.txt`: Lista de dependencias de Python.

## Cómo Ejecutar el Proyecto

### Construir la Imagen Docker

```sh
docker build -t coder_project_salas .