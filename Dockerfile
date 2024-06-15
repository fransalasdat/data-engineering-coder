# Usar una imagen base oficial de Python
FROM python:3.8

# Información del autor y la versión
LABEL version="1.0"
LABEL author="fransalasdat"

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar los archivos necesarios al directorio de trabajo del contenedor
COPY main.py requirements.txt ./

# Actualizar pip e instalar las dependencias
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Definir el punto de entrada del contenedor
ENTRYPOINT ["python", "main.py"]