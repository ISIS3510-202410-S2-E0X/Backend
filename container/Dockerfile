# Usar una imagen base oficial de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar el archivo local al contenedor
COPY foodbook-back-firebase-adminsdk-to94a-90fe879afa.json .

# Copiar los archivos de la aplicación FastAPI al directorio de trabajo
COPY ./app /app

# Instalar las dependencias necesarias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto en el que FastAPI correrá
EXPOSE 8000

# Comando para ejecutar la aplicación FastAPI usando uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
