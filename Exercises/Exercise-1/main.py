import os # Para manejar rutas y crear carpetas
import requests # Para descargar archivos desde la web
import zipfile # Para descomprimir archivos ZIP


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOAD_FOLDER = "downloads" # Carpeta donde se guardarán los archivos descargados y descomprimidos

def download_file(url, folder):
    """Descarga un archivo desde una URL y lo guarda en la carpeta de descargas."""
    filename = url.split("/")[-1] # Obtiene el nombre del archivo de la URL
    filepath = os.path.join(folder, filename) # Ruta donde se guardará el archivo

    try:
        response = requests.get(url, stream=True) # Descarga el archivo en fragmentos
        response.raise_for_status() # Lanza una excepción si la descarga falla

       
        with open(filepath, "wb") as file:  # Guarda el archivo en la carpeta de descargas
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk) # Descarga en bloques pequeños
        print(f"✅ Descargado: {filename}")
        return filepath # Devuelve la ruta del archivo descargado
    
    except requests.exceptions.RequestException as e: # Captura errores de descarga
        print(f"❌ Error al descargar {filename}: {e}") # Muestra un mensaje de error
        return None


def unzip_file(zip_path, folder):
    """Descomprime un archivo ZIP en la carpeta especificada"""
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref: # Abre el archivo ZIP en modo lectura
            zip_ref.extractall(folder) # Extrae todo el contenido en la carpeta de descargas
        print(f"✅ Descomprimido: {zip_path}")
        os.remove(zip_path) # Elimina el archivo ZIP después de descomprimirlo
    
    except zipfile.BadZipFile:
        print(f"❌ Archivo ZIP corrupto: {zip_path}")

def main():
    """Funcion principal para descargar y descomprimir archivos"""

    # Crear la carpeta de descargas si no existe
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    for url in download_uris:
        zip_path = download_file(url, DOWNLOAD_FOLDER) # Descarga el archivo y obtiene su ruta
        if zip_path:
            unzip_file(zip_path, DOWNLOAD_FOLDER) # Descomprime el archivo descargado
    
    print("✅ Proceso completado!")
    
if __name__ == "__main__":
    main()
