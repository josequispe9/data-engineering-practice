import boto3
import gzip
import io
from botocore.exceptions import NoCredentialsError
from botocore import UNSIGNED
from botocore.config import Config

# Nombre del bucket y la clave del archivo .gz en S3
BUCKET_NAME = "commoncrawl"
GZ_FILE_KEY = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

def download_s3_file(bucket_name, key):
    """ Descarga un archivo de S3 y lo devuelve como un objeto en memoria """
    # Usar UNSIGNED para acceder a buckets públicos sin credenciales
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED, retries={"max_attempts": 10}))
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        return response["Body"].read()
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")
        raise

def extract_first_uri(gz_data):
    """ Extrae la primera línea del archivo .gz en memoria """
    with gzip.GzipFile(fileobj=io.BytesIO(gz_data), mode="rt") as f:
        return f.readline().strip()  # Obtener la primera línea y eliminar espacios en blanco

def stream_s3_file(bucket_name, file_key):
    """ Descarga y muestra línea por línea un archivo de S3 sin cargarlo en memoria """
    # Usar UNSIGNED para acceder a buckets públicos sin credenciales
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED, retries={"max_attempts": 10}))
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = response["Body"]

        # Leer y mostrar línea por línea sin cargar todo el archivo en memoria
        for line in body.iter_lines():
            print(line.decode("utf-8"))
    except Exception as e:
        print(f"Error al transmitir el archivo: {e}")
        raise

def main():
    try:
        print("Descargando el archivo .gz desde S3...")
        gz_data = download_s3_file(BUCKET_NAME, GZ_FILE_KEY)

        print("Extrayendo la primera URI...")
        first_uri = extract_first_uri(gz_data)
        print(f"Primer URI obtenido: {first_uri}")

        print("Descargando y mostrando el archivo de la URI obtenida...")
        stream_s3_file(BUCKET_NAME, first_uri)
    except Exception as e:
        print(f"Error en la ejecución principal: {e}")

if __name__ == "__main__":
    main()