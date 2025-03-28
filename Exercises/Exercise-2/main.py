import requests
import pandas as pd
from bs4 import BeautifulSoup

# URL base de los archivos
BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

def get_filename_by_date(target_date):
    """Scrapea la página y encuentra el nombre del archivo con la fecha específica."""
    response = requests.get(BASE_URL)
    response.raise_for_status()  # Asegura que la solicitud fue exitosa

    soup = BeautifulSoup(response.text, "html.parser")
    table_rows = soup.find_all("tr")

    for row in table_rows:
        cols = row.find_all("td")
        if len(cols) >= 2:  # Asegurarse de que tiene columnas suficientes
            filename = cols[0].text.strip()
            last_modified = cols[1].text.strip()

            if last_modified == target_date:
                return filename  # Retornar el nombre del archivo correcto

    return None  # Si no se encuentra el archivo

def download_file(filename):
    """Descarga el archivo desde la URL y lo guarda localmente."""
    file_url = BASE_URL + filename
    response = requests.get(file_url)
    response.raise_for_status()  # Verifica que la descarga fue exitosa

    with open(filename, "wb") as file:
        file.write(response.content)

    return filename  # Retorna el nombre del archivo guardado

def process_data(filename):
    """Abre el archivo con Pandas, encuentra la temperatura más alta y la imprime."""
    df = pd.read_csv(filename)

    if "HourlyDryBulbTemperature" in df.columns:
        max_temp = df["HourlyDryBulbTemperature"].max()
        max_temp_rows = df[df["HourlyDryBulbTemperature"] == max_temp]
        print(max_temp_rows)
    else:
        print("No se encontró la columna HourlyDryBulbTemperature en el archivo.")

def main():
    target_date = "2024-01-19 10:27"

    print("Buscando archivo...")
    filename = get_filename_by_date(target_date)

    if filename:
        print(f"Archivo encontrado: {filename}")
        print("Descargando archivo...")
        local_filename = download_file(filename)

        print("Procesando datos...")
        process_data(local_filename)
    else:
        print("No se encontró el archivo con la fecha especificada.")

if __name__ == "__main__":
    main()
