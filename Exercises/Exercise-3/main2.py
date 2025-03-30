import boto3
import pandas as pd
import io
from botocore import UNSIGNED
from botocore.config import Config
import matplotlib.pyplot as plt
import seaborn as sns

"""
EJERCICIO: ANÁLISIS DE DATOS METEOROLÓGICOS PÚBLICOS DE NOAA

Este ejercicio utiliza el conjunto de datos público de la NOAA (National Oceanic and Atmospheric Administration)
disponible en AWS Open Data. Extraeremos datos meteorológicos, los procesaremos y realizaremos 
un análisis básico.

El bucket 'noaa-gsod-pds' contiene datos meteorológicos globales históricos y es de acceso público.
"""

# Configuración para acceso sin credenciales a buckets públicos
def get_s3_client():
    return boto3.client(
        's3',
        config=Config(signature_version=UNSIGNED, 
        retries={'max_attempts': 10})
    )

def list_sample_files(bucket_name, prefix, max_files=10):
    """Lista algunos archivos de ejemplo en el bucket"""
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=max_files)
        if 'Contents' in response:
            print(f"Archivos encontrados en {prefix}:")
            for obj in response['Contents']:
                print(f"  - {obj['Key']}")
            return [obj['Key'] for obj in response['Contents']]
        else:
            print(f"No se encontraron archivos en {prefix}")
            return []
    except Exception as e:
        print(f"Error al listar archivos: {e}")
        return []

def download_and_process_weather_data(bucket_name, file_key):
    """Descarga y procesa un archivo de datos meteorológicos"""
    s3 = get_s3_client()
    try:
        print(f"Descargando {file_key}...")
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        
        # Los archivos GSOD son archivos de texto con formato fijo
        # Convertimos los datos a un DataFrame de pandas
        df = pd.read_csv(
            io.BytesIO(response['Body'].read()),
            delimiter=',',
            header=0,
            low_memory=False
        )
        
        return df
    except Exception as e:
        print(f"Error al descargar o procesar el archivo {file_key}: {e}")
        return None

def analyze_weather_data(df):
    """Realiza un análisis básico de los datos meteorológicos"""
    if df is None or df.empty:
        print("No hay datos para analizar")
        return
    
    print("\n--- ANÁLISIS DE DATOS METEOROLÓGICOS ---")
    print(f"Rango de fechas: {df['DATE'].min()} a {df['DATE'].max()}")
    
    # Convertir columna de fecha a tipo datetime
    df['DATE'] = pd.to_datetime(df['DATE'])
    
    # Resumen estadístico de temperaturas
    print("\nResumen estadístico de temperaturas (°F):")
    temp_stats = df['TEMP'].describe()
    for stat, value in temp_stats.items():
        print(f"  {stat}: {value:.2f}")
    
    # Calcular temperaturas promedio por mes
    df['month'] = df['DATE'].dt.month
    monthly_temps = df.groupby('month')['TEMP'].mean()
    
    print("\nTemperaturas promedio por mes (°F):")
    for month, temp in monthly_temps.items():
        print(f"  Mes {month}: {temp:.2f}°F")
    
    # Encontrar día más caluroso
    hottest_day = df.loc[df['TEMP'].idxmax()]
    print(f"\nDía más caluroso: {hottest_day['DATE'].strftime('%Y-%m-%d')} con {hottest_day['TEMP']:.2f}°F")
    
    # Encontrar día más frío
    coldest_day = df.loc[df['TEMP'].idxmin()]
    print(f"Día más frío: {coldest_day['DATE'].strftime('%Y-%m-%d')} con {coldest_day['TEMP']:.2f}°F")
    
    # Analizar precipitaciones
    rain_days = df[df['PRCP'] > 0]
    print(f"\nDías con precipitación: {len(rain_days)} ({len(rain_days)/len(df)*100:.2f}%)")
    if not rain_days.empty:
        max_rain = rain_days.loc[rain_days['PRCP'].idxmax()]
        print(f"Día con mayor precipitación: {max_rain['DATE'].strftime('%Y-%m-%d')} con {max_rain['PRCP']:.2f} pulgadas")
    
    print("\nAnálisis completo!")
    
    return {
        'df': df,
        'monthly_temps': monthly_temps
    }

def plot_weather_data(analysis_results):
    """Crea visualizaciones de los datos meteorológicos"""
    if analysis_results is None:
        print("No hay datos para visualizar")
        return
    
    df = analysis_results['df']
    monthly_temps = analysis_results['monthly_temps']
    
    # Configurar el estilo de las gráficas
    sns.set(style="whitegrid")
    
    # Gráfica de temperaturas mensuales
    plt.figure(figsize=(12, 6))
    monthly_temps.plot(kind='bar', color='skyblue')
    plt.title('Temperatura Promedio por Mes')
    plt.xlabel('Mes')
    plt.ylabel('Temperatura (°F)')
    plt.tight_layout()
    plt.savefig('temperaturas_mensuales.png')
    print("Gráfica guardada como 'temperaturas_mensuales.png'")
    
    # Gráfica de temperaturas diarias
    plt.figure(figsize=(12, 6))
    plt.plot(df['DATE'], df['TEMP'], 'b-', alpha=0.7)
    plt.title('Temperaturas Diarias')
    plt.xlabel('Fecha')
    plt.ylabel('Temperatura (°F)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('temperaturas_diarias.png')
    print("Gráfica guardada como 'temperaturas_diarias.png'")
    
    # Histograma de precipitaciones
    plt.figure(figsize=(12, 6))
    rain_data = df[df['PRCP'] > 0]['PRCP']
    if not rain_data.empty:
        sns.histplot(rain_data, bins=20, kde=True, color='blue')
        plt.title('Distribución de Precipitaciones')
        plt.xlabel('Precipitación (pulgadas)')
        plt.ylabel('Frecuencia')
        plt.tight_layout()
        plt.savefig('histograma_precipitaciones.png')
        print("Gráfica guardada como 'histograma_precipitaciones.png'")
    else:
        print("No hay datos de precipitación para visualizar")

def main():
    # Bucket público de NOAA Global Surface Summary of the Day
    BUCKET_NAME = "noaa-gsod-pds"
    
    # Primer paso: listar algunos archivos para ver la estructura
    print("Explorando la estructura del bucket...")
    list_sample_files(BUCKET_NAME, "2022/", max_files=5)
    
    # Seleccionar uno de los archivos encontrados
    print("\nListando más archivos para elegir uno para análisis...")
    more_files = list_sample_files(BUCKET_NAME, "2022/01", max_files=5)
    
    if more_files:
        # Seleccionar un archivo para análisis
        selected_file = more_files[0]
        print(f"\nSeleccionando archivo para análisis: {selected_file}")
        
        # Descargar y procesar el archivo
        weather_df = download_and_process_weather_data(BUCKET_NAME, selected_file)
        
        # Analizar los datos
        analysis_results = analyze_weather_data(weather_df)
        
        # Visualizar los datos
        plot_weather_data(analysis_results)
    else:
        print("No se encontraron archivos para analizar.")
        
        # Intento alternativo con otro prefijo
        print("\nIntentando con otro prefijo...")
        alt_files = list_sample_files(BUCKET_NAME, "", max_files=10)
        if alt_files:
            print("\nEstructura general del bucket:")
            for file_key in alt_files:
                if not file_key.endswith('/'):  # Evitar directorios
                    selected_file = file_key
                    print(f"\nSeleccionando archivo para análisis: {selected_file}")
                    
                    # Descargar y procesar el archivo
                    weather_df = download_and_process_weather_data(BUCKET_NAME, selected_file)
                    
                    # Analizar los datos
                    analysis_results = analyze_weather_data(weather_df)
                    
                    # Visualizar los datos
                    plot_weather_data(analysis_results)
                    break  # Solo procesar un archivo

if __name__ == "__main__":
    main()