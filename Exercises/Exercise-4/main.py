import os
import json
import csv
import glob

def aplanar_json(datos, prefijo=""):
    """
    Función para aplanar estructuras JSON anidadas.
    Convierte estructuras anidadas como {"type":"Point","coordinates":[-99.9,16.88333]}
    en columnas separadas como type, coordinates_0, coordinates_1
    """
    resultado = {}
    
    # Procesar cada campo del JSON
    for clave, valor in datos.items():
        nueva_clave = f"{prefijo}{clave}"
        
        # Si el valor es un diccionario, aplanarlo recursivamente
        if isinstance(valor, dict):
            resultado.update(aplanar_json(valor, f"{nueva_clave}_"))
        # Si el valor es una lista, convertir cada elemento en columnas separadas
        elif isinstance(valor, list):
            for i, item in enumerate(valor):
                if isinstance(item, (dict, list)):
                    resultado.update(aplanar_json({str(i): item}, f"{nueva_clave}_"))
                else:
                    resultado[f"{nueva_clave}_{i}"] = item
        else:
            resultado[nueva_clave] = valor
            
    return resultado

def json_a_csv(ruta_json, ruta_csv):
    """
    Convierte un archivo JSON a CSV, aplanando las estructuras anidadas.
    """
    # Leer el archivo JSON
    with open(ruta_json, 'r', encoding='utf-8') as archivo_json:
        datos = json.load(archivo_json)
    
    # Si los datos están en una lista, procesar cada elemento
    if isinstance(datos, list):
        # Aplanar cada elemento de la lista
        datos_aplanados = [aplanar_json(item) for item in datos]
        
        # Obtener todas las claves únicas para las columnas del CSV
        todas_claves = set()
        for item in datos_aplanados:
            todas_claves.update(item.keys())
        
        # Ordenar las claves para tener un orden consistente
        claves_ordenadas = sorted(todas_claves)
        
        # Escribir el archivo CSV
        with open(ruta_csv, 'w', newline='', encoding='utf-8') as archivo_csv:
            escritor = csv.DictWriter(archivo_csv, fieldnames=claves_ordenadas)
            escritor.writeheader()
            escritor.writerows(datos_aplanados)
    else:
        # Si es un solo objeto JSON, aplanarlo y escribir el CSV
        datos_aplanados = aplanar_json(datos)
        
        with open(ruta_csv, 'w', newline='', encoding='utf-8') as archivo_csv:
            escritor = csv.DictWriter(archivo_csv, fieldnames=datos_aplanados.keys())
            escritor.writeheader()
            escritor.writerow(datos_aplanados)
            
    print(f"Convertido: {ruta_json} -> {ruta_csv}")

def main():
    # Definir la ruta base
    directorio_base = "data"
    
    # Buscar todos los archivos JSON en la estructura de directorios
    patron_busqueda = os.path.join(directorio_base, "**", "*.json")
    archivos_json = glob.glob(patron_busqueda, recursive=True)
    
    print(f"Se encontraron {len(archivos_json)} archivos JSON:")
    
    # Procesar cada archivo JSON
    for ruta_json in archivos_json:
        # Generar la ruta del archivo CSV correspondiente
        ruta_csv = os.path.splitext(ruta_json)[0] + ".csv"
        print(f"Procesando: {ruta_json}")
        
        # Convertir de JSON a CSV
        try:
            json_a_csv(ruta_json, ruta_csv)
        except Exception as e:
            print(f"Error al procesar {ruta_json}: {str(e)}")

if __name__ == "__main__":
    main()