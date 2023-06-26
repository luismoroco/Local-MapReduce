import os
import string
from multiprocessing.pool import ThreadPool

# Ruta del directorio que contiene los archivos .txt
directorio = 'ruta/del/directorio'

# Obtener la lista de archivos en el directorio
archivos_txt = [archivo for archivo in os.listdir(directorio) if archivo.endswith('.txt')]

# Función para eliminar los signos de puntuación
def eliminar_puntuacion(texto):
    sin_puntuacion = texto.translate(str.maketrans('', '', string.punctuation))
    return sin_puntuacion

# Función para procesar un archivo
def procesar_archivo(archivo):
    ruta_archivo = os.path.join(directorio, archivo)
    with open(ruta_archivo, 'r') as f:
        contenido = f.read()
        contenido = contenido.lower()  # Convertir a minúsculas
        contenido = eliminar_puntuacion(contenido)  # Eliminar signos de puntuación
        num_caracteres = len(contenido)
        return archivo, num_caracteres

# Crear un ThreadPool con 4 hilos
pool = ThreadPool(4)

# Procesar los archivos en paralelo utilizando map_async
resultados = pool.map_async(procesar_archivo, archivos_txt)

# Esperar a que todos los archivos sean procesados
resultados.wait()

# Obtener los resultados
resultados = resultados.get()

# Imprimir los resultados
for archivo, num_caracteres in resultados:
    print(f"El archivo {archivo} tiene {num_caracteres} caracteres.")

# Cerrar el ThreadPool
pool.close()
pool.join()


"""
for file in files:
    with open(file, 'r', encoding='latin-1') as f:
        content = f.read()
        content = formatText(content)
        print(len(content))
"""