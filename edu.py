import os
import threading
import string
import time
from collections import defaultdict
import matplotlib.pyplot as plt

def mapper(files, results):
    word_counts = defaultdict(int)
    total_words = 0

    for file in files:
        with open(file, 'r', encoding='latin-1') as f:
            for line in f:
                line = line.strip().lower()
                words = line.translate(str.maketrans('', '', string.punctuation)).split()

                for word in words:
                    word_counts[word] += 1
                    total_words += 1

    results.append((word_counts, total_words))

def reducer(results_list):
    word_counts = defaultdict(int)
    total_words = 0

    for word_count, count in results_list:
        for word, word_count in word_count.items():
            word_counts[word] += word_count
        total_words += count

    return word_counts, total_words

def get_files(directory):
    file_list = []

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_list.append(file_path)

    return file_list

def process_data(percentage, num_threads, file_names):
    num_files = int(len(file_names) * percentage)
    files_to_process = file_names[:num_files]

    threads = []
    results = []

    # Iniciar la medición del tiempo de procesamiento
    start_time = time.time()

    # Crear e iniciar los hilos de ejecución
    for i in range(num_threads):
        start = i * len(files_to_process) // num_threads
        end = (i + 1) * len(files_to_process) // num_threads
        file_slice = files_to_process[start:end]

        thread = threading.Thread(target=mapper, args=(file_slice, results))
        thread.start()
        threads.append(thread)

    # Esperar a que todos los hilos de ejecución terminen
    for thread in threads:
        thread.join()

    # Aplicar la función de reducción
    word_counts, total_words = reducer(results)

    # Finalizar la medición del tiempo de procesamiento
    end_time = time.time()

    return end_time - start_time, total_words

def main():
    data_path = '/home/lmoroco/Documentos/5to/BigData/labs/Gutenberg_Text'  # Ruta donde se encuentra la carpeta Gutenberg_Text-master
    file_names = get_files(data_path)

    percentages = [0.25, 0.5, 1.0]  # Porcentajes del conjunto de datos a utilizar
    num_processors = os.cpu_count()  # Número de procesadores disponibles
    max_threads = min(5, num_processors)  # Máximo 5 threads

    time_data = []  # Lista para almacenar los tiempos de procesamiento
    words_data = []  # Lista para almacenar el total de palabras contadas
    thread_data = []  # Lista para almacenar el número de threads utilizados

    for percentage in percentages:
        num_files = int(len(file_names) * percentage)
        files_to_process = file_names[:num_files]

        for num_threads in range(1, max_threads + 1):
            # Procesar los datos y obtener el tiempo de procesamiento y el total de palabras contadas
            processing_time, total_words = process_data(percentage, num_threads, files_to_process)

            # Guardar los resultados en un archivo de texto
            output_file = f"results_{int(percentage * 100)}percent_{num_threads}threads.txt"
            with open(output_file, 'w') as f:
                f.write(f"Porcentaje del conjunto de datos: {percentage * 100}%\n")
                f.write(f"Número de threads utilizados: {num_threads}\n")
                f.write(f"Tiempo de procesamiento: {processing_time:.2f} segundos\n")
                f.write(f"Total de palabras contadas: {total_words}\n")
                f.write("------------------------------\n")

            # Imprimir los resultados en la salida
            print(f"Porcentaje del conjunto de datos: {percentage * 100}%")
            print(f"Número de threads utilizados: {num_threads}")
            print(f"Tiempo de procesamiento: {processing_time:.2f} segundos")
            print(f"Total de palabras contadas: {total_words}")
            print("------------------------------")

            # Guardar los tiempos, palabras contadas y número de threads utilizados en las listas
            time_data.append(processing_time)
            words_data.append(total_words)
            thread_data.append(num_threads)

    # Generar el gráfico
    plt.figure(figsize=(10, 6))
    plt.plot(thread_data, time_data, marker='o', label='Tiempo de Procesamiento')
    plt.plot(thread_data, words_data, marker='o', label='Total de Palabras')
    plt.xlabel('Número de Threads')
    plt.ylabel('Valor')
    plt.legend()
    plt.title('Rendimiento del Contador de Palabras')
    plt.savefig('grafico.png')  # Guardar el gráfico en un archivo de imagen

if __name__ == '__main__':
    main()