import concurrent.futures
import os
import gc 
import time

def map_function(document):
    words = document.split()  
    word_count_pairs = [(word, 1) for word in words]  
    return word_count_pairs

def reduce_function(word, counts):
    total_count = sum(counts)  
    return word, total_count


DATA_DIR = './Gutenberg_Text/'

def get_files(directory):
    file_list = []

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_list.append(file_path)
        gc.collect()
    return file_list

def read_batches(file_list, batch_size, slice = 5):
    batches = []

    file_list = file_list[:slice]

    for file_path in file_list:
        with open(file_path, 'r', encoding='latin-1') as file:
            lines = file.readlines()
            batches.extend([' '.join(lines[i:i+batch_size]) for i in range(0, len(lines), batch_size)])
        gc.collect()
        
    return batches

def main():
    file_list = get_files(DATA_DIR)
    print(len(file_list))
    batches = read_batches(file_list, 3, 50)

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        intermediate_results = [executor.submit(map_function, batch) for batch in batches]
        intermediate_pairs = []
        for future in concurrent.futures.as_completed(intermediate_results):
            intermediate_pairs.extend(future.result())
        
        gc.collect()

        # Agrupar los valores intermedios por clave
        grouped_pairs = {}
        for key, value in intermediate_pairs:
            if key not in grouped_pairs:
                grouped_pairs[key] = []
            grouped_pairs[key].append(value)
        
        gc.collect()

        final_results = [executor.submit(reduce_function, key, value) for key, value in grouped_pairs.items()]
        final_output = []
        for future in concurrent.futures.as_completed(final_results):
            final_output.append(future.result())

        print(len(final_output))

if __name__ == "__main__":
    main()