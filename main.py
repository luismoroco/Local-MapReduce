from src.Framework.MapreduceMultipro import MapReduceFramework
from src.Strategy.Map import EmmiterTextCountMapper
from src.Strategy.Reduce import TextCounterReducer

import logging as logg
import gc
import os 

logg.basicConfig(level=logg.DEBUG, format='%(threadName)s: %(message)s')

DATA_DIR = './Gutenberg_Text/'

def getNameFiles(directory):
    vecFiles = []

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            vecFiles.append(file_path)

    gc.collect()
    return vecFiles

def readBatches(file_list, batch_size, slice: int = 5):
    batches = []

    file_list = file_list[:slice]

    for file_path in file_list:
        with open(file_path, 'r', encoding='latin-1') as file:
            lines = file.readlines()
            batches.extend([' '.join(lines[i:i+batch_size]) for i in range(0, len(lines), batch_size)])
        gc.collect()
        
    return batches

fileList = getNameFiles(DATA_DIR)
print(len(fileList))
batches = readBatches(fileList, 4, 1000)

mapReduce = MapReduceFramework(
   EmmiterTextCountMapper(), 
   TextCounterReducer(),
   12)

mapReduce.setBatches(batches)
mapReduce.map()
mapReduce.reduce()
gc.collect()

print(len(mapReduce.getResults()))