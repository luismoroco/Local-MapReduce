from ..Strategy.Map import EmmiterTextCountMapper
from ..Strategy.Reduce import TextCounterReducer
from .interfaces import Map, Reduce

import time 
import logging as logg 
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from typing import List, Tuple
import gc

from .info import SUCESS_SUBSCRIBE, ERROR_FRAMEWROK_INIT, PAIRS_CREATED, APIRS_ERROR, REDUCE_COMPLETED, REDUCE_ERROR

documents = [
    "Lorem Lorem ipsum dolor sit amet consectetur adipiscing elit",
    "sed do eiusmod tempor Lorem incididunt ut labore et dolore magna aliqua",
    "Ut enim ad minim Lorem veniam quis nostrud exercitation ullamco laboris"
]

class MapReduceFramework(Map, Reduce):
    
    _mapStategy: EmmiterTextCountMapper
    _reduceStrategy: TextCounterReducer
    _threadPool: ThreadPoolExecutor
    poolSize: int
    _pairs: List[Tuple[str, int]]
    _result: List[Tuple[str, int]]

    def __init__(self, map: EmmiterTextCountMapper, reduce: TextCounterReducer, 
                 poolSize: int = multiprocessing.cpu_count()) -> None:
        try:
          self.poolSize = poolSize
          self._mapStategy = map
          self._reduceStrategy = reduce
          self._threadPool = ThreadPoolExecutor(max_workers=self.poolSize)
          self._pairs = []
          
          logg.info(SUCESS_SUBSCRIBE)
        except:
          logg.error(ERROR_FRAMEWROK_INIT)
          exit()
    
    def regenerateThreadPool(self) -> None:
       self._threadPool = ThreadPoolExecutor(max_workers=self.poolSize)
    
    def map(self) -> None:
        try:
          self._pairs = [self._threadPool.submit(self._mapStategy.execute, doc) for doc in documents]
          self._threadPool.shutdown(wait=True)
          
          logg.info(PAIRS_CREATED)
          gc.collect()
        except:
          logg.error(APIRS_ERROR)
          exit()

    def reduce(self) -> None:
        try:
            self.regenerateThreadPool()

            grouped_pairs = {}
            for key, value in self._pairs:
                if key not in grouped_pairs:
                    grouped_pairs[key] = []
                grouped_pairs[key].append(value)
            gc.collect()

            reduced_pairs = []
            for key, values in grouped_pairs.items():
                reduced_pairs.append(self._threadPool.submit(self._reduceStrategy.execute, key, values))

            self._threadPool.shutdown(wait=True)
            
            result = []
            for future in reduced_pairs:
                result.append(future.result())
            
            self._result = result
            logg.info(REDUCE_COMPLETED)
        except:
            logg.error(REDUCE_ERROR)
            exit()
    
    def getResults(self) -> List[Tuple[str, int]]:
       return self._result

       