from ..Strategy.Map import EmmiterTextCountMapper
from ..Strategy.Reduce import TextCounterReducer
from .interfaces import Map, Reduce

import time
import logging as logg
import multiprocessing
from typing import List, Tuple

from .info import SUCESS_SUBSCRIBE, ERROR_FRAMEWROK_INIT, PAIRS_CREATED, APIRS_ERROR, REDUCE_COMPLETED, REDUCE_ERROR

documents = [
    "Lorem Lorem ipsum dolor sit amet consectetur adipiscing elit",
    "sed do eiusmod tempor Lorem incididunt ut labore et dolore magna aliqua",
    "Ut enim ad minim Lorem veniam quis nostrud exercitation ullamco laboris"
]

class MapReduceFramework(Map, Reduce):

    _mapStategy: EmmiterTextCountMapper
    _reduceStrategy: TextCounterReducer
    _pool: multiprocessing.Pool
    poolSize: int
    _pairs: List[Tuple[str, int]]
    _result: List[Tuple[str, int]]
    _batches: any

    def __init__(self, map: EmmiterTextCountMapper, reduce: TextCounterReducer, 
                 poolSize: int = multiprocessing.cpu_count()) -> None:
        try:
            self.poolSize = poolSize
            self._mapStategy = map
            self._reduceStrategy = reduce
            self._pool = multiprocessing.Pool(processes=self.poolSize)
            self._pairs = []

            logg.info(SUCESS_SUBSCRIBE)
        except:
            logg.error(ERROR_FRAMEWROK_INIT)
            exit()
    
    def setBatches(self, batches: any) -> None:
        self._batches = batches

    def map(self) -> None:
        try:
            results = self._pool.map(self._mapStategy.execute, self._batches)
            self._pairs = [pair for sublist in results for pair in sublist]
            logg.info(PAIRS_CREATED)
        except:
            logg.error(APIRS_ERROR)
            exit()

    def reduce(self) -> None:
        try:
            grouped_pairs = {}
            for key, value in self._pairs:
                if key not in grouped_pairs:
                    grouped_pairs[key] = []
                grouped_pairs[key].append(value)

            reduced_pairs = self._pool.starmap(self._reduceStrategy.execute, grouped_pairs.items())

            self._result = reduced_pairs
            logg.info(REDUCE_COMPLETED)
        except:
            logg.error(REDUCE_ERROR)
            exit()

    def getResults(self) -> List[Tuple[str, int]]:
        return self._result