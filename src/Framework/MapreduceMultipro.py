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
    _mapStrategy: EmmiterTextCountMapper
    _reduceStrategy: TextCounterReducer
    _lock: multiprocessing.Lock
    _processes: List[multiprocessing.Process]
    poolSize: int
    _pairs: List[Tuple[str, int]]
    _result: List[Tuple[str, int]]

    def __init__(self, map: EmmiterTextCountMapper, reduce: TextCounterReducer,
                 poolSize: int = multiprocessing.cpu_count()) -> None:
        try:
            self.poolSize = poolSize
            self._mapStrategy = map
            self._reduceStrategy = reduce
            self._lock = multiprocessing.Lock()
            self._processes = []
            self._pairs = []

            logg.info(SUCESS_SUBSCRIBE)
        except:
            logg.error(ERROR_FRAMEWROK_INIT)
            exit()

    def regenerateProcesses(self) -> None:
        self._processes = []

    def map(self) -> None:
        try:
            self.regenerateProcesses()

            for doc in documents:
                process = multiprocessing.Process(target=self._mapStrategy.execute, args=(doc, self._lock, self._pairs))
                process.start()
                self._processes.append(process)

            for process in self._processes:
                process.join()

            logg.info(PAIRS_CREATED)
        except:
            logg.error(APIRS_ERROR)
            exit()

    def reduce(self) -> None:
        try:
            self.regenerateProcesses()

            grouped_pairs = {}
            for key, value in self._pairs:
                if key not in grouped_pairs:
                    grouped_pairs[key] = []
                grouped_pairs[key].append(value)

            for key, values in grouped_pairs.items():
                process = multiprocessing.Process(
                    target=self._reduceStrategy.execute, args=(key, values, self._lock, self._pairs)
                )
                process.start()
                self._processes.append(process)

            for process in self._processes:
                process.join()

            result = []
            for key, value in self._pairs:
                result.append((key, value))

            self._result = result
            logg.info(REDUCE_COMPLETED)
        except:
            logg.error(REDUCE_ERROR)
            exit()

    def getResults(self) -> List[Tuple[str, int]]:
        return self._result
