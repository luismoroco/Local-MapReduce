import threading
from typing import Callable, List
from interfaces import RunTask
import logging as logg
import multiprocessing
from inf import ERR_EMMIT_TASK

class Worker(threading.Thread, RunTask):
    _task: Callable[..., None]

    def __init__(self, task: Callable[..., None]) -> None:
        super().__init__()
        self._task = task
    
    def run(self) -> None:
        try:
          self._task()
        except:
          logg.error(ERR_EMMIT_TASK)

class MasterBasedOnThreadPool:
   poolSize: int 
   _threadPool: List[Worker] 

   def __init__(self, poolSize: int = multiprocessing.cpu_count()) -> None:
      self.poolSize = poolSize
      self._threadPool = []




def task() -> None:
   print('Exec at', threading.current_thread().name)

th = Worker(task=task)
th.start()

th.join()


