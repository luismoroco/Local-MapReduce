from abc import ABC, abstractmethod
import logging as logg

class EmitStatus(ABC):
    
    @abstractmethod
    def emmitStatus() -> None:
        pass 
    

class RunTask(ABC):
    
    @abstractmethod
    def run(self) -> None:
        pass 