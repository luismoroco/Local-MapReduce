from abc import ABC, abstractmethod

class Map(ABC):
    
    @abstractmethod
    def map(self) -> None:
        pass 

class Reduce(ABC):
    
    @abstractmethod
    def reduce(self) -> None:
      pass