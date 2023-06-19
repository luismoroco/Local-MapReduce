from abc import ABC, abstractmethod
from typing import List, Tuple

class MapStrategy(ABC):
    
    @abstractmethod
    def execute(self, document: str) -> List[Tuple[str, int]]:
        pass 
    
class ReduceStrategy(ABC):
    
    @abstractmethod
    def execute(self, key: str, values: List[int]) -> Tuple[str, int]:
        pass 
