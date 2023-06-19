from typing import List, Tuple
from .interfaces import MapStrategy

class EmmiterTextCountMapper(MapStrategy):
    
    def execute(self, document: str) -> List[Tuple[str, int]]:
        words = document.split()
        wordCountPairs = [(word, 1) for word in words]
        return wordCountPairs
    