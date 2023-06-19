from typing import Any, List, Tuple
from .interfaces import ReduceStrategy

class TextCounterReducer(ReduceStrategy):
    
    def execute(self, key: str, values: List[int]) -> Tuple[str, int]:
        totalCount = sum(values)
        return key, totalCount