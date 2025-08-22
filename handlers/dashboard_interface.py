from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List

class Context():
    def __init__(self, strategy: Strategy, id_context=None) -> None:
        self._strategy = strategy
        self.id_context = id_context
        if hasattr(self._strategy, 'id_context'):
            self._strategy.id_context = id_context

    @property
    def strategy(self) -> Strategy:
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def do_business_logic(self) -> None:
        print("Context: Getting data using the strategy (Redis-driven)")
        result = self._strategy.do_algorithm()
        print(result)
        return result

class Strategy(ABC):
    """
    The Strategy interface declares operations common to all supported versions
    of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    @abstractmethod
    def do_algorithm(self):
        pass

    @abstractmethod
    def validate(self, redis_key: str) -> bool:
        pass

    @abstractmethod
    def build_key(self, redis_key: str) -> dict:
        pass

    @abstractmethod
    def get_cached_data(self, redis_key:str) -> dict:
        pass

