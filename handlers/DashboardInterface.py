from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List

class Context():
    def __init__(self, strategy: Strategy) -> None:
        self._strategy = strategy

    @property
    def strategy(self) -> Strategy:
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def do_some_business_logic(self, redis_key: str) -> None:
        print("Context: Getting data using the strategy (Redis-driven)")
        result = self._strategy.do_algorithm(redis_key)
        print(result)

class Strategy(ABC):
    """
    The Strategy interface declares operations common to all supported versions
    of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    @abstractmethod
    def do_algorithm(self, data: List):
        pass

    @abstractmethod
    def validate(self) -> bool:
        pass

    @abstractmethod
    def build_key(self, redis_key: str) -> dict:
        pass

    @abstractmethod
    def get_cached_data(self, redis_key:str) -> dict:
        # get data from redis using the redis_key
        # put it inside dictionary 
        # return the data
        pass

