from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List
import polars as pl

class Context():
    def __init__(self, df: pl.DataFrame,  strategies: List[Strategy]) -> None:
        self.df = df
        self.exam_data = {}
        self._strategies = strategies 

    @property
    def strategy(self) -> Strategy:
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def do_business_logic(self) -> None:
        processed_df = self._get_latest_attempts_df()
        
        for strategy in self._strategies:
            # Each strategy runs its calculation on the *processed* DataFrame
            report_chunk = strategy.calculate(processed_df)
            self.exam_data.update(report_chunk)
        
        exam_performance = {
            'exam_performance' : self.exam_data,
            'raw_exam_performance' : processed_df.to_dicts()
        }
        return exam_performance

    def _get_latest_attempts_df(self) -> pl.DataFrame:
        get_max_attempts = (
            self.df
            .group_by("user_id")
            .agg(
                pl.col("attempt").max().alias("latest_attempt")
            )
        )

        df_with_max = self.df.join(get_max_attempts, on="user_id", how="left")

        latest_attempts_df = (
            df_with_max
            .filter(pl.col("attempt") == pl.col("latest_attempt"))
            .drop("latest_attempt")
        )
        
        return latest_attempts_df
    
class Strategy(ABC):
    @abstractmethod
    def calculate(self, df:pl.DataFrame):
        pass


