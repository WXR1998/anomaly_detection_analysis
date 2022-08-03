from abc import ABC, abstractmethod

from model import TimeSeries


class AnomalyDetection(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def train(self, **kwargs):
        pass

    @abstractmethod
    def detect(self, ts: TimeSeries) -> TimeSeries:
        pass

    @abstractmethod
    def tail_is_anomaly(self, ts: TimeSeries) -> bool:
        pass