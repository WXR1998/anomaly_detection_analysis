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