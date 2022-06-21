from abc import ABC, abstractmethod

class AnomalyDetection(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def preprocess(self, **kwargs):
        pass

    @abstractmethod
    def detect(self, **kwargs):
        return