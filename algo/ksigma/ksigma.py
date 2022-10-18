from algo import AnomalyDetector
from model import TimeSeries

class KSigma(AnomalyDetector):
    def __init__(self, k: float = 3, **kwargs):
        """
        使用一开始的一段作为正常数据，以此计算mu和sigma。后面的异常判断都以该值计算。
        """
        super().__init__(**kwargs)
        self._k = k

    def train(self, **kwargs):
        pass

    def detect(self, ts: TimeSeries, **kwargs) -> TimeSeries:
        # deprecated
        return ts

    def tail_is_anomaly(self, ts: TimeSeries) -> bool:
        mu = ts._mu
        sigma = ts._sigma

        up_thres = mu + sigma * self._k
        down_thres = mu - sigma * self._k

        v = ts.tail_value()
        return not (down_thres <= v <= up_thres) and len(ts) > ts._normal_window_length
