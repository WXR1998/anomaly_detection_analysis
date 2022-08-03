import numpy as np

from algo import AnomalyDetection
from model import TimeSeries

class KSigma(AnomalyDetection):
    def __init__(self, k: float = 3, **kwargs):
        super().__init__(**kwargs)
        self._k = k

    def train(self, **kwargs):
        pass

    def detect(self, ts: TimeSeries, **kwargs) -> TimeSeries:
        values = ts.value()
        mu = np.nanmean(values)
        sigma = np.nanstd(values)
        up_thres = mu + sigma * self._k
        down_thres = mu - sigma * self._k

        result = np.zeros(values.shape)
        anomaly_idxs = (values < down_thres) | (values > up_thres)
        result[anomaly_idxs] = 1

        return TimeSeries(result)

    def tail_is_anomaly(self, ts: TimeSeries) -> bool:
        # result = self.detect(ts)
        # return result.value()[-1] == 1
        return True