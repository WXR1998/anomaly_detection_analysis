import logging
from typing import Optional
import math
import numpy as np

class StatList:
    def __init__(self, lim: int=5):
        self._mu = 0
        self._sigma = 0
        self._value = list()
        self._lim = lim

    def reset(self):
        self._mu = 0
        self._sigma = 0
        self._value = list()

    def add(self, value: float):
        n = len(self._value)
        mu = (self._mu * n + value) / (n + 1)
        sigma = math.sqrt((n * (self._sigma**2 + (mu - self._mu)**2) + (mu - value)**2) / (n + 1))

        self._mu = mu
        self._sigma = sigma
        self._value.append(value)

        if len(self._value) > self._lim:
            value = self._value.pop(0)
            n = self._lim + 1
            mu = (self._mu * n - value) / (n - 1)
            inner = (n * (self._sigma**2 + (mu - self._mu)**2) - (mu - value)**2) / (n - 1)
            sigma = math.sqrt(max(0, (n * (self._sigma**2 + (mu - self._mu)**2) - (mu - value)**2) / (n - 1)))

            self._mu = mu
            self._sigma = sigma

    def stats(self):
        return self._mu, self._sigma

class TimeSeries:
    def __init__(self,
                 k: float=3,
                 normal_window_length: int=10,
                 abnormal_window_length: int=2,
                 minimum_sigma: float=0,
                 ):
        """
        对于时间序列的前normal_window_length个元素，认为其是正常的。
        用正常的部分训练模型、获取算法需要的超参数。
        """

        self._k = k
        self._normal_window_length = normal_window_length
        self._abnormal_window_length = abnormal_window_length
        self._minimum_sigma = minimum_sigma

        self._value = list()
        self._stat_value = StatList(normal_window_length + abnormal_window_length)

    def reset(self):
        self._value = list()
        self._stat_value.reset()

    def add(self, value: float):
        assert type(value) in (float, int)

        try:
            self._value.append(value)
            # 最新的值暂时不参与mu和sigma的计算
            if len(self._value) > self._abnormal_window_length:
                self._stat_value.add(self._value[- self._abnormal_window_length - 1])
        except:
            logging.warning('多线程导致Timeseries数据出错')

    def value(self, limit: Optional[int]=None) -> list:
        if limit and len(self._value) > limit:
            return self._value[len(self._value) - limit:]
        return self._value

    def is_abnormal(self):
        mu, sigma = self._stat_value.stats()
        sigma = max(self._minimum_sigma, sigma)
        low = mu - self._k * sigma
        high = mu + self._k * sigma

        if len(self) >= self._normal_window_length + self._abnormal_window_length:
            result = True
            for v in self._value[-self._abnormal_window_length:]:
                result = result and (not low <= v <= high)

            return result

        return False

    def __len__(self):
        return len(self._value)

    def __str__(self):
        return f'Value:\t{self.value()}\n'
