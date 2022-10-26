from typing import Optional
import math
import numpy as np


class TimeSeries:
    def __init__(self,
                 k: float=3,
                 normal_window_length: int=10):
        """
        对于时间序列的前normal_window_length个元素，认为其是正常的。
        用正常的部分训练模型、获取算法需要的超参数。
        """

        self._k = k
        self._normal_window_length = normal_window_length

        self._value = list()
        self._mu = 0
        self._sigma = 1

    def reset(self):
        self._value = list()
        self._mu = 0
        self._sigma = 1

    def add(self, value: float):
        assert type(value) in (float, int)

        n = len(self._value)
        if len(self._value) <= self._normal_window_length:
            mu = (self._mu * n + value) / (n + 1)
            sigma = math.sqrt((n * (self._sigma**2 + (mu - self._mu)**2) + (mu - self._mu)**2) / (n + 1))
            self._mu = mu
            self._sigma = sigma

        self._value.append(value)

    def _tail_value(self) -> Optional[float]:
        if len(self) == 0:
            return np.nan
        return self._value[-1]

    def value(self, limit: Optional[int]=None) -> list:
        if limit and len(self._value) > limit:
            return self._value[len(self._value) - limit:]
        return self._value

    def is_abnormal(self):
        low = self._mu - self._k * self._sigma
        high = self._mu + self._k * self._sigma
        return not low <= self._tail_value() <= high and len(self) > self._normal_window_length

    def __len__(self):
        return len(self._value)

    def __str__(self):
        return f'Value:\t{self.value()}\n'
