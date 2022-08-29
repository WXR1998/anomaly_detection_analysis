from typing import Optional, Union, Any, Iterable

import math
import numpy as np
import datetime

from sam.base.p4NFUsage import P4NFUsage


class TimeSeries:
    def __init__(self,
                 value: Union[list, tuple, np.ndarray]=tuple(),
                 timestamp: Union[list, tuple, np.ndarray]=tuple(),
                 normal_window_length: int=60):
        """
        对于时间序列的前normal_window_length个元素，认为其是正常的。
        用正常的部分训练模型、获取算法需要的超参数。
        """

        assert len(value) == len(timestamp)
        self._value = list(value)
        self._timestamp = list(timestamp)
        self._normal_window_length = normal_window_length
        self._mu = 0
        self._sigma = 1

    def add(self, value: Any, timestamp: int=0):
        if value is None:
            value = 0
        if isinstance(value, Iterable):
            value = float(np.mean(value))
        if isinstance(value, P4NFUsage):
            value = 0

        n = len(self._value)
        if len(self._value) <= self._normal_window_length:
            mu = (self._mu * n + value) / (n + 1)
            sigma = math.sqrt((n * (self._sigma**2 + (mu - self._mu)**2) + (mu - self._mu)**2) / (n + 1))
            self._mu = mu
            self._sigma = sigma

        self._value.append(value)
        self._timestamp.append(timestamp)

    def tail_value(self) -> Optional[float]:
        if len(self._value) == 0:
            return None
        return self._value[-1]

    def value(self, normal_part: bool=False) -> list:
        if normal_part and len(self._value) > self._normal_window_length:
            return self._value[:self._normal_window_length]
        return self._value

    def timestamp(self, normal_part: bool=False) -> list:
        if normal_part and len(self._timestamp) > self._normal_window_length:
            return self._timestamp[:self._normal_window_length]
        return self._timestamp

    def standardize(self):
        mu = np.nanmean(self.value(normal_part=True))
        sigma = np.nanstd(self.value(normal_part=True))
        self._value -= mu
        if sigma > 0:
            self._value /= sigma

    def __str__(self):
        return f'Value:\t{self.value()}\n'