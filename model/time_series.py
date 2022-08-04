from typing import Optional, Union
import numpy as np

class TimeSeries:
    def __init__(self,
                 value: Union[list, tuple, np.ndarray]=tuple(),
                 normal_window_length: int=60):
        """
        对于时间序列的前normal_window_length个元素，认为其是正常的。
        用正常的部分训练模型、获取算法需要的超参数。
        """

        value = np.array(value, dtype=float)

        self._value = value
        self._normal_window_length = normal_window_length

    def add(self, value: float):
        self._value = np.append(self._value, value)

    def value(self, normal_part: bool=False) -> np.ndarray:
        if normal_part and len(self._value) > self._normal_window_length:
            return self._value[:self._normal_window_length]
        return self._value

    def standardize(self):
        mu = np.nanmean(self.value(normal_part=True))
        sigma = np.nanstd(self.value(normal_part=True))
        self._value -= mu
        if sigma > 0:
            self._value /= sigma

    def __str__(self):
        return f'Value:\t{self.value()}\n'