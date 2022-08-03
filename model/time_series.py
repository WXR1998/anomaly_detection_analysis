from typing import Optional, Union
import numpy as np

class TimeSeries:
    def __init__(self,
                 value: Union[list, tuple, np.ndarray]=tuple(),
                 maximum_length: int=None):
        value = np.array(value, dtype=float)

        self._value = value
        self._maximum_length = maximum_length

    def add(self, value: float):
        self._value = np.append(self._value, value)

    def value(self, all: bool=False):
        if all or self._maximum_length is None or self._maximum_length < len(self._value):
            return self._value
        return self._value[:-self._maximum_length]

    def standardize(self):
        mu = np.nanmean(self.value())
        std = np.nanstd(self.value())
        self._value -= mu
        if std > 0:
            self._value /= std

    def __str__(self):
        return f'Value:\t{self.value()}\n'