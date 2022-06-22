from typing import Optional, Union
import numpy as np

class TimeSeries:
    def __init__(self,
                 value: Union[list, tuple, np.ndarray],
                 time: Optional[Union[list, tuple, np.ndarray]]=None):
        value = np.array(value, dtype=float)
        if time is not None:
            time = np.array(time, dtype=float)
            assert time.shape == value.shape

        self._time = time
        self._value = value

    def value(self):
        return self._value

    def time(self):
        return self._time

    def standardize(self):
        mu = np.nanmean(self.value())
        std = np.nanstd(self.value())
        self._value -= mu
        if std > 0:
            self._value /= std

    def __str__(self):
        return f'Time:\t{self.time()}\nValue:\t{self.value()}\n'