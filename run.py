import numpy as np
import algo
from model import TimeSeries

t = algo.Funnel()
ts = TimeSeries(np.random.random(40))
ts.standardize()

print(t.detect(ts))