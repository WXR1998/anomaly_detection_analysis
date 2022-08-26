import os.path

import numpy as np
import pandas as pd
import datetime

class Dataset:
    def __init__(self):
        self._data = {}

    def add_csv(self, csv_path: str):
        instance = os.path.splitext(os.path.basename(csv_path))[0]
        df = pd.read_csv(csv_path)
        columns = df.columns

        timestamp = df['timestamp']
        timestamp = timestamp.apply(lambda x: int(datetime.datetime.strptime('2022-08-01 ' + x, '%Y-%m-%d %H:%M:%S').timestamp()))
        for metric in columns:
            if metric in ['timestamp', 'label']:
                continue
            value = df[metric]
            self._data[f'{instance}_{metric}'] = (np.array(timestamp, dtype=float), np.array(value, dtype=float))

    def data(self):
        return self._data

    def print(self):
        for data_name, _ in self.data().items():
            timestamp, value = _
            print(data_name)
            print(value)
            print()