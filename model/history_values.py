class HistoryValues:
    def __init__(self, len_limit: int=30):
        self._len_limit = len_limit
        self._values = []

    def append(self, obj):
        self._values.append(obj)
        if len(self._values) > self._len_limit:
            self._values.pop(0)

    def value(self):
        return self._values
