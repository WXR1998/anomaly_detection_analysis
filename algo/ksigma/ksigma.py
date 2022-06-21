from algo.anomaly_detection import AnomalyDetection

class KSigma(AnomalyDetection):
    def __init__(self, k: float = 3, **kwargs):
        super().__init__(**kwargs)
        self.k = k

    def preprocess(self, **kwargs):
        pass

    def detect(self, ts: int, **kwargs):
        return 1.0