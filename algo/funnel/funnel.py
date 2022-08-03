import numpy as np
import matplotlib.pyplot as plt
from numpy.linalg import svd

from algo import AnomalyDetection
from model import TimeSeries


class Funnel(AnomalyDetection):
    def __init__(self, window_size: int = 5):
        self._delta = window_size
        self._omega = window_size
        self._gamma = window_size

    @staticmethod
    def _Lanczos(C: np.array,
                beta_i: float,
                k: int,
                omega: int) -> np.ndarray:
        a = np.zeros(k + 1)
        b = np.ones(k + 1)
        q = np.zeros((k + 1, omega))
        r = np.zeros((k + 1, omega))
        s = 0
        r[0] = beta_i
        b[0] = 1
        while s < k:
            q[s + 1] = r[s] / b[s]
            s += 1
            a[s] = q[s].T.dot(C).dot(q[s])
            r[s] = C.dot(q[s]) - a[s] * q[s] - b[s - 1] * q[s - 1]
            b[s] = np.linalg.norm(r[s])
        T_k = np.zeros((k, k))
        for i in range(k):
            T_k[i][i] = a[i + 1]
        for i in range(k - 1):
            T_k[i][i + 1] = b[i + 1]
            T_k[i + 1][i] = b[i + 1]
        return np.nan_to_num(T_k)

    @staticmethod
    def _ql_iteration(A: np.array) -> tuple:
        p = np.eye(np.size(A, 1))
        for ep in range(2000):
            v = np.diag(A)
            q, R = np.linalg.qr(np.linalg.inv(A.T))
            L = np.linalg.inv(R).T
            A = np.dot(L, q)
            p = np.dot(p, q)
            dif = np.abs(max(np.diag(A)) - max(v)) + np.abs(min(np.diag(A)) - min(v))
            if np.abs(dif) < 1e-6:
                break
        return p, np.diag(A)

    def _SST(self,
             t: int, x,
             omega: int,
             delta: int,
             gamma: int,
             eta: int = 3,
             rho: int = 0) -> float:
        """
        奇异谱变换方法
        Args:
            t:      当前的时间戳
            x:      指标值数组
            omega:  时间窗口的长度(Hankel矩阵的行数)
            delta:  变更之前时间窗口的数量(B:Hankel矩阵的列数)
            gamma:  变更之后时间窗口的数量(A:Hankel矩阵的列数)
            eta:    前eta个奇异值用于表征和重建整个时间序列
            rho:    变更持续时长

        Returns:
            float:  以t为中心的change score
        """
        B = np.zeros((omega, delta))
        A = np.zeros((omega, gamma))

        # calculating time before t
        for i in range(t - delta, t):
            for j in range(i - omega + 1, i + 1):
                B[j - i + omega - 1][i - t + delta] = x[j]
        C = B * B.T
        k = 2 * eta
        if eta % 2 == 1:
            k -= 1

        # 计算t时刻之后的Hankel矩阵
        for i in range(t + rho, t + rho + gamma):
            for j in range(i, i + omega):
                A[j - i][i - t - rho] = x[j]


        mu_a, u_a = np.linalg.eig(A * A.T)
        mu_b, u_b = np.linalg.eig(B * B.T)

        # 求A的前eta个特征向量和B的前eta个特征向量
        # 设eta = 1
        u_a_1 = u_a[0]
        u_b_1 = u_b[0]
        x_hat = 1 - u_a_1.T.dot(u_b_1)

        # 计算中位数和平均中位误差
        x_bef = np.array(x[t - 2 * omega + 1:t])
        x_aft = np.array(x[t + 1:t + 2 * omega])
        median_a = np.median(x_bef)
        median_b = np.median(x_aft)
        MAD_a = np.median(np.array([abs(y - median_a) for y in x_bef]))
        MAD_b = np.median(np.array([abs(y - median_b) for y in x_aft]))
        x_tilde = abs(x_hat) * abs(median_a - median_b) * abs(np.sqrt(MAD_a) - np.sqrt(MAD_b))
        x_tilde = min(x_tilde, 1)
        return x_tilde

    def _funnel(self, data: np.array) -> np.ndarray:
        """
        因为funnel需观测某个时间点前后若干个点的值，所以给出的score的长度相比原长度会短一点
        注意**这里不需要再对数据进行标准化操作了**
        Args:
            data:   时间序列数据
        Returns:
            array:  对应的change score
        """

        cost = data

        score = []
        value = []
        for _ in range(self._omega + self._delta, len(cost) - self._omega - self._gamma):
            value.append(cost[_])
            score.append(self._SST(_, x=cost, omega=self._omega, delta=self._delta, gamma=self._gamma))
        return np.array(score)

    @staticmethod
    def plot(data: np.array, score: np.array, window_size: int) -> None:
        """
        给数据和对应的change score画图
        Args:
            data:   数据
            score:  change score
        Returns:
            None
        """
        plt.figure(figsize=(14, 7))

        plt.subplot(2, 1, 1)
        plt.plot(data)
        plt.title(f'KPI Data (Post-Normalization)\nWindow size: {window_size}')

        plt.subplot(2, 1, 2)
        plt.plot(score, c='orange')
        plt.ylim((-0.1, 1.1))
        plt.title('Change Score')

        plt.show()

    def _change_score(self, ts: TimeSeries) -> TimeSeries:
        """
        获得某个序列每一个点为中心的change_score
        Args:
            ts: 输入的时间序列
        Returns:
            TimeSeries of changescore
        """

        length_limit = 2 * self._omega + self._delta + self._gamma
        if len(ts.value()) <= length_limit:
            raise Exception(f'Funnel requires the length of data be longer than {length_limit}.')
        score = np.concatenate([np.zeros(self._omega + self._delta), self._funnel(np.array(ts.value())), np.zeros(self._omega + self._gamma)])
        return TimeSeries(score)

    def train(self, **kwargs):
        pass

    def detect(self, ts: TimeSeries, **kwargs) -> TimeSeries:
        return self._change_score(ts)