class FunnelConfig:
    """Funnel算法配置"""

    def __init__(self,
                 window_size: int = 5) -> None:
        """
        Funnel的配置
        Args:
            delta:  变更前的检查点数量(Hankel矩阵的大小)
            omega:  算法检查时间窗口的长度
            gamma:  变更后的检查点数量(Hankel矩阵的大小)
            建议以上三个参数都相等
        """
        self.delta = window_size
        self.omega = window_size
        self.gamma = window_size