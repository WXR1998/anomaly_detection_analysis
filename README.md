# 异常检测模块

## 基础运行方法

```shell
python run.py
```

## 实现细节
采样间隔为5秒，即每5秒会向数据端发送一个请求包请求获取当前所有的数据。

使用k-sigma方法检测异常。

对于每一个指标，默认前`normal_window_length`次采样获取到的数据是没有故障的。并以这些次数的采样结果作为mu和sigma的计算标准，从而得到k-sigma算法中，该指标正常范围的上下限。超出该上下限即认为发生异常。

## 详细参数
见`run.py`中，有`normal_window_length`, `send_result`。

如果`send_result`为`False`，则不向regulator报告故障（但故障仍会以日志的形式打印在终端中）。正式联调时该项应设为`True`。

## dashboard对接
按照陈浩给定的参数已经实现好接口。异常检测模块会读取相关的请求(`MSG_TYPE_ABNORMAL_DETECTOR_CMD`类别)并返回对应的结果。