# 异常检测模块

## 联调运行方法
在wxr账号下，先`conda activate py39`，然后`python run.py`即可运行异常检测程序。

## 基础运行方法

```shell
python run.py
```

需要在终端同步运行sam项目里面的：
```shell
python sam/simulator/simulator.py
python sam/measurer/measurer.py
```

方可拿到输入的数据。

## 实现细节
采样间隔为`interval`秒，即每`interval`秒会向数据端发送一个请求包请求获取当前所有的数据。

使用k-sigma方法检测异常。

使用多进程，将数据的输入输出和异常检测部分隔离开，异常检测部分使用多个CPU核心同步处理，并将报告的异常情况/前端查询结果发回输入输出进程（IOHandler）进行处理。

对于每一个指标，设当前时间点为`T`,则默认`[T-normal_window_length-abnormal_window_length, T-abnormal_window_length)`数据点是没有故障的。
并以这些次数的采样结果作为mu和sigma的计算标准，从而得到k-sigma算法中，该指标正常范围的上下限。超出该上下限即认为发生异常。

## 详细参数
见`run.py`中，Dispatcher和IOHandler的输入参数有`normal_window_length`, `send_result`, `abnormal_window_length`, `cooldown`, `interval`及其对应的注释。 

如果`send_result`为`False`，则不向regulator报告故障（但故障仍会以日志的形式打印在终端中）。正式联调时该项应设为`True`。

## dashboard对接
按照陈浩给定的参数已经实现好接口。

异常检测模块会读取相关的请求(`MSG_TYPE_ABNORMAL_DETECTOR_CMD`类别)并返回对应的结果。

其他参数不用特殊设置，如果报错可以将报错信息发给我。
