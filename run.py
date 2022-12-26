from multiprocessing import Manager
from concurrent.futures import ProcessPoolExecutor

from util import threading
from netio.io_handler import IOHandler
from util.dispatcher import Dispatcher
from util.logging_config import logging_config

if __name__ == '__main__':
    logging_config()

    data_queue  = Manager().Queue()
    anom_queue  = Manager().Queue()
    cmd_queue   = Manager().Queue()
    res_queue   = Manager().Queue()
    num_workers = 18
    dispatcher = Dispatcher(
        k=5,
        data_queue=data_queue,
        anom_queue=anom_queue,
        cmd_queue=cmd_queue,
        res_queue=res_queue,
        num_workers=num_workers,
        debug=False
    )
    io_handler = IOHandler(
        interval=3.0,
        num_workers=num_workers,
        data_queue=data_queue,
        anom_queue=anom_queue,
        cmd_queue=cmd_queue,
        res_queue=res_queue,
        send_reports=True,
    )

    pool = ProcessPoolExecutor(max_workers=1)
    pool.submit(io_handler.run).add_done_callback(threading.thread_done_callback)
    dispatcher.run()
