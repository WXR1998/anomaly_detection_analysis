import logging

from sam.base import exceptionProcessor

def thread_done_callback(worker):
    worker_exception = worker.exception()
    if worker_exception:
        exceptionProcessor.ExceptionProcessor(logging.getLogger()).logException(worker_exception)
