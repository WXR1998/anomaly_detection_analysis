import logging
import datetime

def logging_config():
    def to_beijing(sec, what):
        beijing_time = datetime.datetime.now() + datetime.timedelta(hours=8)
        return beijing_time.timetuple()

    logging.Formatter.converter = to_beijing
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d [%(levelname)s]\t%(processName)s\t'
               '%(module)s:%(funcName)s():%(lineno)d\t'
               '%(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO
    )
