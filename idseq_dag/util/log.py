import datetime
import inspect
import logging
import multiprocessing
import os
import sys
import json

print_lock = multiprocessing.RLock()


class StreamToLogger(object):
    """Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass


def configure_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Echo to stdout so they get to CloudWatch
    # handler = logging.StreamHandler(sys.stdout)
    # handler.setLevel(logging.INFO)
    # formatter = logging.Formatter("test %(message)s")
    # handler.setFormatter(formatter)
    # logger.addHandler(handler)

    # Direct stdout and stderr to the logger
    stdout_logger = logging.getLogger()
    # formatter = logging.Formatter("stdout: %(message)s")
    handler = logging.StreamHandler(sys.stdout)
    # handler.setFormatter(formatter)
    stdout_logger.addHandler(handler)
    sl = StreamToLogger(stdout_logger, logging.INFO)
    sys.stdout = sl

    # stderr_logger = logging.getLogger('STDERR')
    # sl = StreamToLogger(stderr_logger, logging.ERROR)
    # formatter = logging.Formatter("stderr: %(message)s")
    # handler = logging.StreamHandler(sys.stderr)
    # handler.setFormatter(formatter)
    # stderr_logger.addHandler(handler)
    # sys.stderr = sl


def write(message, level=logging.INFO, prev_caller_num=0):
    # Generate datetime timestamp. Keep all times in UTC, ISO 8601 format with
    # dashes and no microseconds. Include +00:00 offset.
    # Ex: "2018-08-17T18:45:42+00:00"
    ts = datetime.datetime.utcnow()
    ts = ts.replace(microsecond=0, tzinfo=datetime.timezone.utc)
    timestamp = ts.isoformat()

    # Get proper frame info to log caller info
    frame_number = 1
    if prev_caller_num:
        # Use this to get the caller of command.execute for example.
        # 1 means get the caller of the caller. 2 means 2 frames back, etc.
        frame_number += prev_caller_num
    frame_info = inspect.stack()[frame_number]
    file_name = os.path.basename(frame_info.filename)
    module_name = os.path.splitext(file_name)[0]
    function_name = frame_info.function
    line_num = frame_info.lineno

    level = logging.getLevelName(level)

    logger = logging.getLogger()
    with print_lock:
        # logger.info(f"{timestamp} {level} {module_name}.{function_name}:{line_num}: {message}")
        info = {"time": timestamp, "level": level, "module": module_name, "function": function_name, "line": line_num, "msg": message}
        logger.info(json.dumps(info))


def set_up_stdout():
    # Unbuffer stdout and redirect stderr into stdout. This helps observe logged
    # events in realtime.
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    os.dup2(sys.stdout.fileno(), sys.stderr.fileno())
