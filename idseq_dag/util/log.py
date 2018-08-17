import datetime
import inspect
import logging
import multiprocessing
import os
import sys

print_lock = multiprocessing.RLock()


def configure_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Echo to stdout so they get to CloudWatch
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("test %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def write(message, level=logging.INFO, flush=True, prev_caller=False):
    # Generate datetime timestamp. Keep all times in UTC, ISO 8601 format with
    # dashes and no microseconds. Include +00:00 offset.
    # Ex: "2018-08-17T18:45:42+00:00"
    ts = datetime.datetime.utcnow()
    ts = ts.replace(microsecond=0, tzinfo=datetime.timezone.utc)
    timestamp = ts.isoformat()

    # Get proper frame info to log caller info
    frame_number = 1
    if prev_caller:
        # Use this to get the caller of command.execute for example.
        frame_number = 2
    frame_info = inspect.stack()[frame_number]
    file_name = os.path.basename(frame_info.filename)
    module_name = os.path.splitext(file_name)[0]
    function_name = frame_info.function
    line_num = frame_info.lineno

    level = logging.getLevelName(level)

    logger = logging.getLogger()
    with print_lock:
        logger.info(f"{timestamp} {level} {module_name}.{function_name}:{line_num}: {message}")
        if flush:
            sys.stdout.flush()


def set_up_stdout():
    # Unbuffer stdout and redirect stderr into stdout. This helps observe logged
    # events in realtime.
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    os.dup2(sys.stdout.fileno(), sys.stderr.fileno())
