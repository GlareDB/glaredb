import contextlib
import os
import pathlib
import datetime
import logging


@contextlib.contextmanager
def timed(logger: logging.Logger, operation: str):
    start = datetime.datetime.now()
    try:
        yield
    finally:
        end = datetime.datetime.now()
        logger.info(f"operation '{operation if operation else 'unnamed'}' took {start - end}")


@contextlib.contextmanager
def cd(path: pathlib.Path):
    cur = os.getcwd()

    os.chdir(path)

    try:
        yield
    finally:
        os.chdir(cur)


@contextlib.contextmanager
def env(key: str, val: str):
    prev = os.getenv(key)

    os.environ[key] = val

    try:
        yield
    finally:
        if prev is None:
            del os.environ[key]
        else:
            os.environ[key] = prev
