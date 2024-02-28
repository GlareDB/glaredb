import collections.abc
import contextlib
import os
import pathlib


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
