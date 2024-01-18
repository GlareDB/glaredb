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
