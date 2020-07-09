import time
import pytest

import tapystry as tap

def test_sleep():
    def fn():
        while True:
            yield tap.Sleep(0.1)

    tap.run(fn)

