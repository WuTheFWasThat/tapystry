import pytest

import tapestry as tap

def test_simple():
    def fn():
        yield tap.Send('key')
        return 5

    assert tap.run(fn) == 5
