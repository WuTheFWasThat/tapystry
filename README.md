# Tapystry

This is a small library for handling effects, inspired by [redux-saga](https://www.github.com/redux-saga/redux-saga).

## Installation

`pip install tapystry`

## Usage

It's most easily understood by example!

```
import tapystry as tap

def broadcaster(value):
    yield tap.Broadcast('key', value)
    return "success"

def incrementer():
    value = 0
    while True:
        winner, received_val = yield tap.Race(dict(
            receive=tap.Receive('key'),
            exit=tap.Receive('exit'),
        ))
        if winner == 'exit':
            break
        else:
            assert winner == 'receive'
            value += received_val
    return value

def fn():
    # fork off a strand that increments
    recv_strand = yield tap.CallFork(incrementer)
    # broadcast a value to add
    success = yield tap.Call(broadcaster, (5,))
    assert success == "success"
    # equivalent syntax using yield from
    success = yield from broadcaster(8)
    assert success == "success"
    # forked process is not yet done
    assert not recv_strand.is_done()
    yield tap.Broadcast("exit")
    # this value won't get received
    success = yield tap.Call(broadcaster, (1,))
    assert success == "success"
    value = yield tap.Join(recv_strand)
    return value

assert tap.run(fn) == 13
```

### What's the point?

This lets you isolate side effects really easily.

For example, you could fork a `Strand` that repeatedly polls a database for events.
Then you could have a bunch of independent logic that decides what to do with that.
Everything except the logic touching the database is pure and can be unit tested easily.
