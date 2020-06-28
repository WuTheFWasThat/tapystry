# Tapystry

This is a small library for handling effects, inspired by [redux-saga](https://www.github.com/redux-saga/redux-saga).

## Installation

`pip install tapystry`

## Usage

It's most easily understood by example!

```
import tapystry as tap

def sender(value):
    yield tap.Send('key', value)
    return "sent"

def incrementer():
    value = 0
    while True:
        winner, received_val = yield tap.Race([
            tap.Receive('key'),
            tap.Receive('break'),
        ])
        if winner == 1:
            break
        else:
            value += received_val
    return value

def fn():
    # fork off a strand that increments
    recv_strand = yield tap.CallFork(incrementer)
    # send a value to add
    sent = yield tap.Call(sender, (5,))
    assert sent == "sent"
    # equivalent syntax using yield from
    sent = yield from sender(8)
    assert sent == "sent"
    # forked process is not yet done
    assert not recv_strand.is_done()
    yield tap.Send("break")
    # this value won't get received
    sent = yield tap.Call(sender, (1,))
    assert sent == "sent"
    value = yield tap.Join(recv_strand)
    return value

assert tap.run(fn) == 13
```

### What's the point?

This lets you isolate side effects really easily.

For example, you could fork a `Strand` that repeatedly polls a database for events.
Then you could have a bunch of independent logic that decides what to do with that.
Everything except the logic touching the database is pure and can be unit tested easily.
