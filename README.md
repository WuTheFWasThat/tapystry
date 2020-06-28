# Tapystry

This is a tiny library for handling effects.

## Installation

`pip install tapystry`

## Usage

It's most easily understood by example!

```
import tapystry as tap

def sender(value):
    yield tap.Send('key', value)

def incrementer():
    value = yield tap.Receive('key')
    return value + 1

def fn():
    recv_strand = yield tap.CallFork(incrementer)
    send_strand = yield tap.Call(sender, 5)
    value = yield tap.Join(recv_strand)
    assert value == 6
    return dict(result=value)

assert tap.run(fn) == dict(result=6)
```

### What's the point?

This lets you isolate side effects really easily.

For example, you could fork a `Strand` that repeatedly polls a database for events.
Then you could have a bunch of independent logic that decides what to do with that.
Everything except the logic touching the database is pure and can be unit tested easily.
