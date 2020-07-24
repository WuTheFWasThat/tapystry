- reimplement Call in terms of CallFork and Join
- implement CallThread
- (bigger change) make CallThread able to yield back to the event loop

- add a test that canceled stuff doesnt get intercepted
