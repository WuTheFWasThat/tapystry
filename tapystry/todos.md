- reimplement Call in terms of CallFork and Join
- have way for as_effect to override the type name itself, switch First back
- implement CallThread
- (bigger change) make CallThread able to yield back to the event loop

- add a test that canceled stuff doesnt get intercepted
