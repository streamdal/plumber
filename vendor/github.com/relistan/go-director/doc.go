// Director contains a few common loop flow control patterns that can be
// dependency-injected, generally to simplify running goroutines forever in the
// backround. Because the method's loop control is externally injected,
// Director facilitates much easier testing of background goroutines as well.

// The package defines a single interface and a few implementations of the
// interface. The externalization of the loop flow control makes it easy to
// test the internal functions of background goroutines by, for instance,
// only running the loop once while under test.
//
// The design is that any errors which need to be returned from the loop
// will be passed back on a channel whose implementation is left up to
// the individual Looper. Calling methods can wait on execution and for
// any resulting errors by calling the Wait() method on the Looper.
package director
