/*
Package stomp provides operations that allow communication with a message broker that supports the STOMP protocol.
STOMP is the Streaming Text-Oriented Messaging Protocol. See http://stomp.github.com/ for more details.

This package provides support for all STOMP protocol features in the STOMP protocol specifications,
versions 1.0, 1.1 and 1.2. These features including protocol negotiation, heart-beating, value encoding,
and graceful shutdown.

Connecting to a STOMP server is achieved using the stomp.Dial function, or the stomp.Connect function. See
the examples section for a summary of how to use these functions. Both functions return a stomp.Conn object
for subsequent interaction with the STOMP server.

Once a connection (stomp.Conn) is created, it can be used to send messages to the STOMP server, or create
subscriptions for receiving messages from the STOMP server. Transactions can be created to send multiple
messages and/ or acknowledge multiple received messages from the server in one, atomic transaction. The
examples section has examples of using subscriptions and transactions.

The client program can instruct the stomp.Conn to gracefully disconnect from the STOMP server using the
Disconnect method. This will perform a graceful shutdown sequence as specified in the STOMP specification.

Source code and other details for the project are available at GitHub:

   https://github.com/go-stomp/stomp

*/
package stomp
