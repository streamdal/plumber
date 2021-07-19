package stream

import (
	"bufio"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"net"
	"sync"
	"sync/atomic"
)

type socket struct {
	connection net.Conn
	writer     *bufio.Writer
	mutex      *sync.Mutex
	closed     int32
	destructor *sync.Once
}

func (sck *socket) setOpen() {
	atomic.StoreInt32(&sck.closed, 1)
}

func (sck *socket) isOpen() bool {
	return atomic.LoadInt32(&sck.closed) == 1
}
func (sck *socket) shutdown(err error) {
	if !sck.isOpen() {
		return
	}
	atomic.StoreInt32(&sck.closed, 0)

	sck.destructor.Do(func() {
		sck.mutex.Lock()
		defer sck.mutex.Unlock()
		err := sck.connection.Close()
		if err != nil {
			logs.LogWarn("error during close socket: %s", err)
		}
	})

}

//func (sck *socket) SetConnect(value bool) {
//	sck.mutex.Lock()
//	defer sck.mutex.Unlock()
//	sck.connected = value
//}

func (sck *socket) writeAndFlush(buffer []byte) error {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	_, err := sck.writer.Write(buffer)
	if err != nil {
		return err
	}
	err = sck.writer.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) handleWrite(buffer []byte, response *Response) responseError {
	return c.handleWriteWithResponse(buffer, response, true)
}

func (c *Client) handleWriteWithResponse(buffer []byte, response *Response, removeResponse bool) responseError {
	result := c.socket.writeAndFlush(buffer)
	resultCode := waitCodeWithDefaultTimeOut(response)
	/// we need to remove the response before evaluate the
	// buffer errSocket
	if removeResponse {
		result = c.coordinator.RemoveResponseById(response.correlationid)
	}

	if result != nil {
		// we just log
		fmt.Printf("Error handleWrite %s", result)
	}

	return resultCode
}
