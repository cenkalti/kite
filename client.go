package kite

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go.net/websocket"
	"github.com/cenkalti/backoff"
	"github.com/koding/kite/dnode"
	"github.com/koding/kite/protocol"
)

// Default timeout value for Client.Tell method.
// It can be overriden with Client.SetTellTimeout.
const DefaultTellTimeout = 4 * time.Second

var forever = backoff.NewExponentialBackoff()

func init() {
	forever.MaxElapsedTime = 365 * 24 * time.Hour // 1 year
}

// Client is the client for communicating with another Kite.
// It has Tell() and Go() methods for calling methods sync/async way.
type Client struct {
	// The information about the kite that we are connecting to.
	protocol.Kite

	// A reference to the current Kite running.
	LocalKite *Kite

	// Credentials that we sent in each request.
	Authentication *Authentication

	// Should we reconnect if disconnected?
	Reconnect bool

	// Websocket connection options.
	WSConfig *websocket.Config

	// Should we process incoming messages concurrently or not? Default: true
	Concurrent bool

	// To signal waiters of Go() on disconnect.
	disconnect chan struct{}

	// Duration to wait reply from remote when making a request with Tell().
	tellTimeout time.Duration

	// Websocket connection
	conn *websocket.Conn

	// dnode scrubber for saving callbacks sent to remote.
	scrubber *dnode.Scrubber

	// Time to wait before redial connection.
	redialBackOff backoff.ExponentialBackoff

	// on connect/disconnect handlers are invoked after every
	// connect/disconnect.
	onConnectHandlers    []func()
	onDisconnectHandlers []func()

	// For protecting access over OnConnect and OnDisconnect handlers.
	m sync.RWMutex

	firstRequestHandlersNotified sync.Once
}

// NewClient returns a pointer to a new Client. The returned instance
// is not connected. You have to call Dial() or DialForever() before calling
// Tell() and Go() methods.
func (k *Kite) NewClient(remoteURL *url.URL) *Client {
	// Must send an "Origin" header. Does not checked on server.
	origin, _ := url.Parse("")

	r := &Client{
		LocalKite:     k,
		disconnect:    make(chan struct{}),
		redialBackOff: *forever,
		scrubber:      dnode.NewScrubber(),
		WSConfig: &websocket.Config{
			Version:  websocket.ProtocolVersionHybi13,
			Origin:   origin,
			Location: remoteURL,
		},
		Concurrent: true,
	}
	r.SetTellTimeout(DefaultTellTimeout)

	var m sync.Mutex
	r.OnDisconnect(func() {
		m.Lock()
		close(r.disconnect)
		r.disconnect = make(chan struct{})
		m.Unlock()
	})

	return r
}

// NewClientString creates a new Client from a URL string.
func (k *Kite) NewClientString(remoteURL string) *Client {
	parsed, err := url.Parse(remoteURL)
	if err != nil {
		panic(err)
	}
	return k.NewClient(parsed)
}

// SetTellTimeout sets the timeout duration for requests made with Tell().
func (c *Client) SetTellTimeout(d time.Duration) { c.tellTimeout = d }

func (c *Client) RemoteAddr() string {
	if c.conn != nil {
		if req := c.conn.Request(); req != nil {
			return req.RemoteAddr
		}
	}
	return ""
}

// Dial connects to the remote Kite. Returns error if it can't.
func (c *Client) Dial() (err error) {
	c.LocalKite.Log.Info("Dialing remote kite: [%s %s]", c.Kite.Name, c.WSConfig.Location.String())

	if err := c.dial(); err != nil {
		return err
	}

	go c.run()

	return nil
}

// Dial connects to the remote Kite. If it can't connect, it retries indefinitely.
func (c *Client) DialForever() (connected chan bool, err error) {
	c.LocalKite.Log.Info("Dialing remote kite: [%s %s]", c.Kite.Name, c.WSConfig.Location.String())

	c.Reconnect = true
	connected = make(chan bool, 1)
	go c.dialForever(connected)
	return
}

func (c *Client) dial() (err error) {
	// Reset the wait time.
	defer c.redialBackOff.Reset()

	fixPortNumber(c.WSConfig.Location)

	c.conn, err = websocket.DialConfig(c.WSConfig)
	if err != nil {
		return err
	}

	// Must be run in a goroutine because a handler may wait a response from
	// server.
	go c.callOnConnectHandlers()

	return nil
}

// fixPortNumber appends 80 or 443 depending on the scheme
// if there is no port number in the URL.
func fixPortNumber(u *url.URL) {
	_, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		if missingPortErr, ok := err.(*net.AddrError); ok && missingPortErr.Err == "missing port in address" {
			var port string
			switch u.Scheme {
			case "ws":
				port = "80"
			case "wss":
				port = "443"
			default:
				panic("unknown scheme: " + u.Scheme)
			}
			u.Host = net.JoinHostPort(strings.TrimRight(missingPortErr.Addr, ":"), port)
		} else {
			panic(err) // Other kind of error
		}
	}
}

func (c *Client) dialForever(connectNotifyChan chan bool) {
	dial := func() error {
		if !c.Reconnect {
			return nil
		}
		return c.dial()
	}

	backoff.Retry(dial, &c.redialBackOff) // this will retry dial forever

	close(connectNotifyChan) // This is executed only once.

	go c.run()
}

// run consumes incoming dnode messages. Reconnects if necessary.
func (c *Client) run() (err error) {
	for {
		err = c.readLoop()

		// falls here when connection disconnects
		c.callOnDisconnectHandlers()

		if !c.Reconnect {
			break
		}

		// Redial
		connected := make(chan bool, 1)
		go c.dialForever(connected)
		<-connected
	}
	return
}

// readLoop reads a message from websocket and processes it.
func (c *Client) readLoop() error {
	for {
		msg, err := c.receiveData()
		if err != nil {
			return err
		}

		processed := make(chan bool)
		go func(msg []byte, processed chan bool) {
			if err := c.processMessage(msg); err != nil {
				c.LocalKite.Log.Warning("error processing message err: %s message: %q", err.Error(), string(msg))
			}
			close(processed)
		}(msg, processed)

		if !c.Concurrent {
			<-processed
		}
	}
}

// processMessage processes a single message and calls a handler or callback.
func (c *Client) processMessage(data []byte) error {
	var (
		err     error
		ok      bool
		msg     dnode.Message
		handler HandlerFunc
	)

	// Call error handler.
	defer func() {
		if err != nil {
			onError(err)
		}
	}()

	if err = json.Unmarshal(data, &msg); err != nil {
		return err
	}

	sender := func(id uint64, args []interface{}) error {
		_, err := c.marshalAndSend(id, args)
		return err
	}

	// Replace function placeholders with real functions.
	if err = dnode.ParseCallbacks(&msg, sender); err != nil {
		return err
	}

	// Find the handler function. Method may be string or integer.
	switch method := msg.Method.(type) {
	case float64:
		id := uint64(method)
		callback := c.scrubber.GetCallback(id)
		if callback == nil {
			err = CallbackNotFoundError{id, Arguments{msg.Arguments}}
			return err
		}
		c.runCallback(callback, msg.Arguments)
	case string:
		if handler, ok = c.LocalKite.handlers[method]; !ok {
			err = MethodNotFoundError{method, Arguments{msg.Arguments}}
			return err
		}
		c.runMethod(method, handler, msg.Arguments)
	default:
		return fmt.Errorf("Mehtod is not string or integer: %+v (%T)", msg.Method, msg.Method)
	}
	return nil
}

func (c *Client) Close() {
	c.Reconnect = false
	if c.conn != nil {
		c.conn.Close()
	}
}

// sendData sends the msg over the websocket.
func (c *Client) sendData(msg []byte) error {
	if os.Getenv("DNODE_PRINT_SEND") != "" {
		fmt.Fprintf(os.Stderr, "\nSending: %s\n", string(msg))
	}

	if c.conn == nil {
		return errors.New("not connected")
	}

	return websocket.Message.Send(c.conn, string(msg))
}

// receiveData reads a message from the websocket.
func (c *Client) receiveData() ([]byte, error) {
	if c.conn == nil {
		return nil, errors.New("not connected")
	}

	var msg []byte
	err := websocket.Message.Receive(c.conn, &msg)

	if os.Getenv("DNODE_PRINT_RECV") != "" {
		fmt.Fprintf(os.Stderr, "\nReceived: %s\n", string(msg))
	}

	return msg, err
}

// OnConnect registers a function to run on connect.
func (c *Client) OnConnect(handler func()) {
	c.m.Lock()
	c.onConnectHandlers = append(c.onConnectHandlers, handler)
	c.m.Unlock()
}

// OnDisconnect registers a function to run on disconnect.
func (c *Client) OnDisconnect(handler func()) {
	c.m.Lock()
	c.onDisconnectHandlers = append(c.onDisconnectHandlers, handler)
	c.m.Unlock()
}

// callOnConnectHandlers runs the registered connect handlers.
func (c *Client) callOnConnectHandlers() {
	c.m.RLock()
	for _, handler := range c.onConnectHandlers {
		func() {
			defer recover()
			handler()
		}()
	}
	c.m.RUnlock()
}

// callOnDisconnectHandlers runs the registered disconnect handlers.
func (c *Client) callOnDisconnectHandlers() {
	c.m.RLock()
	for _, handler := range c.onDisconnectHandlers {
		func() {
			defer recover()
			handler()
		}()
	}
	c.m.RUnlock()
}

// callOptions is the type of first argument in the dnode message.
// It is used when unmarshalling a dnode message.
type callOptions struct {
	// Arguments to the method
	Kite             protocol.Kite   `json:"kite" dnode:"-"`
	Authentication   *Authentication `json:"authentication"`
	WithArgs         *dnode.Partial  `json:"withArgs" dnode:"-"`
	ResponseCallback Function        `json:"responseCallback"`
}

// callOptionsOut is the same structure with callOptions.
// It is used when marshalling a dnode message.
type callOptionsOut struct {
	callOptions

	// Override this when sending because args will not be a *dnode.Partial.
	WithArgs []interface{} `json:"withArgs"`
}

func (c *Client) wrapMethodArgs(args []interface{}, responseCallback Function) []interface{} {
	options := callOptionsOut{
		WithArgs: args,
		callOptions: callOptions{
			Kite:             *c.LocalKite.Kite(),
			Authentication:   c.Authentication,
			ResponseCallback: responseCallback,
		},
	}
	return []interface{}{options}
}

// Authentication is used when connecting a Client.
type Authentication struct {
	// Type can be "kiteKey", "token" or "sessionID" for now.
	Type string `json:"type"`
	Key  string `json:"key"`
}

// response is the type of the return value of Tell() and Go() methods.
type response struct {
	Result *Arguments
	Err    error
}

// Tell makes a blocking method call to the server.
// Waits until the callback function is called by the other side and
// returns the result and the error.
func (c *Client) Tell(method string, args ...interface{}) (result *Arguments, err error) {
	return c.TellWithTimeout(method, 0, args...)
}

// TellWithTimeout does the same thing with Tell() method except it takes an
// extra argument that is the timeout for waiting reply from the remote Kite.
// If timeout is given 0, the behavior is same as Tell().
func (c *Client) TellWithTimeout(method string, timeout time.Duration, args ...interface{}) (result *Arguments, err error) {
	response := <-c.GoWithTimeout(method, timeout, args...)
	return response.Result, response.Err
}

// Go makes an unblocking method call to the server.
// It returns a channel that the caller can wait on it to get the response.
func (c *Client) Go(method string, args ...interface{}) chan *response {
	return c.GoWithTimeout(method, 0, args...)
}

// GoWithTimeout does the same thing with Go() method except it takes an
// extra argument that is the timeout for waiting reply from the remote Kite.
// If timeout is given 0, the behavior is same as Go().
func (c *Client) GoWithTimeout(method string, timeout time.Duration, args ...interface{}) chan *response {
	// We will return this channel to the caller.
	// It can wait on this channel to get the response.
	c.LocalKite.Log.Debug("Telling method [%s] on kite [%s]", method, c.Name)
	responseChan := make(chan *response, 1)

	c.sendMethod(method, args, timeout, responseChan)

	return responseChan
}

// sendMethod wraps the arguments, adds a response callback,
// marshals the message and send it over websocket.
func (c *Client) sendMethod(method string, args []interface{}, timeout time.Duration, responseChan chan *response) {
	// To clean the sent callback after response is received.
	// Send/Receive in a channel to prevent race condition because
	// the callback is run in a separate goroutine.
	removeCallback := make(chan uint64, 1)

	// When a callback is called it will send the response to this channel.
	doneChan := make(chan *response, 1)

	cb := c.makeResponseCallback(doneChan, removeCallback, method, args)
	args = c.wrapMethodArgs(args, cb)

	// BUG: This sometimes does not return an error, even if the remote
	// kite is disconnected. I could not find out why.
	// Timeout below in goroutine saves us in this case.
	callbacks, err := c.marshalAndSend(method, args)
	if err != nil {
		responseChan <- &response{
			Result: nil,
			Err:    &Error{"sendError", err.Error()},
		}
		return
	}

	// Use default timeout from r (Client) if zero.
	if timeout == 0 {
		timeout = c.tellTimeout
	}

	// Waits until the response has came or the connection has disconnected.
	go func() {
		select {
		case resp := <-doneChan:
			responseChan <- resp
		case <-c.disconnect:
			responseChan <- &response{nil, &Error{"disconnect", "Remote kite has disconnected"}}
		case <-time.After(timeout):
			responseChan <- &response{nil, &Error{"timeout", fmt.Sprintf("No response to %q method in %s", method, timeout)}}

			// Remove the callback function from the map so we do not
			// consume memory for unused callbacks.
			if id, ok := <-removeCallback; ok {
				c.scrubber.RemoveCallback(id)
			}
		}
	}()

	sendCallbackID(callbacks, removeCallback)
}

// marshalAndSend takes a method and arguments, scrubs the arguments to create
// a dnode message, marshals the message to JSON and sends it over the wire.
func (c *Client) marshalAndSend(method interface{}, arguments []interface{}) (callbacks map[string]dnode.Path, err error) {
	callbacks = c.scrubber.Scrub(arguments)

	defer func() {
		if err != nil {
			c.removeCallbacks(callbacks)
		}
	}()

	// Do not encode empty arguments as "null", make it "[]".
	if arguments == nil {
		arguments = make([]interface{}, 0)
	}

	rawArgs, err := json.Marshal(arguments)
	if err != nil {
		return nil, err
	}

	msg := dnode.Message{
		Method:    method,
		Arguments: &dnode.Partial{Raw: rawArgs},
		Callbacks: callbacks,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	err = c.sendData(data)
	return
}

// Used to remove callbacks after error occurs in send().
func (c *Client) removeCallbacks(callbacks map[string]dnode.Path) {
	for sid, _ := range callbacks {
		// We don't check for error because we have created
		// the callbacks map in the send function above.
		// It does not come from remote, so cannot contain errors.
		id, _ := strconv.ParseUint(sid, 10, 64)
		c.scrubber.RemoveCallback(id)
	}
}

// sendCallbackID send the callback number to be deleted after response is received.
func sendCallbackID(callbacks map[string]dnode.Path, ch chan<- uint64) {
	// TODO fix finding of responseCallback in dnode message when removing callback
	for id, path := range callbacks {
		if len(path) != 2 {
			continue
		}
		p0, ok := path[0].(string)
		if !ok {
			continue
		}
		p1, ok := path[1].(string)
		if !ok {
			continue
		}
		if p0 != "0" || p1 != "responseCallback" {
			continue
		}
		i, _ := strconv.ParseUint(id, 10, 64)
		ch <- i
		return
	}
	close(ch)
}

// makeResponseCallback prepares and returns a callback function sent to the server.
// The caller of the Tell() is blocked until the server calls this callback function.
// Sets theResponse and notifies the caller by sending to done channel.
func (c *Client) makeResponseCallback(doneChan chan *response, removeCallback <-chan uint64, method string, args []interface{}) Function {
	return Callback(func(arguments *Arguments) {
		// Single argument of response callback.
		var resp struct {
			Result *dnode.Partial `json:"result"`
			Err    *Error         `json:"error"`
		}

		// Notify that the callback is finished.
		defer func() {
			if resp.Err != nil {
				c.LocalKite.Log.Warning("Error received from kite: %q method: %q args: %#v err: %s", c.Kite.Name, method, args, resp.Err.Error())
				doneChan <- &response{&Arguments{resp.Result}, resp.Err}
			} else {
				doneChan <- &response{&Arguments{resp.Result}, nil}
			}
		}()

		// Remove the callback function from the map so we do not
		// consume memory for unused callbacks.
		if id, ok := <-removeCallback; ok {
			c.scrubber.RemoveCallback(id)
		}

		// We must only get one argument for response callback.
		arg, err := arguments.SliceOfLength(1)
		if err != nil {
			resp.Err = &Error{Type: "invalidResponse", Message: err.Error()}
			return
		}

		// Unmarshal callback response argument.
		err = arg[0].Unmarshal(&resp)
		if err != nil {
			resp.Err = &Error{Type: "invalidResponse", Message: err.Error()}
			return
		}

		// At least result or error must be sent.
		keys := make(map[string]interface{})
		err = arg[0].Unmarshal(&keys)
		_, ok1 := keys["result"]
		_, ok2 := keys["error"]
		if !ok1 && !ok2 {
			resp.Err = &Error{
				Type:    "invalidResponse",
				Message: "Server has sent invalid response arguments",
			}
			return
		}
	})
}

// onError is called when an error happened in a method handler.
func onError(err error) {
	// TODO do not marshal options again here
	switch e := err.(type) {
	case MethodNotFoundError: // Tell the requester "method is not found".
		args, err2 := e.Args.Slice()
		if err2 != nil {
			return
		}

		if len(args) < 1 {
			return
		}

		var options callOptions
		if err := args[0].Unmarshal(&options); err != nil {
			return
		}

		if options.ResponseCallback.Caller != nil {
			response := Response{
				Result: nil,
				Error:  &Error{"methodNotFound", err.Error()},
			}
			options.ResponseCallback.Call(response)
		}
	}
}
