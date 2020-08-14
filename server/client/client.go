package client

import (
	"context"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512

	// Maximum amounts of messages to buffer to a client before disconnecting them
	buffedMsgCount = 200
)

var newline = []byte{'\n'}

type OnClient func(send chan<- []byte, recv <-chan []byte)

// Client is a middleman between the websocket connection and the hub.
type Client struct {
	unregister func()

	// The websocket connection.
	conn *websocket.Conn

	// Buffered channel of outbound messages.
	send chan []byte

	ctx context.Context
	cancel context.CancelFunc

	// Buffered channel of inbound messages.
	recv chan []byte
}

// The client stops and unregisters itself once the connection closes
func NewClient(conn *websocket.Conn, unregister func()) *Client {
	ctx, cancel := context.WithCancel(context.Background())

	sendCh := make(chan []byte, buffedMsgCount)
	recvCh := make(chan []byte, buffedMsgCount)

	conn.SetCloseHandler(func(code int, text string) error {
		cancel()
		message := websocket.FormatCloseMessage(code, "")
		conn.WriteControl(websocket.CloseMessage, message, time.Now().Add(writeWait))
		return nil
	})
	return &Client{
		unregister: func() {
			close(sendCh)
			close(recvCh)
			unregister()
		},
		conn:       conn,
		send:       sendCh,
		recv:       recvCh,
		ctx: ctx,
		cancel: cancel,
	}
}

// ReadPump pumps messages from the websocket connection to the client message handler.
//
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (c *Client) ReadPump() {
	defer func() {
		c.cancel()
		c.unregister()
		if err := c.conn.Close(); err != nil {
			log.Printf("Client %v unregistered with an error: %v", c, err)
		}
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		c.recv <- message
	}
	log.Println("quiting client")
}

// WritePump pumps messages from the client send channel to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		c.cancel()
		ticker.Stop()
		if err := c.conn.Close(); err != nil {
			log.Printf("Stopped connection with client %v, but with an error: %v", c, err)
		}
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			log.Printf("sending msg: %x", message)
			if _, err := w.Write(message); err != nil {
				log.Printf("Error when sending msg to client: err: %v", err)
			}

			// Add queued chat messages to the current websocket message.
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(newline)
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) Send() chan<- []byte {
	return c.send
}

func (c *Client) Recv() <-chan []byte {
	return c.recv
}

func (c *Client) Ctx() context.Context {
	return c.ctx
}

func (c *Client) Close() {
	if err := c.conn.Close(); err != nil {
		log.Printf("warning: connection failed to close: %v", err)
	}
	c.cancel()
}
