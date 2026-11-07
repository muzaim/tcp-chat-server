package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

type commandID int

const (
	cmdNick commandID = iota
	cmdJoin
	cmdRooms
	cmdUsers
	cmdMsg
	cmdBroadcast
	cmdHelp
	cmdQuit
)

type command struct {
	id     commandID
	client *client
	args   []string
}

type client struct {
	conn      net.Conn
	nick      string
	room      *room
	commands  chan<- command
	outbound  chan string
	done      chan struct{}
	closeOnce sync.Once
}

func newClient(conn net.Conn, commands chan<- command) *client {
	return &client{
		conn:     conn,
		nick:     "anonymous",
		commands: commands,
		outbound: make(chan string, 32),
		done:     make(chan struct{}),
	}
}

func (c *client) enqueue(cmd command) {
	select {
	case c.commands <- cmd:
	case <-c.done:
	}
}

func (c *client) readInput() {
	scanner := bufio.NewScanner(c.conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "/") {
			parts := strings.Fields(line)
			cmd := strings.ToLower(parts[0])
			args := parts[1:]

			switch cmd {
			case "/nick":
				c.enqueue(command{id: cmdNick, client: c, args: args})
			case "/join":
				c.enqueue(command{id: cmdJoin, client: c, args: args})
			case "/rooms":
				c.enqueue(command{id: cmdRooms, client: c})
			case "/users":
				c.enqueue(command{id: cmdUsers, client: c})
			case "/all":
				c.enqueue(command{id: cmdBroadcast, client: c, args: args})
			case "/msg":
				c.enqueue(command{id: cmdMsg, client: c, args: args})
			case "/help":
				c.enqueue(command{id: cmdHelp, client: c})
			case "/quit":
				c.enqueue(command{id: cmdQuit, client: c})
				return
			default:
				c.send(fmt.Sprintf("[server] unknown command: %s (type /help for commands)", cmd))
			}
			continue
		}

		c.enqueue(command{
			id:     cmdMsg,
			client: c,
			args:   strings.Fields(line),
		})
	}

	c.enqueue(command{id: cmdQuit, client: c})
}

func (c *client) writeOutput() {
	defer func() {
		_ = c.conn.Close()
	}()

	for {
		select {
		case msg := <-c.outbound:
			if _, err := fmt.Fprintln(c.conn, msg); err != nil {
				return
			}
		case <-c.done:
			for {
				select {
				case msg := <-c.outbound:
					_, _ = fmt.Fprintln(c.conn, msg)
				default:
					return
				}
			}
		}
	}
}

func (c *client) send(msg string) {
	select {
	case <-c.done:
		return
	default:
	}

	select {
	case c.outbound <- msg:
	case <-c.done:
	default:
	}
}

func (c *client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.SetReadDeadline(time.Now())
	})
}
