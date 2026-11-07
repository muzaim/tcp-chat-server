package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
)

type room struct {
	name    string
	members map[*client]bool
}

func (r *room) broadcast(sender *client, msg string) {
	for member := range r.members {
		if member != sender {
			member.send(msg)
		}
	}
}

type Server struct {
	addr     string
	listener net.Listener
	rooms    map[string]*room
	clients  map[*client]bool
	register chan *client
	commands chan command
	quit     chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
}

func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		rooms:    make(map[string]*room),
		clients:  make(map[*client]bool),
		register: make(chan *client),
		commands: make(chan command, 64),
		quit:     make(chan struct{}),
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	log.Printf("TCP chat server listening on %s", ln.Addr().String())

	s.wg.Add(1)
	go s.run()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				if errors.Is(err, net.ErrClosed) {
					return nil
				}
				log.Printf("accept error: %v", err)
				continue
			}
		}

		c := newClient(conn, s.commands)
		select {
		case s.register <- c:
		case <-s.quit:
			_ = conn.Close()
			return nil
		}
	}
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return s.addr
	}
	return s.listener.Addr().String()
}

func (s *Server) Shutdown() {
	select {
	case <-s.quit:
		return
	default:
		close(s.quit)
	}

	s.mu.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	log.Println("server stopped gracefully")
}

func (s *Server) run() {
	defer s.wg.Done()

	for {
		select {
		case c := <-s.register:
			s.clients[c] = true
			s.wg.Add(2)
			go func() {
				defer s.wg.Done()
				c.writeOutput()
			}()
			go func() {
				defer s.wg.Done()
				c.readInput()
			}()

			c.send("[server] welcome to Go TCP Chat Server! Type /help for available commands.")
			s.joinRoom(c, []string{"general"})

		case cmd := <-s.commands:
			if !s.clients[cmd.client] {
				continue
			}
			switch cmd.id {
			case cmdNick:
				s.setNick(cmd.client, cmd.args)
			case cmdJoin:
				s.joinRoom(cmd.client, cmd.args)
			case cmdRooms:
				s.listRooms(cmd.client)
			case cmdUsers:
				s.listUsers(cmd.client)
			case cmdMsg:
				s.sendRoomMsg(cmd.client, cmd.args)
			case cmdBroadcast:
				s.broadcastAll(cmd.client, cmd.args)
			case cmdHelp:
				s.sendHelp(cmd.client)
			case cmdQuit:
				s.removeClient(cmd.client)
			}

		case <-s.quit:
			for c := range s.clients {
				c.send("[server] server is shutting down. Goodbye!")
				c.close()
				delete(s.clients, c)
			}
			return
		}
	}
}

func (s *Server) setNick(c *client, args []string) {
	if len(args) == 0 {
		c.send("[server] usage: /nick <name>")
		return
	}
	newNick := args[0]
	oldNick := c.nick
	c.nick = newNick
	c.send(fmt.Sprintf("[server] nickname changed to %s", newNick))
	if c.room != nil {
		c.room.broadcast(c, fmt.Sprintf("[room:%s] %s is now known as %s", c.room.name, oldNick, newNick))
	}
}

func (s *Server) joinRoom(c *client, args []string) {
	if len(args) == 0 {
		c.send("[server] usage: /join <room>")
		return
	}
	roomName := strings.TrimPrefix(args[0], "#")
	if roomName == "" {
		c.send("[server] invalid room name")
		return
	}

	r, exists := s.rooms[roomName]
	if !exists {
		r = &room{
			name:    roomName,
			members: make(map[*client]bool),
		}
		s.rooms[roomName] = r
	}

	if c.room == r {
		c.send(fmt.Sprintf("[server] already in room #%s", roomName))
		return
	}

	s.leaveCurrentRoom(c)
	r.members[c] = true
	c.room = r

	r.broadcast(c, fmt.Sprintf("[room:%s] %s joined the room", r.name, c.nick))
	c.send(fmt.Sprintf("[server] joined room #%s", r.name))
}

func (s *Server) leaveCurrentRoom(c *client) {
	if c.room == nil {
		return
	}
	oldRoom := c.room
	delete(oldRoom.members, c)
	oldRoom.broadcast(c, fmt.Sprintf("[room:%s] %s left the room", oldRoom.name, c.nick))
	if len(oldRoom.members) == 0 {
		delete(s.rooms, oldRoom.name)
	}
	c.room = nil
}

func (s *Server) listRooms(c *client) {
	if len(s.rooms) == 0 {
		c.send("[server] no active rooms")
		return
	}
	names := make([]string, 0, len(s.rooms))
	for name, r := range s.rooms {
		names = append(names, fmt.Sprintf("#%s (%d)", name, len(r.members)))
	}
	sort.Strings(names)
	c.send(fmt.Sprintf("[server] active rooms: %s", strings.Join(names, ", ")))
}

func (s *Server) listUsers(c *client) {
	if c.room == nil {
		c.send("[server] you are not in a room")
		return
	}
	users := make([]string, 0, len(c.room.members))
	for member := range c.room.members {
		users = append(users, member.nick)
	}
	sort.Strings(users)
	c.send(fmt.Sprintf("[room:%s] users: %s", c.room.name, strings.Join(users, ", ")))
}

func (s *Server) sendRoomMsg(c *client, args []string) {
	if len(args) == 0 {
		c.send("[server] message cannot be empty")
		return
	}
	if c.room == nil {
		c.send("[server] join a room first using /join <room>")
		return
	}
	msg := strings.Join(args, " ")
	c.room.broadcast(c, fmt.Sprintf("[#%s] %s: %s", c.room.name, c.nick, msg))
}

func (s *Server) broadcastAll(c *client, args []string) {
	if len(args) == 0 {
		c.send("[server] usage: /all <message>")
		return
	}
	msg := strings.Join(args, " ")
	formatted := fmt.Sprintf("[broadcast] %s: %s", c.nick, msg)
	for member := range s.clients {
		if member != c {
			member.send(formatted)
		}
	}
}

func (s *Server) sendHelp(c *client) {
	help := []string{
		"[server] commands:",
		"  /nick <name>   - set your nickname",
		"  /join <room>   - join or create a room",
		"  /rooms         - list active rooms",
		"  /users         - list users in current room",
		"  /msg <text>    - send message to current room (or just type text)",
		"  /all <text>    - broadcast message to all connected clients",
		"  /quit          - disconnect from the server",
	}
	c.send(strings.Join(help, "\n"))
}

func (s *Server) removeClient(c *client) {
	if !s.clients[c] {
		return
	}
	s.leaveCurrentRoom(c)
	delete(s.clients, c)
	c.send("[server] disconnected. Goodbye!")
	c.close()
}
