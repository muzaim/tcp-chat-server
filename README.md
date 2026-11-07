# Go TCP Chat Server

A concurrent, real-time TCP chat server written in Go using the standard library. It coordinates multiple client connections, room-based messaging, server-wide broadcasts, and graceful shutdown through goroutines and channels.

## Features

- **Concurrent Client Connections**: Each client runs dedicated read and write goroutines communicating with a central coordinator loop over Go channels.
- **Room-Based Messaging**: Clients automatically join `#general` on connect and can create or switch rooms dynamically.
- **Broadcast Messaging**: Supports both room-scoped broadcasts and global server-wide announcements.
- **Connection Management**: Tracks active clients, cleans up empty rooms automatically, and handles unexpected disconnects cleanly.
- **Graceful Shutdown**: Intercepts `SIGINT` and `SIGTERM` signals, notifies connected clients, flushes pending messages, and waits for all active goroutines to terminate.

## Project Structure

```text
.
├── main.go         # Entry point, CLI flag parsing, and OS signal handling
├── server.go       # Central hub, room management, command routing, and graceful shutdown
├── client.go       # Client state, input parsing, and buffered outbound writer
├── server_test.go  # Integration tests for concurrency, rooms, broadcast, and shutdown
└── go.mod          # Go module definition
```

## Getting Started

### Prerequisites

- Go 1.22 or later
- A TCP client such as `nc` (netcat) or `telnet`

### Run the Server

```bash
go run . -addr :8080
```

### Connect Clients

Open multiple terminal sessions and connect using `nc` or `telnet`:

```bash
nc localhost 8080
```

## Available Commands

Once connected, any plain text message is sent to your current room. Use slash commands to manage your session:

| Command | Description |
| :--- | :--- |
| `/nick <name>` | Set your display nickname |
| `/join <room>` | Join an existing room or create a new one |
| `/rooms` | List all active rooms and their member counts |
| `/users` | List all users in your current room |
| `/msg <text>` | Send a message to your current room |
| `/all <text>` | Broadcast a message to all connected clients across all rooms |
| `/help` | Display available commands |
| `/quit` | Disconnect from the server |

## Example Session

**Client 1:**
```text
$ nc localhost 8080
[server] welcome to Go TCP Chat Server! Type /help for available commands.
[server] joined room #general
/nick alice
[server] nickname changed to alice
/join backend
[server] joined room #backend
[room:backend] bob joined the room
[#backend] bob: hey alice!
hello bob!
```

**Client 2:**
```text
$ nc localhost 8080
[server] welcome to Go TCP Chat Server! Type /help for available commands.
[server] joined room #general
/nick bob
[server] nickname changed to bob
/join backend
[server] joined room #backend
hey alice!
[#backend] alice: hello bob!
```

## Running Tests

Run the test suite with the race detector enabled:

```bash
go test -v -race ./...
```

## License

MIT
