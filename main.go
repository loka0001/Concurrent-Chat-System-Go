// Assignment: Concurrent Chat System (Terminal UI)

package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
)

type Message struct {
	From string
	Text string
}

type Client struct {
	username string
	inbox    chan string
	done     chan struct{}
	console  *Console
}

func NewClient(username string, console *Console) *Client {
	return &Client{
		username: username,
		inbox:    make(chan string, 32),
		done:     make(chan struct{}),
		console:  console,
	}
}

func (c *Client) run() {
	defer close(c.done)
	for msg := range c.inbox {
		c.console.Printf("[%s] %s\n", c.username, msg)
	}
}

type joinRequest struct {
	client *Client
	reply  chan error
}

type leaveRequest struct {
	username string
	reply    chan error
}

type messageRequest struct {
	message Message
	reply   chan error
}

type Server struct {
	mu      sync.Mutex
	clients map[string]*Client

	joinCh    chan joinRequest
	leaveCh   chan leaveRequest
	messageCh chan messageRequest
	stopCh    chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
}

func NewServer() *Server {
	return &Server{
		clients:   make(map[string]*Client),
		joinCh:    make(chan joinRequest),
		leaveCh:   make(chan leaveRequest),
		messageCh: make(chan messageRequest),
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (s *Server) run() {
	defer close(s.done)

	for {
		select {
		case req := <-s.joinCh:
			req.reply <- s.handleJoin(req.client)

		case req := <-s.leaveCh:
			req.reply <- s.handleLeave(req.username)

		case req := <-s.messageCh:
			req.reply <- s.handleMessage(req.message)

		case <-s.stopCh:
			s.closeAllClients()
			return
		}
	}
}

func (s *Server) handleJoin(client *Client) error {
	s.mu.Lock()
	if _, exists := s.clients[client.username]; exists {
		s.mu.Unlock()
		return fmt.Errorf("username %q is already connected", client.username)
	}

	// Keep the list of users who were already connected. The new user must
	// not receive their own join notification.
	recipients := make([]*Client, 0, len(s.clients))
	for _, existing := range s.clients {
		recipients = append(recipients, existing)
	}

	s.clients[client.username] = client
	s.mu.Unlock()

	go client.run()

	notification := fmt.Sprintf("User %s joined the chat.", client.username)
	for _, recipient := range recipients {
		recipient.inbox <- notification
	}

	return nil
}

func (s *Server) handleLeave(username string) error {
	s.mu.Lock()
	client, exists := s.clients[username]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("user %q is not connected", username)
	}

	delete(s.clients, username)
	recipients := make([]*Client, 0, len(s.clients))
	for _, remaining := range s.clients {
		recipients = append(recipients, remaining)
	}
	s.mu.Unlock()

	close(client.inbox)
	<-client.done

	notification := fmt.Sprintf("User %s left the chat.", username)
	for _, recipient := range recipients {
		recipient.inbox <- notification
	}

	return nil
}

func (s *Server) handleMessage(message Message) error {
	s.mu.Lock()
	_, senderExists := s.clients[message.From]
	if !senderExists {
		s.mu.Unlock()
		return fmt.Errorf("user %q is not connected", message.From)
	}

	recipients := make([]*Client, 0, len(s.clients)-1)
	for username, client := range s.clients {
		if username != message.From {
			recipients = append(recipients, client)
		}
	}
	s.mu.Unlock()

	formatted := fmt.Sprintf("%s: %s", message.From, message.Text)
	for _, recipient := range recipients {
		recipient.inbox <- formatted
	}

	return nil
}

func (s *Server) Join(client *Client) error {
	reply := make(chan error)
	req := joinRequest{client: client, reply: reply}

	select {
	case s.joinCh <- req:
		return <-reply
	case <-s.done:
		return errors.New("server is shutting down")
	}
}

func (s *Server) Leave(username string) error {
	reply := make(chan error)
	req := leaveRequest{username: username, reply: reply}

	select {
	case s.leaveCh <- req:
		return <-reply
	case <-s.done:
		return errors.New("server is shutting down")
	}
}

func (s *Server) Send(from, text string) error {
	reply := make(chan error)
	req := messageRequest{
		message: Message{From: from, Text: text},
		reply:   reply,
	}

	select {
	case s.messageCh <- req:
		return <-reply
	case <-s.done:
		return errors.New("server is shutting down")
	}
}

func (s *Server) UserExists(username string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.clients[username]
	return exists
}

func (s *Server) Usernames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	names := make([]string, 0, len(s.clients))
	for username := range s.clients {
		names = append(names, username)
	}
	sort.Strings(names)
	return names
}

func (s *Server) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

func (s *Server) Shutdown() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		<-s.done
	})
}

func (s *Server) closeAllClients() {
	s.mu.Lock()
	clients := make([]*Client, 0, len(s.clients))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	s.clients = make(map[string]*Client)
	s.mu.Unlock()

	for _, client := range clients {
		close(client.inbox)
	}
	for _, client := range clients {
		<-client.done
	}
}

type Console struct {
	mu sync.Mutex
}

func (c *Console) Printf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf(format, args...)
}

func printHelp(console *Console) {
	console.Printf("Commands:\n")
	console.Printf("  join <username>      Create a new chat user and connect them\n")
	console.Printf("  users                List all connected users\n")
	console.Printf("  select <username>    Choose which user you're acting as\n")
	console.Printf("  send <message>       Send a message as the currently selected user\n")
	console.Printf("  remove <username>    Disconnect a user\n")
	console.Printf("  who                  Show the currently selected user\n")
	console.Printf("  help                 Show this help text\n")
	console.Printf("  quit / exit          Gracefully shut down and exit\n")
}

func main() {
	console := &Console{}
	server := NewServer()
	go server.run()

	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			server.Shutdown()
		})
	}

	// Ctrl+C / termination handling.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	go func() {
		<-signals
		console.Printf("\nShutting down...\n")
		shutdown()
		// Unblock Scanner if it is currently waiting for input.
		_ = os.Stdin.Close()
	}()

	printHelp(console)
	console.Printf("------------------------------------------------------------\n")

	selectedUser := ""
	scanner := bufio.NewScanner(os.Stdin)

	for {
		console.Printf("> ")
		if !scanner.Scan() {
			shutdown()
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		command := strings.ToLower(parts[0])
		argument := ""
		if len(parts) == 2 {
			argument = strings.TrimSpace(parts[1])
		}

		switch command {
		case "join":
			if argument == "" || strings.ContainsAny(argument, " \t") {
				console.Printf("error: usage: join <username>\n")
				continue
			}

			client := NewClient(argument, console)
			if err := server.Join(client); err != nil {
				console.Printf("error: %v\n", err)
				continue
			}

			console.Printf("%s has joined the chat.\n", argument)
			if selectedUser == "" {
				selectedUser = argument
				console.Printf("(now acting as %s)\n", selectedUser)
			}

		case "users":
			names := server.Usernames()
			if len(names) == 0 {
				console.Printf("No connected users.\n")
				continue
			}

			// Match the example style by showing the selected user first.
			if selectedUser != "" && server.UserExists(selectedUser) {
				console.Printf("* %s\n", selectedUser)
			}
			for _, name := range names {
				if name != selectedUser {
					console.Printf("  %s\n", name)
				}
			}

		case "select":
			if argument == "" || strings.ContainsAny(argument, " \t") {
				console.Printf("error: usage: select <username>\n")
				continue
			}
			if !server.UserExists(argument) {
				console.Printf("error: user %q is not connected\n", argument)
				continue
			}
			selectedUser = argument
			console.Printf("now acting as %s\n", selectedUser)

		case "send":
			if selectedUser == "" || !server.UserExists(selectedUser) {
				console.Printf("error: no connected user is currently selected\n")
				continue
			}
			if argument == "" {
				console.Printf("error: usage: send <message>\n")
				continue
			}

			if err := server.Send(selectedUser, argument); err != nil {
				console.Printf("error: %v\n", err)
				continue
			}
			// The sender does not receive its own message through its inbox;
			// this line is only the local terminal echo shown in the example.
			console.Printf("%s: %s\n", selectedUser, argument)

		case "remove":
			if argument == "" || strings.ContainsAny(argument, " \t") {
				console.Printf("error: usage: remove <username>\n")
				continue
			}

			if err := server.Leave(argument); err != nil {
				console.Printf("error: %v\n", err)
				continue
			}
			console.Printf("%s has left the chat.\n", argument)
			if selectedUser == argument {
				selectedUser = ""
			}

		case "who":
			if selectedUser == "" || !server.UserExists(selectedUser) {
				console.Printf("acting as: no user selected\n")
			} else {
				console.Printf("acting as: %s\n", selectedUser)
			}

		case "help":
			printHelp(console)

		case "quit", "exit":
			shutdown()
			return

		default:
			console.Printf("error: unknown command %q. Type 'help' for available commands.\n", command)
		}
	}
}
