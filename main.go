package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

type EventType int

const (
	EventJoin EventType = iota
	EventMessage
	EventLeave
	EventList
	EventShutdown
)

type Event struct {
	Type     EventType
	Username string
	Message  string
	Reply    chan string
}

type Client struct {
	Username string
	Inbox    chan string
	Done     chan struct{}
}

func (c *Client) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case message := <-c.Inbox:
			fmt.Printf("\n[%s] %s\n> ", c.Username, message)

		case <-c.Done:
			return
		}
	}
}

type Server struct {
	events  chan Event
	mu      sync.Mutex
	clients map[string]*Client
	wg      sync.WaitGroup
}

func NewServer() *Server {
	return &Server{
		events:  make(chan Event),
		clients: make(map[string]*Client),
	}
}

func (s *Server) Run() {
	for {
		select {
		case event := <-s.events:
			switch event.Type {
			case EventJoin:
				s.handleJoin(event)

			case EventMessage:
				s.handleMessage(event)

			case EventLeave:
				s.handleLeave(event)

			case EventList:
				s.handleList(event)

			case EventShutdown:
				s.shutdown()
				return
			}
		}
	}
}

func (s *Server) handleJoin(event Event) {
	s.mu.Lock()

	if _, exists := s.clients[event.Username]; exists {
		s.mu.Unlock()
		event.Reply <- "ERROR: Username already exists."
		return
	}

	client := &Client{
		Username: event.Username,
		Inbox:    make(chan string, 20),
		Done:     make(chan struct{}),
	}

	s.clients[event.Username] = client

	s.mu.Unlock()

	s.wg.Add(1)
	go client.Run(&s.wg)

	event.Reply <- "OK"

	s.broadcastExcept(
		event.Username,
		fmt.Sprintf("User %s joined the chat.", event.Username),
	)
}

func (s *Server) handleMessage(event Event) {
	s.mu.Lock()

	_, exists := s.clients[event.Username]

	s.mu.Unlock()

	if !exists {
		event.Reply <- "ERROR: User is not connected."
		return
	}

	message := fmt.Sprintf(
		"%s: %s",
		event.Username,
		event.Message,
	)

	s.broadcastExcept(event.Username, message)

	event.Reply <- "OK"
}

func (s *Server) handleLeave(event Event) {
	s.mu.Lock()

	client, exists := s.clients[event.Username]

	if exists {
		delete(s.clients, event.Username)
	}

	s.mu.Unlock()

	if !exists {
		event.Reply <- "ERROR: User is not connected."
		return
	}

	close(client.Done)

	s.broadcastExcept(
		event.Username,
		fmt.Sprintf("User %s left the chat.", event.Username),
	)

	event.Reply <- "OK"
}

func (s *Server) handleList(event Event) {
	s.mu.Lock()

	users := make([]string, 0, len(s.clients))

	for username := range s.clients {
		users = append(users, username)
	}

	s.mu.Unlock()

	if len(users) == 0 {
		event.Reply <- "No users are currently connected."
		return
	}

	event.Reply <- "Connected users: " +
		strings.Join(users, ", ")
}

func (s *Server) broadcastExcept(
	except string,
	message string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for username, client := range s.clients {
		if username == except {
			continue
		}

		select {
		case client.Inbox <- message:
		default:
		}
	}
}

func (s *Server) shutdown() {
	s.mu.Lock()

	clients := make([]*Client, 0, len(s.clients))

	for _, client := range s.clients {
		clients = append(clients, client)
	}

	s.clients = make(map[string]*Client)

	s.mu.Unlock()

	for _, client := range clients {
		close(client.Done)
	}

	s.wg.Wait()

	fmt.Println("\nServer shut down cleanly.")
}

func printMenu() {
	fmt.Println("===================================")
	fmt.Println("       Concurrent Chat System")
	fmt.Println("===================================")
	fmt.Println("new <username>  - Create a new user")
	fmt.Println("list            - List connected users")
	fmt.Println("use <username>  - Select active user")
	fmt.Println("send <message>  - Send a message")
	fmt.Println("remove <user>   - Remove a user")
	fmt.Println("help            - Show this menu")
	fmt.Println("exit            - Exit the program")
	fmt.Println("===================================")
}

func main() {
	server := NewServer()

	go server.Run()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		os.Interrupt,
		syscall.SIGTERM,
	)

	go func() {
		<-signalChan

		server.events <- Event{
			Type: EventShutdown,
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)

	var selectedUser string

	printMenu()

	for {
		fmt.Print("\n> ")

		if !scanner.Scan() {
			server.events <- Event{
				Type: EventShutdown,
			}
			return
		}

		input := strings.TrimSpace(scanner.Text())

		if input == "" {
			continue
		}

		parts := strings.Fields(input)

		switch parts[0] {

		case "new":
			if len(parts) != 2 {
				fmt.Println("Usage: new <username>")
				continue
			}

			username := parts[1]
			reply := make(chan string)

			server.events <- Event{
				Type:     EventJoin,
				Username: username,
				Reply:    reply,
			}

			result := <-reply

			if result == "OK" {
				fmt.Printf(
					"User %s joined successfully.\n",
					username,
				)
			} else {
				fmt.Println(result)
			}

		case "list":
			reply := make(chan string)

			server.events <- Event{
				Type:  EventList,
				Reply: reply,
			}

			fmt.Println(<-reply)

		case "use":
			if len(parts) != 2 {
				fmt.Println("Usage: use <username>")
				continue
			}

			username := parts[1]
			reply := make(chan string)

			server.events <- Event{
				Type:  EventList,
				Reply: reply,
			}

			users := <-reply

			if users == "No users are currently connected." {
				fmt.Println(users)
				continue
			}

			found := false

			userList := strings.TrimPrefix(
				users,
				"Connected users: ",
			)

			for _, user := range strings.Split(
				userList,
				", ",
			) {
				if user == username {
					found = true
					break
				}
			}

			if !found {
				fmt.Printf(
					"ERROR: User %s does not exist.\n",
					username,
				)
				continue
			}

			selectedUser = username

			fmt.Printf(
				"Now acting as %s.\n",
				username,
			)

		case "send":
			if selectedUser == "" {
				fmt.Println(
					"ERROR: Select a user first using: use <username>",
				)
				continue
			}

			if len(parts) < 2 {
				fmt.Println("Usage: send <message>")
				continue
			}

			message := strings.TrimSpace(
				strings.TrimPrefix(input, "send"),
			)

			reply := make(chan string)

			server.events <- Event{
				Type:     EventMessage,
				Username: selectedUser,
				Message:  message,
				Reply:    reply,
			}

			result := <-reply

			if result != "OK" {
				fmt.Println(result)
			}

		case "remove":
			if len(parts) != 2 {
				fmt.Println("Usage: remove <username>")
				continue
			}

			username := parts[1]
			reply := make(chan string)

			server.events <- Event{
				Type:     EventLeave,
				Username: username,
				Reply:    reply,
			}

			result := <-reply

			if result == "OK" {
				fmt.Printf(
					"User %s removed.\n",
					username,
				)

				if selectedUser == username {
					selectedUser = ""
				}
			} else {
				fmt.Println(result)
			}

		case "help":
			printMenu()

		case "exit":
			server.events <- Event{
				Type: EventShutdown,
			}

			server.wg.Wait()
			return

		default:
			fmt.Println(
				"Unknown command. Type 'help' for available commands.",
			)
		}
	}
}
