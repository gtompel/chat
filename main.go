package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv" // Для чтения .env файлов
	"gorm.io/driver/sqlite"    // Или другой драйвер БД
	"gorm.io/gorm"
)

// Структура для хранения информации о пользователе
type User struct {
	ID       uint   `gorm:"primaryKey"`
	Username string `gorm:"unique"`
	Password string // TODO: Хранить хеш пароля!
}

// Структура для хранения информации о сообщении
type Message struct {
	ID        uint      `gorm:"primaryKey"`
	Room      string    `gorm:"index"`
	Username  string    // Имя пользователя, отправившего сообщение
	Content   string    `gorm:"size:2048"` // Содержимое сообщения
	CreatedAt time.Time // Время создания сообщения
}

// Структура для хранения информации о клиенте
type Client struct {
	conn     *websocket.Conn
	room     string
	username string // Имя пользователя
}

// Структура для хранения информации о комнате чата
type Room struct {
	clients map[*Client]bool
	mu      sync.Mutex
	db      *gorm.DB // Ссылка на базу данных
}

// Глобальные переменные
var (
	rooms    = make(map[string]*Room)
	db       *gorm.DB
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true // В продакшене нужно проверять origin!
		},
	}
)

func main() {
	// Загрузка переменных окружения из .env файла
	err := godotenv.Load()
	if err != nil {
		log.Println("Ошибка при загрузке .env файла: ", err)
		// Не критическая ошибка, продолжаем работу
	}

	// Инициализация базы данных
	db, err = gorm.Open(sqlite.Open("chat.db"), &gorm.Config{}) // Используем SQLite
	if err != nil {
		log.Fatal("Ошибка при подключении к базе данных:", err)
	}

	// Автомиграция (создание таблиц)
	db.AutoMigrate(&User{}, &Message{})

	// Обработчики
	http.Handle("/", http.FileServer(http.Dir("static")))
	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/register", handleRegister)
	http.HandleFunc("/login", handleLogin)
	http.HandleFunc("/rooms", handleGetRooms)

	// Запуск сервера
	fmt.Println("Сервер запущен на http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// --------------------- Обработчики HTTP ---------------------

func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")

	if username == "" || password == "" {
		http.Error(w, "Необходимо указать имя пользователя и пароль", http.StatusBadRequest)
		return
	}

	// TODO: Хешировать пароль!
	user := User{Username: username, Password: password}
	result := db.Create(&user)
	if result.Error != nil {
		http.Error(w, "Ошибка при регистрации пользователя", http.StatusInternalServerError)
		log.Println("Ошибка регистрации: ", result.Error)
		return
	}

	fmt.Fprintln(w, "Пользователь успешно зарегистрирован")
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")

	if username == "" || password == "" {
		http.Error(w, "Необходимо указать имя пользователя и пароль", http.StatusBadRequest)
		return
	}

	var user User
	result := db.Where("username = ? AND password = ?", username, password).First(&user) // TODO: Проверять хеш пароля
	if result.Error != nil {
		http.Error(w, "Неверное имя пользователя или пароль", http.StatusUnauthorized)
		log.Println("Ошибка при логине: ", result.Error)
		return
	}

	// TODO:  Можно сгенерировать JWT токен и вернуть его клиенту
	fmt.Fprintln(w, "Успешный вход")
}

func handleGetRooms(w http.ResponseWriter, r *http.Request) {
	var roomList []string

	for roomName := range rooms {
		roomList = append(roomList, roomName)
	}

	responseJSON, err := json.Marshal(roomList)

	if err != nil {
		http.Error(w, "Ошибка при формировании списка комнат", http.StatusInternalServerError)
		log.Println("Ошибка marshal: ", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseJSON)
}

// --------------------- WebSocket ---------------------

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Аутентификация пользователя (пример, можно заменить на JWT)
	username := r.URL.Query().Get("username") // Получаем имя пользователя из query
	if username == "" {
		log.Println("Не указано имя пользователя при подключении к WebSocket")
		return // Закрываем соединение
	}

	// Получаем комнату
	roomName := r.URL.Query().Get("room")
	if roomName == "" {
		roomName = "default"
	}

	// Upgrade до WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Ошибка при Upgrade соединения:", err)
		return
	}

	// Создаем клиента
	client := &Client{conn: conn, room: roomName, username: username}
	log.Printf("Новое подключение: %s, комната: %s\n", username, roomName)

	// Получаем или создаем комнату
	room := getOrCreateRoom(roomName)
	room.addClient(client)

	// Отправляем историю сообщений
	sendHistory(client, roomName)

	// Запускаем обработку сообщений
	go client.handleMessages()
}

// --------------------- Комнаты ---------------------

func getOrCreateRoom(roomName string) *Room {
	if _, ok := rooms[roomName]; !ok {
		rooms[roomName] = &Room{
			clients: make(map[*Client]bool),
			db:      db,
		}
	}
	return rooms[roomName]
}

func (r *Room) addClient(client *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[client] = true
}

func (r *Room) removeClient(client *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, client)
}

// --------------------- Сообщения ---------------------

func (r *Room) broadcast(messageType int, message []byte, sender *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()

	msgStr := string(message) // Конвертируем []byte в string для сохранения в БД

	// Сохраняем сообщение в БД
	newMessage := Message{
		Room:      r.roomName(), // Предполагаем, что Room имеет метод roomName()
		Username:  sender.username,
		Content:   msgStr,
		CreatedAt: time.Now(),
	}
	result := r.db.Create(&newMessage)
	if result.Error != nil {
		log.Println("Ошибка при сохранении сообщения в БД:", result.Error)
		// TODO: Обработать ошибку более корректно (например, отправить сообщение об ошибке клиенту)
	}

	for client := range r.clients {
		err := client.conn.WriteMessage(messageType, message)
		if err != nil {
			log.Printf("Ошибка при отправке сообщения клиенту %s: %v\n", client.username, err)
			r.removeClient(client)
			client.conn.Close()
		}
	}
}

func (r *Room) roomName() string {
	for name, rm := range rooms {
		if rm == r {
			return name
		}
	}
	return "" // Или какое-то значение по умолчанию
}

func sendHistory(client *Client, roomName string) {
	var messages []Message
	result := db.Where("room = ?", roomName).Order("created_at DESC").Limit(10).Find(&messages) // Последние 10 сообщений
	if result.Error != nil {
		log.Println("Ошибка при получении истории сообщений:", result.Error)
		return
	}

	for i := len(messages) - 1; i >= 0; i-- { // Отправляем в обратном порядке (от старых к новым)
		msg := messages[i]
		historyMessage := fmt.Sprintf("[История] %s: %s", msg.Username, msg.Content)
		err := client.conn.WriteMessage(websocket.TextMessage, []byte(historyMessage))

		if err != nil {
			log.Printf("Ошибка при отправке истории клиенту %s: %v\n", client.username, err)
			getOrCreateRoom(client.room).removeClient(client)
			client.conn.Close()
			return
		}
	}
}

// --------------------- Клиент ---------------------

func (c *Client) handleMessages() {
	room := getOrCreateRoom(c.room)

	defer func() {
		room.removeClient(c)
		c.conn.Close()
		log.Printf("Соединение закрыто: %s, комната: %s\n", c.username, c.room)
	}()

	for {
		messageType, p, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("Ошибка при чтении сообщения от %s: %v\n", c.username, err)
			break // Выходим из цикла, соединение будет закрыто
		}

		// Broadcast сообщение
		room.broadcast(messageType, p, c)
	}
}
