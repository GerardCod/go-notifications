package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// Connection variables
const (
	DB_USER     = "taller"
	DB_PASSWORD = "Qwerty1234"
	DB_NAME     = "taller"
	DB_SSL      = "disable"
)

func waitForNotification(listener *pq.Listener) {
	for {
		select {
		case n := <-listener.Notify:
			var j bytes.Buffer
			fmt.Printf("Datos recibidos del canal llamado %s\n", n.Channel)

			if err := json.Indent(&j, []byte(n.Extra), "", "\t"); err != nil {
				fmt.Printf("Error al procesar el json: %v\n", err)
				return
			}

			fmt.Println(j.String())

		case <-time.After(60 * time.Second):
			fmt.Println("No se ha recibido información, revisando conexión")

			go func() {
				listener.Ping()
			}()

			return
		}
	}
}

func main() {
	conn := fmt.Sprintf("dbname=%s user=%s password=%s sslmode=%s", DB_NAME, DB_USER, DB_PASSWORD, DB_SSL)

	if _, err := sql.Open("postgres", conn); err != nil {
		panic(err)
	}

	report := func(et pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err)
		}
	}

	listener := pq.NewListener(conn, 10*time.Second, time.Minute, report)
	if err := listener.Listen("channel"); err != nil {
		panic(err)
	}

	fmt.Println("Inicia el proceso de monitoreo")
	for {
		waitForNotification(listener)
	}
}
