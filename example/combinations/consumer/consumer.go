package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"

	rabbitmq "github.com/anggamaulana/rabbitmq-wrapper/rabbitmq"
	// rabbitmq "github.com/anggamaulana/rabbitmq-wrapper/rabbitmq_streadway" // use this for legacy code made in streadway/amqp
)

func main() {

	host_url := "amqp://guest:guest@localhost:5672/"
	rabbit := rabbitmq.NewRabbitMq(host_url, 5, 30)
	defer rabbit.GracefulShutdown()

	for j := 0; j < 3; j++ {
		q_name := fmt.Sprintf("my_queue%d", j)
		go rabbit.Consume(q_name, func(ctx context.Context, body []byte, dc rabbitmq.DeliveryChannelWrapper) {
			// body contains message body
			fmt.Println(string(body))

			b := strings.Split(string(body), ":")

			q_return := b[0]
			payload := []byte(b[1])

			rabbit.RegisterPublisherIfNotExists(q_return)
			rabbit.PublishJson(context.Background(), q_return, payload, "", "")

			dc.Ack(false)
		})

	}

	// ==================================================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	fmt.Println("Quitting...")
}
