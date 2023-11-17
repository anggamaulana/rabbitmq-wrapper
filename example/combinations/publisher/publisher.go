package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	rabbitmq "github.com/anggamaulana/rabbitmq-wrapper/rabbitmq"
	// rabbitmq "github.com/anggamaulana/rabbitmq-wrapper/rabbitmq_streadway" // use this for legacy code made in streadway/amqp
)

func main() {

	host_url := "amqp://guest:guest@localhost:5672/"
	rabbit := rabbitmq.NewRabbitMq(host_url, 5, 30)
	defer rabbit.GracefulShutdown()

	for j := 0; j < 3; j++ {
		q_name := fmt.Sprintf("my_queue%d", j)
		rabbit.RegisterPublisherIfNotExists(q_name)
	}

	for j := 0; j < 3; j++ {
		q_return := fmt.Sprintf("q_return%d", j)

		go rabbit.Consume(q_return, func(ctx context.Context, body []byte, dc rabbitmq.DeliveryChannelWrapper) {
			// body contains message body
			fmt.Println(q_return, ":", string(body))

			dc.Ack(false)
		})

	}

	time.Sleep(5 * time.Second)

	for j := 0; j < 3; j++ {
		q_name := fmt.Sprintf("my_queue%d", j)
		q_return := fmt.Sprintf("q_return%d", j)

		go func() {
			for i := 0; i < 1000; i++ {
				// time.Sleep(1 * time.Second)
				fmt.Println("publish to ", q_name)
				payload := []byte(fmt.Sprintf("%s:test_data%d", q_return, i))
				rabbit.PublishJson(context.Background(), q_name, payload, "", "")
			}
		}()
	}

	// ==================================================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	fmt.Println("Quitting...")
}
