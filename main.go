package main

import (
	"fmt"
	"os"
	"os/signal"
	"rabbitmq-wrapper/rabbitmq"
)

func main() {

	host_url := "amqp://guest:guest@localhost:5672/"
	rabbit := rabbitmq.NewRabbitMq(host_url, 5)
	defer rabbit.GracefulShutdown()

	go rabbit.Consume("my_queue1", func(body []byte, dc rabbitmq.DeliveryChannelWrapper) {
		// body contains message body
		dc.Ack(false)
	})

	go rabbit.Consume("my_queue2", func(body []byte, dc rabbitmq.DeliveryChannelWrapper) {
		// body contains message body
		dc.Ack(false)
	})

	// ==================================================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	fmt.Println("Quitting...")
}
