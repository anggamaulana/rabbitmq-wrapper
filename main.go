package main

import (
	"fmt"
	"os"
	"os/signal"
	rabbitmq "rabbitmq-wrapper/rabbitmq"
	"time"
	// rabbitmq "rabbitmq-wrapper/rabbitmq_streadway" // use this for legacy code made in streadway/amqp
)

func main() {

	host_url := "amqp://guest:guest@localhost:5672/"
	rabbit := rabbitmq.NewRabbitMq(host_url, 5)
	defer rabbit.GracefulShutdown()

	go rabbit.Consume("my_queue1", func(body []byte, dc rabbitmq.DeliveryChannelWrapper) {
		// body contains message body
		fmt.Println(string(body))
		for i := 0; i < 10; i++ {
			fmt.Println("long task ...", i, " you can press ctrl+c to emulate graceful shutdown")
			time.Sleep(5 * time.Second)
		}
		dc.Ack(false)
	})

	go rabbit.Consume("my_queue2", func(body []byte, dc rabbitmq.DeliveryChannelWrapper) {
		// body contains message body
		fmt.Println(string(body))
		dc.Ack(false)
	})

	// ==================================================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	fmt.Println("Quitting...")
}
