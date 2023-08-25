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

	go rabbit.Consume("my_queue1", func(ctx context.Context, body []byte, dc rabbitmq.DeliveryChannelWrapper) {
		// body contains message body
		fmt.Println(string(body))
		for i := 0; i < 20; i++ {
			fmt.Println("long task1 ...", i, " you can press ctrl+c to emulate graceful shutdown")
			time.Sleep(7 * time.Second)

			if ctx.Err() != nil {
				// receive kill signal from os
				fmt.Println("task1 aborted")
				return
			}

		}
		dc.Ack(false)
	})

	go rabbit.Consume("my_queue2", func(ctx context.Context, body []byte, dc rabbitmq.DeliveryChannelWrapper) {
		// body contains message body
		fmt.Println(string(body))
		for i := 0; i < 10; i++ {
			fmt.Println("long task2 ...", i, " you can press ctrl+c to emulate graceful shutdown")
			time.Sleep(1 * time.Second)

			if ctx.Err() != nil {
				// receive kill signal from os
				fmt.Println("task2 aborted")
				return
			}

		}
		dc.Ack(false)
	})

	rabbit.RegisterPublisher("my_queue3")

	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Second)
			fmt.Println("publish to my_queue3")
			rabbit.PublishJson(context.Background(), "my_queue3", []byte("coba"), "", "")
		}
	}()

	// ==================================================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	fmt.Println("Quitting...")
}
