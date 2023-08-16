# rabbitmq-wrapper
My simple wrapper for official rabbitmq library


author : Angga Maulana

RabbitMq wrapper, support  :
- one connection and multiple channel
- reconnecting
- combination of consumer and publisher in one place
- support github.com/rabbitmq/amqp091-go, the official Go client maintained by the RabbitMQ team 
- support streadway/amqp for legacy code, [deprecation warning]
- graceful shutdown, wait all worker to finish their work before shutdown



Example Consumer:

	package main

	import (
		"context"
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

		go rabbit.Consume("my_queue1", func(ctx context.Context, body []byte, dc rabbitmq.DeliveryChannelWrapper) {
			// body contains message body
			fmt.Println(string(body))
			for i := 0; i < 10; i++ {
				fmt.Println("long task1 ...", i, " you can press ctrl+c to emulate graceful shutdown")
				time.Sleep(5 * time.Second)

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
				time.Sleep(5 * time.Second)

				if ctx.Err() != nil {
					// receive kill signal from os
					fmt.Println("task2 aborted")
					return
				}

			}
			dc.Ack(false)
		})

		// ==================================================================
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt)
		<-quit
		fmt.Println("Quitting...")
	}


Example Publisher :

	rabbit := rb.NewRabbitMq(uri_string, 5)
	defer rabbit.GracefulShutdown()

	rabbit.RegisterPublisher("my_queue1")
	rabbit.RegisterPublisher("my_queue2")

	rabbit.PublishJson(ctx, "my_queue1", body, message_id, correlation_id)

Example Combination of Consumer and publisher:

	rabbit := rb.NewRabbitMq(uri_string, 5)
	defer rabbit.GracefulShutdown()

	rabbit.Consume("my_queue1", func(body []byte, dc rb.DeliveryChannelWrapper){
		// body contains message body
		dc.Ack(false)
	})

	rabbit.RegisterPublisher("my_queue2")

	rabbit.PublishJson(ctx, "my_queue1", body, message_id, correlation_id)


