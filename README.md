# rabbitmq-wrapper
My simple wrapper for official rabbitmq library (github.com/rabbitmq/amqp091-go)


author : Angga Maulana

RabbitMq wrapper, support  :
- one connection and multiple channel
- reconnecting
- combination of consumer and publisher in one place
- use github.com/rabbitmq/amqp091-go, the official Go client maintained by the RabbitMQ team.
  streadway/amqp is not actively maintained



Example Consumer:

	rabbit := rb.NewRabbitMq(uri_string, 5)
	defer rabbit.GracefulShutdown()

	go rabbit.Consume("my_queue1", func(body []byte, dc rb.DeliveryChannelWrapper){
		// body contains message body
		dc.Ack(false)
	})

	go rabbit.Consume("my_queue2", func(body []byte, dc rb.DeliveryChannelWrapper){
		// body contains message body
		dc.Ack(false)
	})

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


