package rabbitmq_streadway

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	amqp "github.com/streadway/amqp"
)

/*
author : Angga Maulana
=====================================================
RabbitMq wrapper, support  :
- one connection and multiple channel
- reconnecting
- combination of consumer and publisher in one place
- use github.com/rabbitmq/amqp091-go, the official Go client maintained by the RabbitMQ team or
  streadway/amqp (legacy)
=====================================================
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


=====================================================

*/

type RabbitChannel struct {
	TypeChannel     string // consumer or publisher
	RabbitChannel   *amqp.Channel
	DeliveryChannel <-chan amqp.Delivery
	RabbitQueue     *amqp.Queue
}

const (
	MAXIMUM_CHANNEL = 30
)

type RabbitMq struct {
	sync.Mutex
	host                        string
	ReconnectDelaySeconds       int
	ReconnectWorkerDelaySeconds int
	Conn                        *amqp.Connection
	Channel_registered          map[string]RabbitChannel
	requestReconnect            int
	ExitSignal                  chan bool
	ReconnectingSignal          chan bool
}

type DeliveryChannelWrapper interface {
	Ack(multiple bool) error
}

type CallbackConsumer func(body []byte, dc DeliveryChannelWrapper)

func NewRabbitMq(host_string string, reconnect_delay_seconds int) *RabbitMq {

	rabbit := &RabbitMq{
		host:                        host_string,
		ReconnectDelaySeconds:       reconnect_delay_seconds,
		ReconnectWorkerDelaySeconds: 5,
		Channel_registered:          make(map[string]RabbitChannel),
		ExitSignal:                  make(chan bool, MAXIMUM_CHANNEL),
		ReconnectingSignal:          make(chan bool, MAXIMUM_CHANNEL),
	}

	rabbit.AttempConnect()

	// run reconnect worker in background
	go rabbit.ReconnectWorker()

	return rabbit

}

func (c *RabbitMq) AttempConnect() {
	for {
		err := c.Connect()
		if err == nil {
			break
		} else {
			log.Error().Msg(err.Error())
			log.Info().Msg("RabbitMQ : waiting next retry to reconnect...")
			time.Sleep(time.Duration(c.ReconnectDelaySeconds) * time.Second)
		}
	}
}

func (c *RabbitMq) Connect() error {
	conn, err := amqp.Dial(c.host)
	if err == nil {
		c.Conn = conn
	}

	return err
}

func (c *RabbitMq) Consume(queue_name string, callback CallbackConsumer) {

	c.RegisterConsumer(queue_name)

	for {

		ch := c.GetChannelByName(queue_name)

		for d := range ch.DeliveryChannel {
			callback(d.Body, d)
		}

		c.ExitSignal <- true

		time.Sleep(time.Duration(5) * time.Second)

	}
}

func (c *RabbitMq) RegisterConsumer(name string) error {

	ch, err := c.Conn.Channel()
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	c.Lock()

	c.Channel_registered[name] = RabbitChannel{
		RabbitChannel:   ch,
		DeliveryChannel: msgs,
		RabbitQueue:     &q,
		TypeChannel:     "consumer",
	}

	c.Unlock()

	log.Info().Msg("RabbitMQ : CONSUMER " + name + " is Created.")

	return err

}

func (c *RabbitMq) RegisterPublisher(name string) error {

	ch, err := c.Conn.Channel()
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	c.Lock()

	c.Channel_registered[name] = RabbitChannel{
		RabbitChannel:   ch,
		DeliveryChannel: nil,
		RabbitQueue:     &q,
		TypeChannel:     "publisher",
	}

	c.Unlock()

	log.Info().Msg("RabbitMQ : PUBLISHER " + name + " is Created.")

	return err

}

func (c *RabbitMq) PublishJson(ctx context.Context, channel_name string, body []byte, message_id string, correlation_id string) error {

	c.Lock()
	reconnectReqs := c.requestReconnect
	c.Unlock()

	// ignore publishing while reconnecting to broker
	if reconnectReqs > 0 {
		return nil
	}

	c.Lock()
	ch := c.Channel_registered[channel_name]
	c.Unlock()

	payload := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         body,
	}

	if message_id != "" {
		payload.MessageId = message_id
	}

	if correlation_id != "" {
		payload.CorrelationId = correlation_id
	}

	return ch.RabbitChannel.Publish(
		"",           // exchange
		channel_name, // routing key
		false,        // mandatory
		false,
		payload,
	)
}

func (c *RabbitMq) GetChannelByName(name string) RabbitChannel {

	reconnectReqs := c.requestReconnect

	if reconnectReqs == 0 {
		return c.Channel_registered[name]
	} else {
		// wait while for reconnecting
		<-c.ReconnectingSignal
		return c.Channel_registered[name]
	}

}

func (c *RabbitMq) GracefulShutdown() {

	// Stop recieving message from all channel that registered, either "publisher" or "consumer"
	for k := range c.Channel_registered {
		c.Channel_registered[k].RabbitChannel.Cancel("", false)
		fmt.Println("RabbitMQ : Channel ", k, " is cancelled")

	}

	// close all channel that registered, either "publisher" or "consumer"
	for k := range c.Channel_registered {
		c.Channel_registered[k].RabbitChannel.Close()
	}

	// close rabbit connection
	c.Conn.Close()
}

func (c *RabbitMq) scheduleReconnect() {

	// let's assume if one channel request to reconnect then all channel should reconnect because they are using same connection

	c.Lock()
	c.requestReconnect += 1
	c.Unlock()
}

func (c *RabbitMq) notifyReconnectDone() {

	for k := 0; k < len(c.Channel_registered); k++ {
		c.ReconnectingSignal <- true
	}
}

func (c *RabbitMq) ReconnectWorker() {
	fmt.Println("RabbitMQ : STARTING RECONNECT WORKER IN BACKGROUND...")

	for {

		channel_index := 0
		for {
			// wait exit signal from every channel goroutine  that has been registered in "Channel_registered"
			<-c.ExitSignal
			channel_index += 1

			if channel_index >= len(c.Channel_registered) {
				break
			}
		}

		// out of loop, mean channel is closed, begin reconnect
		c.scheduleReconnect()

		// we use 1 connection multiple channel pattern,
		// so  our job is now is reinitialize 1 connection and all channel that has been registered

	Reconnecting:
		log.Info().Msg("RabbitMQ : RECONNECTING AND REINITIALIZING RABBIT MQ...")

		// reconnect rabbit
		c.AttempConnect()

		// reinitialized all rabbitChannel that registered
		for k := range c.Channel_registered {
			if c.Channel_registered[k].TypeChannel == "consumer" {
				err := c.RegisterConsumer(k)
				if err != nil {
					log.Error().Err(err)
					goto Reconnecting
				}
			} else if c.Channel_registered[k].TypeChannel == "publisher" {
				err := c.RegisterPublisher(k)
				if err != nil {
					log.Error().Err(err)
					goto Reconnecting
				}
			}
		}

		c.Lock()
		c.requestReconnect = 0
		c.Unlock()

		c.notifyReconnectDone()

		log.Info().Msg("RabbitMQ : Successfully connected.")
	}
}
