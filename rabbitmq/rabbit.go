package rabbitmq

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

/*
author : Angga Maulana (https://github.com/anggamaulana)
=====================================================
RabbitMq wrapper, support  :
- one connection and multiple channel
- reconnecting
- combination of consumer and publisher in one place
- use github.com/rabbitmq/amqp091-go, the official Go client maintained by the RabbitMQ team or
  streadway/amqp (legacy)
- graceful shutdown, wait all worker to finish their work before shutdown
=====================================================
Example Consumer:

	rabbit := rb.NewRabbitMq(uri_string, 5)
	defer rabbit.GracefulShutdown()

	go rabbit.Consume("my_queue1", func(ctx context.Context, body []byte, dc rb.DeliveryChannelWrapper){
		// body contains message body
		dc.Ack(false)
	})

	go rabbit.Consume("my_queue2", func(ctx context.Context,body []byte, dc rb.DeliveryChannelWrapper){
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

	rabbit.Consume("my_queue1", func(ctx context.Context,body []byte, dc rb.DeliveryChannelWrapper){
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

type RabbitMq struct {
	sync.Mutex
	host                        string
	ReconnectDelaySeconds       int
	ReconnectWorkerDelaySeconds int
	Conn                        *amqp.Connection
	Channel_registered          map[string]RabbitChannel
	requestReconnect            int
	ReconnectingSignal          chan bool
	SystemExitSignal            chan bool
	SystemExitCommand           bool
	StopAllWorks                context.CancelFunc
	Context                     context.Context
}

type DeliveryChannelWrapper interface {
	Ack(multiple bool) error
}

type CallbackConsumer func(context context.Context, body []byte, dc DeliveryChannelWrapper)

func NewRabbitMq(host_string string, reconnect_delay_seconds int, maximum_channel int) *RabbitMq {

	ctx := context.Background()

	ctx, stop := context.WithCancel(ctx)

	rabbit := &RabbitMq{
		host:                        host_string,
		ReconnectDelaySeconds:       reconnect_delay_seconds,
		ReconnectWorkerDelaySeconds: 5,
		Channel_registered:          make(map[string]RabbitChannel),
		ReconnectingSignal:          make(chan bool, maximum_channel),
		SystemExitSignal:            make(chan bool, maximum_channel),
		Context:                     ctx,
		StopAllWorks:                stop,
	}

	rabbit.AttempConnect()

	return rabbit

}

func (c *RabbitMq) AttempConnect() {
	for {
		err := c.Connect()
		if err == nil {
			break
		} else {

			c.Lock()
			exit_cmd := c.SystemExitCommand
			c.Unlock()
			if exit_cmd {
				return
			}

			log.Error().Msg(err.Error())
			log.Info().Msg("RabbitMQ : waiting next retry to reconnect...")
			time.Sleep(time.Duration(c.ReconnectDelaySeconds) * time.Second)
		}
	}
}

func (c *RabbitMq) Connect() error {
	conn, err := amqp.Dial(c.host)
	if err == nil {
		c.Lock()
		c.Conn = conn
		c.Unlock()

		go c.ReconnectWorker()
	}

	return err
}

func (c *RabbitMq) Consume(queue_name string, callback CallbackConsumer) {

	for {
		err := c.RegisterConsumer(queue_name)

		if err != nil {
			log.Error().Msg("RabbitMQ : error open new channel " + queue_name + " : " + err.Error() + " retry in 5 seconds...")
		}

		ch := c.GetChannelByName(queue_name)

		for d := range ch.DeliveryChannel {
			callback(c.Context, d.Body, d)
		}

		// this line is crucial, we wait if "SystemExitCommand" became true
		time.Sleep(time.Duration(2) * time.Second)

		c.Lock()
		exit_cmd := c.SystemExitCommand
		c.Unlock()

		if exit_cmd {
			c.SystemExitSignal <- true
			return
		}

		time.Sleep(time.Duration(5) * time.Second)

	}
}

func (c *RabbitMq) RegisterConsumer(name string) error {

	c.Lock()
	reconnectReqs := c.requestReconnect
	c.Unlock()

	if reconnectReqs != 0 {
		// wait while for reconnecting
		<-c.ReconnectingSignal
	}

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
		q.Name,             // queue
		q.Name+"_consumer", // consumer
		false,              // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
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

	return ch.RabbitChannel.PublishWithContext(ctx,
		"",           // exchange
		channel_name, // routing key
		false,        // mandatory
		false,
		payload,
	)
}

func (c *RabbitMq) GetChannelByName(name string) RabbitChannel {

	c.Lock()
	channel := c.Channel_registered[name]
	c.Unlock()
	return channel

}

func (c *RabbitMq) GetConsumerCount() int {
	consumer := 0
	c.Lock()
	for _, v := range c.Channel_registered {
		if v.TypeChannel == "consumer" {
			consumer += 1
		}
	}
	c.Unlock()
	return consumer
}

func (c *RabbitMq) GracefulShutdown() {

	// Stop recieving message from all channel that registered, either "publisher" or "consumer"
	for k := range c.Channel_registered {
		c.Channel_registered[k].RabbitChannel.Cancel(k+"_consumer", false)
		log.Info().Msg("RabbitMQ : Channel " + k + " is cancelled")
	}

	c.Lock()
	c.SystemExitCommand = true
	c.Unlock()

	c.StopAllWorks()

	channel_index := 0
	log.Info().Msg("RabbitMQ : wait for all worker finish their work...")
	for {
		// wait exit signal from every channel goroutine  that has been registered in "Channel_registered"
		<-c.SystemExitSignal
		channel_index += 1

		if channel_index >= c.GetConsumerCount() {
			break
		}
	}

	// close all channel that registered, either "publisher" or "consumer"
	for k := range c.Channel_registered {
		c.Channel_registered[k].RabbitChannel.Close()
	}

	// close rabbit connection
	c.Conn.Close()
	log.Info().Msg("RabbitMQ : rabbit is closed")
}

func (c *RabbitMq) scheduleReconnect() {

	// let's assume if one channel request to reconnect then all channel should reconnect because they are using same connection

	c.Lock()
	c.requestReconnect += 1
	c.Unlock()
}

func (c *RabbitMq) notifyReconnectDone() {

	consumer := c.GetConsumerCount()

	for k := 0; k < consumer; k++ {
		c.ReconnectingSignal <- true
	}
}

func (c *RabbitMq) ReconnectWorker() {
	log.Info().Msg("RabbitMQ : STARTING RECONNECT WORKER IN BACKGROUND...")

	c.Lock()
	conn := c.Conn
	c.Unlock()
	<-conn.NotifyClose(make(chan *amqp.Error))

	c.Lock()
	exit_cmd := c.SystemExitCommand
	c.Unlock()

	if exit_cmd {
		log.Info().Msg("RabbitMQ : Reconnect worker terminated.")
		c.notifyReconnectDone()
		return
	}

	log.Info().Msg("RabbitMQ : RECONNECTING AND REINITIALIZING RABBIT MQ...")
	c.scheduleReconnect()

Reconnecting:
	// reconnect rabbit
	c.AttempConnect()

	c.Lock()
	exit_cmd = c.SystemExitCommand
	c.Unlock()

	if exit_cmd {
		log.Info().Msg("RabbitMQ : Reconnect worker terminated.")
		c.notifyReconnectDone()
		return
	}

	// reinitialized all publisher that registered
	var tmp []string
	c.Lock()
	for k := range c.Channel_registered {
		if c.Channel_registered[k].TypeChannel == "publisher" {
			tmp = append(tmp, k)
		}
	}
	c.Unlock()

	for _, v := range tmp {
		err := c.RegisterPublisher(v)
		if err != nil {
			log.Error().Err(err)
			goto Reconnecting
		}
	}

	c.Lock()
	c.requestReconnect = 0
	c.Unlock()

	c.notifyReconnectDone()

	log.Info().Msg("RabbitMQ : Successfully connected.")

}
