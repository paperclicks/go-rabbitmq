package gorabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

var conn *amqp.Connection
var ch *amqp.Channel

//RabbitMQ is a concrete instance of the package
type RabbitMQ struct {
	Conn                 *amqp.Connection
	URI                  string
	ErrorChan            chan *amqp.Error
	closed               bool
	ConnectionContext    context.Context
	reconnected          bool
	connectionCancelFunc context.CancelFunc
}

//QueueInfo holds the necessary info to describe a queue
type QueueInfo struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

//New creates a new instance of RabbitMQ
func New(uri string) *RabbitMQ {

	ctx, cancel := context.WithCancel(context.Background())
	rmq := &RabbitMQ{URI: uri, ConnectionContext: ctx, connectionCancelFunc: cancel, reconnected: false}

	rmq.connect(uri)

	//launch a goroutine that will listen for messages on ErrorChan and try to reconnect in case of errors
	go rmq.reconnector()

	return rmq
}

func (rmq *RabbitMQ) connect(uri string) {

	log.Printf("Connecting to RabbitMQ...")

	for {

		conn, err := amqp.Dial(uri)

		if err == nil {

			//crete the error chan
			rmq.ErrorChan = make(chan *amqp.Error)

			rmq.Conn = conn

			//notify all close signals on ErrorChan so that a reconnect can be retried
			rmq.Conn.NotifyClose(rmq.ErrorChan)

			log.Printf("Connection successful.")

			//set reconnected to true so all successive reconnections are not considered as the first connection
			if !rmq.reconnected {
				rmq.reconnected = true
				return
			}

			//cancel the connection context to notify any listeners
			rmq.connectionCancelFunc()

			//renew the context after reconnecting
			ctx, cancel := context.WithCancel(context.Background())
			rmq.connectionCancelFunc = cancel
			rmq.ConnectionContext = ctx

			return
		}

		log.Printf("Failed to connect to %s %v! Retrying in 5s...", uri, err)

		time.Sleep(5000 * time.Millisecond)

	}

}

func (rmq *RabbitMQ) reconnector() {
	for {
		err := <-rmq.ErrorChan
		if !rmq.closed {
			log.Printf("Reconnecting after connection closed: %v\n", err)

			rmq.connect(rmq.URI)
		}
	}
}

//Close the connection
func (rmq *RabbitMQ) Close() {

	log.Println("RabbitMQ closing connection")
	rmq.closed = true
	rmq.Conn.Close()
}

//Channel opens and returns a channel
func (rmq *RabbitMQ) Channel(prefetch int, prefSize int, global bool) (*amqp.Channel, error) {

	//create ch and declare its topology
	ch, err := rmq.Conn.Channel()

	if err != nil {

		return ch, fmt.Errorf("Failed to open a channel %v", err)

	}

	err = ch.Qos(
		prefetch, // prefetch count
		prefSize, // prefetch size
		global,   // global
	)
	if err != nil {

		return ch, err
	}
	return ch, nil
}

//Status checks the connection status of RabbitMQ by publishing and then receiving a message from a test queue
func (rmq *RabbitMQ) Status(qInfo QueueInfo) (string, error) {

	//create ch and declare its topology
	ch, err := rmq.Channel(1, 0, false)

	if err != nil {
		return "ERROR", err
	}
	defer ch.Close()

	//publish
	headersTable := make(amqp.Table)

	headersTable["json"] = true

	//publish a message to test connection queue
	err = rmq.Publish(qInfo, "Ping", headersTable)

	if err != nil {
		return "ERROR", err
	}

	//register a consumer to test connection queue
	testConsumerCH, err := ch.Consume(
		qInfo.Name,      // queue
		"test-consumer", // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		return "ERROR", err
	}

	//Use a select with timout 10s on the channel to check for messages.
	//if after 10s no messages have been received, return with an error.
	select {
	case d := <-testConsumerCH:

		d.Ack(true)
		return "OK", nil

	case <-time.After(60 * time.Second):

		return "ERROR", errors.New("Rabbit status check timed out after 60 seconds")
	}

}

//Consume opens a channel, declares a queue using qInfo and starts consuming messages with the given prefetch. Messages are processesd by the Consumer which is launched in a new goroutime.
func (rmq *RabbitMQ) Consume(ctx context.Context, qInfo QueueInfo, prefetch int, consumer func(d amqp.Delivery) error) error {

	var msgs <-chan amqp.Delivery

	//create ch and declare its topology
	ch, err := rmq.Channel(prefetch, 0, false)

	if err != nil {

		return err

	}

	//initialize consumer
	msgs, err = ch.Consume(
		qInfo.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	//wait for messages and
	for d := range msgs {

		select {
		default:

			go consumer(d)

		case <-ctx.Done():
			return ctx.Err()
		}

	}

	return nil
}

//Publish publishes a message to a queue
func (rmq *RabbitMQ) Publish(qInfo QueueInfo, body string, headersTable amqp.Table) error {

	//create ch and declare its topology
	ch, err := rmq.Channel(1, 0, false)

	if err != nil {
		return err

	}
	defer ch.Close()

	err = ch.Publish(
		"",         // exchange
		qInfo.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:     headersTable,
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	if err != nil {
		return err
	}

	return nil
}

//PublishRPC publishes a message to a queue using rpc pattern
func (rmq *RabbitMQ) PublishRPC(qInfo QueueInfo, body string, headersTable amqp.Table, replyTo string, correlationID string) error {

	//create ch and declare its topology
	ch, err := rmq.Channel(1, 0, false)

	if err != nil {
		return err

	}
	defer ch.Close()

	err = ch.Publish(
		"",         // exchange
		qInfo.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ReplyTo:       replyTo,
			CorrelationId: correlationID,
			Headers:       headersTable,
			ContentType:   "text/plain",
			Body:          []byte(body),
		})

	if err != nil {
		return err
	}

	return nil
}
