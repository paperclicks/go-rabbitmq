package rabbitmq

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
	Queues               map[string]QueueInfo
	ErrorChan            chan *amqp.Error
	closed               bool
	ConnectionContext    context.Context
	reconnected          bool
	connectionCancelFunc context.CancelFunc
}

type QueueInfo struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

//New creates a new instance of RabbitMQ
func New(uri string, qInfo map[string]QueueInfo) *RabbitMQ {

	ctx, cancel := context.WithCancel(context.Background())
	rmq := &RabbitMQ{URI: uri, Queues: qInfo, ConnectionContext: ctx, connectionCancelFunc: cancel, reconnected: false}

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

func (rmq *RabbitMQ) Close() {

	log.Println("RabbitMQ closing connection")
	rmq.closed = true
	rmq.Conn.Close()
}

//Consumer is a func type that can be used to process a Delivery
type Consumer func(d amqp.Delivery) error

//Publish publishes a message to a queue without trying to assert the queue
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
func (rmq *RabbitMQ) PublishRPC(queue string, body string, replyTo string, correlationID string) error {

	//create ch and declare its topology
	ch, err := rmq.Channel(1, 0, false)

	if err != nil {
		return err

	}
	defer ch.Close()

	//declare the queue
	q, err := ch.QueueDeclare(
		rmq.Queues[queue].Name,       // name
		rmq.Queues[queue].Durable,    // durable
		rmq.Queues[queue].AutoDelete, // delete when unused
		rmq.Queues[queue].Exclusive,  // exclusive
		rmq.Queues[queue].NoWait,     // no-wait
		rmq.Queues[queue].Args,       // arguments
	)
	if err != nil {
		return err
	}

	//publish
	headersTable := make(amqp.Table)

	headersTable["json"] = true

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
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
func (rmq *RabbitMQ) Status(queue string) (string, error) {

	//create ch and declare its topology
	ch, err := rmq.Channel(1, 0, false)

	if err != nil {
		return "ERROR", err
	}
	defer ch.Close()

	qInfo := QueueInfo{}
	qInfo.Name=queue

	//publish a message to test connection queue
	err = rmq.Publish(qInfo, "Ping",amqp.Table{})

	if err != nil {
		return "ERROR", err
	}

	//register a consumer to test connection queue
	testConsumerCH, err := ch.Consume(
		rmq.Queues[queue].Name, // queue
		"test-consumer",        // consumer
		false,                  // auto-ack
		false,                  // exclusive
		false,                  // no-local
		false,                  // no-wait
		nil,                    // args
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

func (rmq *RabbitMQ) Consume(ch *amqp.Channel, queue string, name string) (<-chan amqp.Delivery, error) {

	var msgs <-chan amqp.Delivery

	//declare the queue to avoid NOT FOUND errors
	_, err := ch.QueueDeclare(
		rmq.Queues[queue].Name,       // name
		rmq.Queues[queue].Durable,    // durable
		rmq.Queues[queue].AutoDelete, // delete when unused
		rmq.Queues[queue].Exclusive,  // exclusive
		rmq.Queues[queue].NoWait,     // no-wait
		rmq.Queues[queue].Args,       // arguments
	)
	if err != nil {
		return msgs, err
	}

	//initialize consumer
	msgs, err = ch.Consume(
		rmq.Queues[queue].Name, // queue
		name,                   // consumer
		false,                  // auto-ack
		false,                  // exclusive
		false,                  // no-local
		false,                  // no-wait
		nil,                    // args
	)
	if err != nil {
		return msgs, fmt.Errorf("Failed to register %s: %v", name, err)
	}

	return msgs, nil
}

//ConsumeOne consumes 1 message at a time. No new goroutine is launched
func (rmq *RabbitMQ) ConsumeOne(ctx context.Context, qInfo QueueInfo, consumer Consumer) error {

	var msgs <-chan amqp.Delivery

	//create ch and declare its topology
	ch, err := rmq.Channel(1, 0, false)

	if err != nil {

		return err

	}

	//declare the queue to avoid NOT FOUND errors
	_, err = ch.QueueDeclare(
		qInfo.Name,       // name
		qInfo.Durable,    // durable
		qInfo.AutoDelete, // delete when unused
		qInfo.Exclusive,  // exclusive
		qInfo.NoWait,     // no-wait
		qInfo.Args,       // arguments
	)
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

			consumer(d)

		case <-ctx.Done():
			return ctx.Err()
		}

	}

	return nil
}

//Consume2
func (rmq *RabbitMQ) Consume2(ctx context.Context, qInfo QueueInfo, prefetch int, consumer Consumer) error {

	var msgs <-chan amqp.Delivery

	//create ch and declare its topology
	ch, err := rmq.Channel(prefetch, 0, false)

	if err != nil {

		return err

	}

	//declare the queue to avoid NOT FOUND errors
	_, err = ch.QueueDeclare(
		qInfo.Name,       // name
		qInfo.Durable,    // durable
		qInfo.AutoDelete, // delete when unused
		qInfo.Exclusive,  // exclusive
		qInfo.NoWait,     // no-wait
		qInfo.Args,       // arguments
	)
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

//ConsumeAutoack consumes and auto acks the delivery
func (rmq *RabbitMQ) ConsumeAutoack(ctx context.Context, qInfo QueueInfo, prefetch int, consumer Consumer) error {

	var msgs <-chan amqp.Delivery

	//create ch and declare its topology
	ch, err := rmq.Channel(prefetch, 0, false)

	if err != nil {

		return err

	}

	//declare the queue to avoid NOT FOUND errors
	_, err = ch.QueueDeclare(
		qInfo.Name,       // name
		qInfo.Durable,    // durable
		qInfo.AutoDelete, // delete when unused
		qInfo.Exclusive,  // exclusive
		qInfo.NoWait,     // no-wait
		qInfo.Args,       // arguments
	)
	if err != nil {
		return err
	}

	//initialize consumer
	msgs, err = ch.Consume(
		qInfo.Name, // queue
		"",         // consumer
		true,       // auto-ack
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

//PublishAssert publishes a message to a queue, trying to also asset the queue before publishing
func (rmq *RabbitMQ) PublishAssert(qInfo QueueInfo, body string, headersTable amqp.Table) error {

	//create ch and declare its topology
	ch, err := rmq.Channel(1, 0, false)

	if err != nil {
		return err

	}
	defer ch.Close()

	//declare the queue to avoid NOT FOUND errors
	_, err = ch.QueueDeclare(
		qInfo.Name,       // name
		qInfo.Durable,    // durable
		qInfo.AutoDelete, // delete when unused
		qInfo.Exclusive,  // exclusive
		qInfo.NoWait,     // no-wait
		qInfo.Args,       // arguments
	)
	if err != nil {
		return err
	}

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

//PublishRPC2 publishes a message using the rpc pattern, and blocks for the response until the context expires
func (rmq *RabbitMQ) PublishRPC2(publishTo QueueInfo, body string, headersTable amqp.Table, replyTo QueueInfo, correlationID string, timeout time.Duration) (amqp.Delivery, error) {

	var response amqp.Delivery
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	//open a channel
	ch, err := rmq.Channel(1, 0, false)
	if err != nil {
		return response, err
	}
	defer ch.Close()

	//declare the replyTo queue and wait for messages (start consuming)
	replyToQueue, err := ch.QueueDeclare(
		replyTo.Name,       // name
		replyTo.Durable,    // durable
		replyTo.AutoDelete, // delete when unused
		replyTo.Exclusive,  // exclusive
		replyTo.NoWait,     // no-wait
		replyTo.Args,       // arguments
	)
	if err != nil {
		return response, err
	}

	replyToMessages, err := ch.Consume(
		replyToQueue.Name, // queue
		correlationID,     // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)

	if err != nil {
		return response, err
	}

	//declare the queue where the message will be published, and publish the message
	_, err = ch.QueueDeclare(
		publishTo.Name,       // name
		publishTo.Durable,    // durable
		publishTo.AutoDelete, // delete when unused
		publishTo.Exclusive,  // exclusive
		publishTo.NoWait,     // no-wait
		publishTo.Args,       // arguments
	)
	if err != nil {
		return response, err
	}

	err = ch.Publish(
		"",             // exchange
		publishTo.Name, // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ReplyTo:       replyToQueue.Name,
			CorrelationId: correlationID,
			Headers:       headersTable,
			ContentType:   "text/plain",
			Body:          []byte(body),
		})

	if err != nil {
		ch.Close()
		return response, err
	}

	for {
		//wait for a response having the same correlation ID, until the timeout exceeds
		select {
		case response = <-replyToMessages:
			if response.CorrelationId == correlationID {
				response.Ack(false)
				ch.Close()
				return response, nil
			}
			response.Nack(false, true)
		case <-ctx.Done():
			ch.Close()
			return response, fmt.Errorf("Context expired: %s", ctx.Err())
		}
	}

}
