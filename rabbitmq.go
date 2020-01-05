package rabbitmq

import (
	"context"
	"fmt"

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

//QueueInfo represents the queue info
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

	return rmq
}

//Consumer is a func type that can be used to process a Delivery
type Consumer func(d amqp.Delivery) error

//Channel creates and returns a new channel
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

//Consume from a queue
func (rmq *RabbitMQ) Consume(ctx context.Context, qInfo QueueInfo, prefetch int, consumer Consumer) error {

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

//Publish publishes a message to a queue
func (rmq *RabbitMQ) Publish(qInfo QueueInfo, body string, headersTable amqp.Table) error {

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

//PublishRPC publishes a message using the rpc pattern, and blocks for the response until the context expires
func (rmq *RabbitMQ) PublishRPC(ctx context.Context, publishTo QueueInfo, body string, headersTable amqp.Table, replyTo QueueInfo, correlationID string) (amqp.Delivery, error) {

	var response amqp.Delivery

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
		return response, err
	}

	for {
		//wait for a response having the same correlation ID, until the timeout exceeds
		select {
		case response = <-replyToMessages:
			if response.CorrelationId == correlationID {
				response.Ack(false)
				return response, nil
			}
		case <-ctx.Done():

			return response, fmt.Errorf("Context expired: %s", ctx.Err())
		}
	}

}
