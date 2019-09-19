package rabbitmq

import (
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
	Conn          *amqp.Connection
	URI           string
	Queues        map[string]QueueInfo
	ErrorChan     chan *amqp.Error
	closed        bool
	ReconnectChan chan struct{}
	reconnection  bool
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
	reconnectChan := make(chan struct{})
	rmq := &RabbitMQ{URI: uri, Queues: qInfo, ReconnectChan: reconnectChan, reconnection: false}

	rmq.connect(uri)

	//launch a goroutine that will listen for messages on ErrorChan and try to reconnect in case of errors
	go rmq.reconnector()

	return rmq
}

func (rmq *RabbitMQ) connect(uri string) {

	fmt.Println("Connecting to RabbitMQ...")

	for {

		conn, err := amqp.Dial(uri)

		if err == nil {

			//crete the error chan
			rmq.ErrorChan = make(chan *amqp.Error)

			rmq.Conn = conn

			//notify all close signals on ErrorChan so that a reconnect can be retried
			rmq.Conn.NotifyClose(rmq.ErrorChan)

			fmt.Println("Connection successful.")

			//set reconnection to true so all successive reconnections are not considered as the first connection
			if !rmq.reconnection {
				rmq.reconnection = true
				return
			}

			//close reconnect chan so that all listeners are notified that a reconnection took place
			close(rmq.ReconnectChan)

			rmq.ReconnectChan = make(chan struct{})
			return
		}

		log.Printf("Failed to connect to %s %v!\nRetrying in 5s...", uri, err)
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

//Publish publishes a message to a queue
func (rmq *RabbitMQ) Publish(queue string, body string) error {

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

	//publish a message to test connection queue
	err = rmq.Publish(queue, "Ping")

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
