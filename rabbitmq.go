package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

var conn *amqp.Connection
var ch *amqp.Channel
var rpcMap map[string]chan amqp.Delivery
var rpcMapMutex sync.RWMutex

// RabbitMQ is a concrete instance of the package
type RabbitMQ struct {
	Conn                   *amqp.Connection
	URI                    string
	ConnectionError        chan *amqp.Error
	SignalConnectionClosed chan struct{}
	RPCReconnect           chan struct{}
	ConnectionContext      context.Context
	UUID                   string
}

type QueueInfo struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// New creates a new instance of RabbitMQ
func New(uri string) (*RabbitMQ, error) {

	conn, err := amqp.Dial(uri)

	if err != nil {
		return &RabbitMQ{}, err
	}

	rmq := &RabbitMQ{URI: uri, Conn: conn}

	rmq.SignalConnectionClosed = make(chan struct{})

	rmq.ConnectionError = make(chan *amqp.Error)
	//notify all close signals on ErrorChan so that a reconnect can be retried
	rmq.Conn.NotifyClose(rmq.ConnectionError)

	//init the uuid of this instance
	uuid := uuid.New()

	if err != nil {
		return &RabbitMQ{}, err
	}
	rmq.UUID = fmt.Sprintf("%s", uuid)

	return rmq, nil
}

func (rmq *RabbitMQ) Reconnect() error {

	log.Printf("reconnecting %s ...\n", rmq.UUID)
	conn, err := amqp.Dial(rmq.URI)

	if err != nil {
		log.Printf("%s\n", err.Error())

		return err
	}

	rmq.Conn = conn

	//rebuild the signal channel because it has been closed
	rmq.SignalConnectionClosed = make(chan struct{})

	//tell the StartRPC goroutine that can try reconnection
	rmq.RPCReconnect <- struct{}{}

	log.Printf("reconnection successful %s/n", rmq.UUID)

	return nil
}

func (rmq *RabbitMQ) Close() {

	log.Println("RabbitMQ closing connection")
	rmq.Conn.Close()
}

// Consumer is a func type that can be used to process a Delivery
type Consumer func(d amqp.Delivery) error

// Publish publishes a message to a queue without trying to assert the queue
func (rmq *RabbitMQ) Publish(qInfo QueueInfo, publishing amqp.Publishing) error {

	select {
	case <-rmq.SignalConnectionClosed:
		return fmt.Errorf("connection is closed cannot publis")
	default:
		if rmq.Conn.IsClosed() {
			close(rmq.SignalConnectionClosed)
		}
	}

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
		publishing)

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

// ConsumeOne consumes 1 message at a time. No new goroutine is launched
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

// Consume consumes messages in parallel by launching a new goroutine for every new message available
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

// MultiConsume launches "maxConsumers" goroutines to consume messages in parallel
func (rmq *RabbitMQ) ConsumeMany(ctx context.Context, qInfo QueueInfo, prefetch int, consumer Consumer, maxConsumers int) error {

	if maxConsumers <= 0 {
		return fmt.Errorf("maxConsumers should be >0")
	}

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

	//init a chan of size maxConsumers that will control the number of goroutines that can be active at any given time
	semaphore := make(chan struct{}, maxConsumers)

	//range over messages in the rabbitmq channel
	for d := range msgs {

		select {
		default:
			// blocks if semaphore is full
			semaphore <- struct{}{}
			go func(d amqp.Delivery) {
				consumer(d)
				//removes 1 element from semaphore so that a new goroutine can be started
				<-semaphore
			}(d)

		case <-ctx.Done():
			return ctx.Err()

		}

	}

	// launch maxConsumers goroutines. Each goroutine executes a consumer which listens for messages in the channel
	// for i := 0; i < maxConsumers; i++ {

	// 	go func(msgs <-chan amqp.Delivery, ctx context.Context) error {

	// 		//wait for messages and
	// 		for d := range msgs {

	// 			select {
	// 			default:

	// 				consumer(d)

	// 			case <-ctx.Done():
	// 				return ctx.Err()

	// 			}

	// 		}
	// 		return nil
	// 	}(msgs, ctx)
	// }

	return nil
}

// ConsumeAutoack consumes and auto acks the delivery
func (rmq *RabbitMQ) ConsumeAutoack(ctx context.Context, qInfo QueueInfo, prefetch int, consumer Consumer) error {

	select {
	case <-rmq.SignalConnectionClosed:
		return fmt.Errorf("connection is closed cannot consume")
	default:
		if rmq.Conn.IsClosed() {
			close(rmq.SignalConnectionClosed)
			return fmt.Errorf("connection is closed cannot consume")

		}
	}

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

			consumer(d)

		case <-ctx.Done():
			return ctx.Err()

		}

	}

	return nil
}

// PublishAssert publishes a message to a queue, trying to also asset the queue before publishing
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

// StartRPC starts the RPC process by declaring the reply-to queue and initializing the consuming process from it.
// RPC responses are added to a map, having the correlation id as key. All consumers interested in a RPC response need
// to poll this map until their response is available
func (rmq *RabbitMQ) StartRPC(queueName string, ctx context.Context) error {

	var ch *amqp.Channel
	var deliveries <-chan amqp.Delivery
	var err error

	if rmq.Conn.IsClosed() {
		return fmt.Errorf("can not start RPC. Connection is closed")
	}

	rpcQueue := fmt.Sprintf("%s.%s", queueName, rmq.UUID)
	rpcMap = make(map[string]chan amqp.Delivery)
	rpcMapMutex = sync.RWMutex{}

	ch, err = rmq.Channel(1, 0, false)
	if err != nil {
		return err
	}

	_, err = ch.QueueDeclare(
		rpcQueue, // name
		false,    // durable
		true,     // delete when unused
		true,     // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return err
	}

	//start consuming from reply-to
	deliveries, err = ch.Consume(
		rpcQueue, // queue
		rpcQueue,
		false, // auto-ack
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)

	if err != nil {
		return err
	}

	go func(<-chan amqp.Delivery) {
		//loop until context expires or we get the wanted response
		for {
			if rmq.Conn.IsClosed() {
				continue
			}
			select {

			//wait for the next delivery from rabbitmq and add it to the corresponding map element
			case delivery := <-deliveries:
				//if the map entry exists, publish delivery into the map channel, else log an error
				if _, exists := rpcMap[delivery.CorrelationId]; exists {
					rpcMapMutex.Lock()
					rpcMap[delivery.CorrelationId] <- delivery
					delivery.Ack(false)
					rpcMapMutex.Unlock()
				} else {
					log.Printf("ERROR: RPC map entry not initialized for correlation id: %s", delivery.CorrelationId)
					delivery.Ack(false)
				}

			//if context expired close the channel
			case <-ctx.Done():
				log.Printf("context canceled. Closing RPC channel\n")
				err = ch.Close()
				if err != nil {
					log.Printf(err.Error())
				}
				return

			}
		}
	}(deliveries)

	return nil
}

// /RPC publishes a message using the rpc pattern, and blocks for the response until the context expires
func (rmq *RabbitMQ) RPC(queueName string, publishing amqp.Publishing, ctx context.Context) (amqp.Delivery, error) {
	var response amqp.Delivery

	select {
	case <-rmq.SignalConnectionClosed:
		return amqp.Delivery{}, fmt.Errorf("connection is closed")
	default:
		if rmq.Conn.IsClosed() {
			close(rmq.SignalConnectionClosed)
			return amqp.Delivery{}, fmt.Errorf("connection is closed")
		}
	}

	if rpcMap == nil {
		return response, fmt.Errorf("map has been not initialised. Make sure to call StartRPC() before using RPC()")
	}

	//open a channel
	ch, err := rmq.Channel(1, 0, false)
	if err != nil {
		return response, err
	}
	defer ch.Close()

	//publish the request
	err = ch.Publish(
		"",
		queueName,
		false,
		false,
		publishing)

	if err != nil {
		ch.Close()
		return response, err
	}

	//add a new channel to the map to wait for this response
	responseChan := make(chan amqp.Delivery, 1)
	rpcMapMutex.Lock()
	rpcMap[publishing.CorrelationId] = responseChan
	rpcMapMutex.Unlock()

	//loop until context expires or we get the wanted response
	for {

		select {

		//wait for the wanted response and close the channel once we got it
		case response = <-responseChan:
			if response.CorrelationId == publishing.CorrelationId {
				delete(rpcMap, publishing.CorrelationId)
				ch.Close()
				return response, nil
			}
		//if context expired close the channel and remove the entry from the map
		case <-ctx.Done():
			delete(rpcMap, publishing.CorrelationId)
			ch.Close()
			return response, fmt.Errorf("context expired for request: %s - error: %s", string(publishing.Body), ctx.Err())
		}
	}

}
