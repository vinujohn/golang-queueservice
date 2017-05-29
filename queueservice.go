/* Things to think about
1) Fail messages on redelivery option
5) TLS
*/

package qsvc

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type QueueService struct {
	connection      *amqp.Connection
	inboundChannel  *amqp.Channel
	outboundChannel *amqp.Channel
}

//MessageProcessor represents a type that can be used to process messages
//from an open inbound connection
type MessageProcessor interface {
	Process(b []byte)
}

//New creates a new queue service with an open TCP connection to rabbit
//with an inbound and outbound channel.
func New(connectionString string) QueueService {

	connection := createConnection(connectionString)

	return QueueService{
		connection:      connection,
		inboundChannel:  createChannel(connection),
		outboundChannel: createChannel(connection),
	}
}

func createConnection(uri string) *amqp.Connection {
	conn, err := amqp.Dial(uri)
	failOnError(err, "Could not make a connection to rabbit.")
	connectionCloseError := make(chan *amqp.Error)
	conn.NotifyClose(connectionCloseError)

	go func() {
		for {
			rabbitErr := <-connectionCloseError
			if rabbitErr != nil {
				log.Println("Connection to rabbit has been lost!")
				panic(rabbitErr)
			}
		}
	}()
	return conn
}

func createChannel(connection *amqp.Connection) *amqp.Channel {
	channel, err := connection.Channel()
	failOnError(err, "Could not open a channel")
	channelCloseError := make(chan *amqp.Error)
	channel.NotifyClose(channelCloseError)

	go func() {
		for {
			rabbitErr := <-channelCloseError
			if rabbitErr != nil {
				log.Println("Channel has been lost. Closing connection...!")
				connection.Close()
				panic(rabbitErr)
			}
		}
	}()

	return channel
}

//Subscribe attaches a subscriber to an inbound channel and uses the passed in
//MessageProcessor to
func (qs *QueueService) Subscribe(queueName string, processor MessageProcessor) {

	log.Printf("Attempting to subscribe to queue with name %s\n", queueName)

	q, err := qs.inboundChannel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	incoming, err := qs.inboundChannel.Consume(
		q.Name, // queue
		"test", // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	for delivery := range incoming {
		go func(delivery amqp.Delivery) {
			defer func() {
				if r := recover(); r != nil {
					log.Println("Error encountered when processing message: ", r)
					delivery.Reject(false)
				}
			}()

			startTime := time.Now()
			processor.Process(delivery.Body)
			log.Printf("Finished processing message in %d milliseconds.", (time.Since(startTime).Nanoseconds() / int64(1000000)))
			delivery.Ack(false)
		}(delivery)
	}
}

func (qs *QueueService) Publish(routingKey string, message []byte) {
	qs.outboundChannel.Publish(
		"",
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
