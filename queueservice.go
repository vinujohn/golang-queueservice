/* Things to think about
1) Fail messages on redelivery option
2) How to log to a file system
3) How to handle routing keys
4) Logging time
*/

package qsvc

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type queueService struct {
	connString string
}

type MessageProcessor interface {
	Process(b []byte)
}

func New(connString string) queueService {

	//conn, err := amqp.Dial(connString)

	return queueService{
		connString: connString,
	}
}

func (qs queueService) Subscribe(queueName string, processor MessageProcessor) {

	log.Println("Attempting to start service")

	conn, err := amqp.Dial(qs.connString)
	failOnError(err, "Could not make a connection")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Could not open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	deliveryChannel, err := ch.Consume(
		q.Name, // queue
		"test", // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	for delivery := range deliveryChannel {
		go func(delivery amqp.Delivery) {
			startTime := time.Now()
			processor.Process(delivery.Body)
			log.Printf("Finished processing message in %d milliseconds.", (time.Since(startTime).Nanoseconds() / int64(1000000)))
			delivery.Ack(false)
		}(delivery)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
}

func Publish(routingKey string) {

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
