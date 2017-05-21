/* Things to think about
1) Fail messages on redelivery option
2) How to log to a file system
3) How to handle routing keys
*/

package qsvc

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type queueService struct {
	connString string
}

func NewQueueService(connString string) queueService {

	return queueService{
		connString: connString,
	}
}

func (qs queueService) Receive(queueName string, messageReceivedHandler func(b []byte)) {

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

	messages, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	for message := range messages {
		go func(message amqp.Delivery) {
			messageReceivedHandler(message.Body)
			message.Ack(false)
		}(message)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	<-forever
}
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
