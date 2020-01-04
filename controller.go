package gousukafka

import (
	"encoding/base64"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/indece-official/go-gousu"
)

// KafkaHandleMsgFunc defines a function for handling kafka messages
type KafkaHandleMsgFunc func(msg *kafka.Message, topic string) error

// IKafkaController defines the interface of a controller consuming events on one kafka topic
type IKafkaController interface {
	gousu.IController
}

// KafkaController is a kafka consumer running in a separate thread waiting for message in topic
type KafkaController struct {
	kafkaService  IKafkaService
	log           *gousu.Log
	error         error
	topic         string
	HandleMsgFunc KafkaHandleMsgFunc
}

var _ IKafkaController = (*KafkaController)(nil)

// Start starts a kafka consumer waiting for new Mail-Messages
func (c *KafkaController) Start() error {
	go func() {
		subscription := make(chan *kafka.Message)
		var err error

		subscription, err = c.kafkaService.Subscribe(c.topic)
		if err != nil {
			c.error = c.log.ErrorfX("Error subscribing to kafka topic: %s", err)

			return
		}

		for {
			select {
			case msg, ok := <-subscription:
				if !ok {
					return
				}

				c.log.Debugf("Received kafka message on topic '%s': %s", c.topic, base64.StdEncoding.EncodeToString(msg.Value))

				err := c.HandleMsgFunc(msg, c.topic)

				c.kafkaService.Done(msg, err)
			}
		}
	}()

	return nil
}

// Health checks if the kafka consumer is healthy
func (c *KafkaController) Health() error {
	return c.error
}

// Stop stops the KafkaController
func (c *KafkaController) Stop() error {
	return nil
}

// NewKafkaControllerBase instantiates a new KafkaController
func NewKafkaControllerBase(log *gousu.Log,
	topic string,
	kafkaService IKafkaService) KafkaController {
	return KafkaController{
		topic:        topic,
		kafkaService: kafkaService,
		log:          log,
		HandleMsgFunc: func(msg *kafka.Message, topic string) error {
			return nil
		},
	}
}
