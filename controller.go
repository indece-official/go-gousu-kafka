package gousukafka

import (
	"encoding/base64"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/indece-official/go-gousu"
)

// ControllerName is the name of the kafka controller used for dependency injection
const ControllerName = "kafka"

// KafkaHandleMsgFunc defines a function for handling kafka messages
type KafkaHandleMsgFunc func(msg *kafka.Message, topic string) error

// IController defines the interface of a controller consuming events on one kafka topic
type IController interface {
	gousu.IController
}

// Controller is a kafka consumer running in a separate thread waiting for message in topic
type Controller struct {
	kafkaService  IService
	log           *gousu.Log
	error         error
	topic         string
	HandleMsgFunc KafkaHandleMsgFunc
}

var _ IController = (*Controller)(nil)

// Name returns the name of the kafka controller form ControllerName
func (c *Controller) Name() string {
	return ControllerName
}

// Start starts a kafka consumer waiting for new Mail-Messages
func (c *Controller) Start() error {
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
func (c *Controller) Health() error {
	return c.error
}

// Stop stops the Controller
func (c *Controller) Stop() error {
	return nil
}

// NewControllerBase instantiates a new kafka controller
//
// Required dependencies:
// * goususkafka.IService as "kafka"
func NewControllerBase(ctx gousu.IContext, topic string) IController {
	return &Controller{
		topic:        topic,
		kafkaService: ctx.GetService(ServiceName).(IService),
		log:          gousu.GetLogger(fmt.Sprintf("controller.%s", ControllerName)),
		HandleMsgFunc: func(msg *kafka.Message, topic string) error {
			return nil
		},
	}
}
