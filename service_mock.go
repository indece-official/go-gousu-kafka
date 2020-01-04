package gousukafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/indece-official/go-gousu"
)

// MockKafkaService for simply mocking IKafkaService
type MockKafkaService struct {
	gousu.MockService

	DoneFunc            func(msg *kafka.Message, err error)
	SubscribeFunc       func(topic string) (chan *kafka.Message, error)
	ProduceFunc         func(topic string, value []byte) error
	DoneFuncCalled      int
	SubscribeFuncCalled int
	ProduceFuncCalled   int
}

// MockKafkaService implements IKafkaService
var _ (IKafkaService) = (*MockKafkaService)(nil)

// Done calls DoneFunc and increases DoneFuncCalled
func (s *MockKafkaService) Done(msg *kafka.Message, err error) {
	s.DoneFuncCalled++

	s.DoneFunc(msg, err)
}

// Subscribe calls SubscribeFunc and increases SubscribeFuncCalled
func (s *MockKafkaService) Subscribe(topic string) (chan *kafka.Message, error) {
	s.SubscribeFuncCalled++

	return s.SubscribeFunc(topic)
}

// Produce calls ProduceFunc and increases ProduceFuncCalled
func (s *MockKafkaService) Produce(topic string, value []byte) error {
	s.ProduceFuncCalled++

	return s.ProduceFunc(topic, value)
}

// NewMockKafkaService creates a new initialized instance of MockKafkaService
func NewMockKafkaService() *MockKafkaService {
	return &MockKafkaService{
		MockService: gousu.MockService{},

		DoneFunc: func(msg *kafka.Message, err error) {},
		SubscribeFunc: func(topic string) (chan *kafka.Message, error) {
			return nil, nil
		},
		ProduceFunc: func(topic string, value []byte) error {
			return nil
		},
		DoneFuncCalled:      0,
		SubscribeFuncCalled: 0,
		ProduceFuncCalled:   0,
	}
}
