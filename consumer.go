package sarama

import (
	"time"
)

// ConsumerConfig is used to pass multiple configuration options to NewConsumer.
type ConsumerConfig struct {
	// The default (maximum) amount of data to fetch from the broker in each request. The default is 32768 bytes.
	DefaultFetchSize int32
	// The minimum amount of data to fetch in a request - the broker will wait until at least this many bytes are available.
	// The default is 1, as 0 causes the consumer to spin when no messages are available.
	MinFetchSize int32
	// The maximum permittable message size - messages larger than this will return MessageTooLarge. The default of 0 is
	// treated as no limit.
	MaxMessageSize int32
	// The maximum amount of time the broker will wait for MinFetchSize bytes to become available before it
	// returns fewer than that anyways. The default is 250ms, since 0 causes the consumer to spin when no events are available.
	// 100-500ms is a reasonable range for most cases. Kafka only supports precision up to milliseconds; nanoseconds will be truncated.
	MaxWaitTime time.Duration
	// The number of events to buffer in the Events channel. Having this non-zero permits the
	// consumer to continue fetching messages in the background while client code consumes events,
	// greatly improving throughput. The default is 16.
	EventBufferSize int
}

// NewConsumerConfig creates a ConsumerConfig instance with sane defaults.
func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		DefaultFetchSize: 32768,
		MinFetchSize:     1,
		MaxWaitTime:      250 * time.Millisecond,
		EventBufferSize:  16,
	}
}

// Validate checks a ConsumerConfig instance. It will return a
// ConfigurationError if the specified value doesn't make sense.
func (config *ConsumerConfig) Validate() error {
	if config.DefaultFetchSize <= 0 {
		return ConfigurationError("Invalid DefaultFetchSize")
	}

	if config.MinFetchSize <= 0 {
		return ConfigurationError("Invalid MinFetchSize")
	}

	if config.MaxMessageSize < 0 {
		return ConfigurationError("Invalid MaxMessageSize")
	}

	if config.MaxWaitTime < 1*time.Millisecond {
		return ConfigurationError("Invalid MaxWaitTime, it needs to be at least 1ms")
	} else if config.MaxWaitTime < 100*time.Millisecond {
		Logger.Println("ConsumerConfig.MaxWaitTime is very low, which can cause high CPU and network usage. See documentation for details.")
	} else if config.MaxWaitTime%time.Millisecond != 0 {
		Logger.Println("ConsumerConfig.MaxWaitTime only supports millisecond precision; nanoseconds will be truncated.")
	}

	if config.EventBufferSize < 0 {
		return ConfigurationError("Invalid EventBufferSize")
	}

	return nil
}

// ConsumerEvent is what is provided to the user when an event occurs. It is either an error (in which case Err is non-nil) or
// a message (in which case Err is nil and Offset, Key, and Value are set). Topic and Partition are always set.
type ConsumerEvent struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
	Err        error
}

// Consumer processes Kafka messages from a given topic and partition.
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Consumer struct {
	client *Client

	config ConsumerConfig

	stopper, done chan bool
	events        chan *ConsumerEvent
}

// NewConsumer creates a new consumer attached to the given client. It will read messages from the given topic and partition
func NewConsumer(client *Client, config *ConsumerConfig) (*Consumer, error) {
	// Check that we are not dealing with a closed Client before processing
	// any other arguments
	if client.Closed() {
		return nil, ClosedClient
	}

	if config == nil {
		config = NewConsumerConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	c := &Consumer{
		client:  client,
		config:  *config,
		stopper: make(chan bool),
		done:    make(chan bool),
		events:  make(chan *ConsumerEvent, config.EventBufferSize),
	}

	go withRecover(c.dispatcher)

	return c, nil
}

// Events returns the read channel for any events (messages or errors) that might be returned by the broker.
func (c *Consumer) Events() <-chan *ConsumerEvent {
	return c.events
}

// Close stops the consumer from fetching messages. It is required to call this function before
// a consumer object passes out of scope, as it will otherwise leak memory. You must call this before
// calling Close on the underlying client.
func (c *Consumer) Close() error {
	close(c.stopper)
	<-c.done
	return nil
}

type workUnit struct {
	topic     string
	partition int32
	offset    int64
}

type action int

const (
	startConsuming action = iota
	stopConsuming
)

type workAction struct {
	action action
	work   *workUnit
}

func (c *Consumer) dispatcher() {
}

func (c *Consumer) mediator(b *Broker, in chan *workAction) {
	var buf []*workAction
	out := make(chan []*workAction)

	go withRecover(func() { c.fetcher(b, out) })

	for {
		select {
		case unit, ok := <-in:
			if ok {
				buf = append(buf, unit)
			} else {
				goto shutdown
			}
		case out <- buf:
			buf = nil
		}
	}

shutdown:
	if len(buf) > 0 {
		out <- buf
	}
	close(out)
}

func (c *Consumer) fetcher(b *Broker, in chan []*workAction) {
	for newWork := range in {
	}
}
