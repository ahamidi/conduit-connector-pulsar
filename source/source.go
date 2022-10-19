package source

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	MetadataTopic = "topic"
)

type Source struct {
	sdk.UnimplementedSource

	config   Config
	client   pulsar.Client
	consumer pulsar.Consumer
}

func New() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		ConfigURL: {
			Default:  "", // pulsar://localhost:6650
			Required: true,
			Description: "Pulsar protocol URL for the Apache Pulsar cluster. Multiple URLs may be used by providing a comma delimited list " +
				"(e.g. pulsar://host1.example.com:6650,pulsar://host2.example.com:6650).",
		},
		ConfigOperationTimeout: {
			Default:     "30s",
			Required:    false,
			Description: "The operation timeout to be used by the internal Pulsar client.",
		},
		ConfigConnectionTimeout: {
			Default:     "30s",
			Required:    false,
			Description: "The connection timeout to be used by the internal Pulsar client.",
		},
		ConfigTopic: {
			Default:     "",
			Required:    true,
			Description: "The topic to consume. (e.g. persistent://public/default/sample)",
		},
		ConfigSubscriptionName: {
			Default:     "",
			Required:    true,
			Description: "The name of the subscription to be used.",
		},
		ConfigSubscriptionType: {
			Default:     "Exclusive",
			Required:    false,
			Description: "The type of subscription to use. Acceptable options are Exclusive, Failover, Shared and Key_Shared. Default is \"Exclusive\"",
		},
	}
}

// Configure parses and initializes the config.
func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	config, err := Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %v", err)
	}

	s.config = config

	return nil
}

// Open opens a connection to Pulsar broker
func (s *Source) Open(ctx context.Context, position sdk.Position) error {
	clientOpts := pulsar.ClientOptions{
		URL:               s.config.URL,
		OperationTimeout:  s.config.OperationTimeout,
		ConnectionTimeout: s.config.ConnectionTimeout,
	}

	client, err := pulsar.NewClient(clientOpts)
	if err != nil {
		return fmt.Errorf("could not initialize connection to Pulsar broker: %v", err)
	}

	s.client = client

	// create consumer
	consumerOpts := pulsar.ConsumerOptions{
		Topic:            s.config.Topic,
		SubscriptionName: s.config.SubscriptionName,
		Type:             s.config.SubscriptionType,
	}
	consumer, err := client.Subscribe(consumerOpts)
	if err != nil {
		return err
	}

	s.consumer = consumer

	return nil
}

// Read fetches records from the Pulsar broker
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	msg, err := s.consumer.Receive(ctx)
	if err != nil {
		return sdk.Record{}, err
	}

	return formatRecord(msg), nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	// pull message ID out of position to ack
	messageID, err := pulsar.DeserializeMessageID(position)
	if err != nil {
		return err
	}
	err = s.consumer.AckID(messageID)
	if err != nil {
		return err
	}
	return nil
}

// Teardown closes connections
func (s *Source) Teardown(ctx context.Context) error {
	if s.consumer != nil {
		err := s.consumer.Unsubscribe()
		if err != nil {
			return err
		}
		s.consumer.Close()
	}
	if s.client != nil {
		s.client.Close()
	}

	return nil
}

func formatRecord(msg pulsar.Message) sdk.Record {
	metadata := sdk.Metadata{
		MetadataTopic: msg.Topic(),
	}
	// todo: check if EventTime is available
	metadata.SetCreatedAt(msg.EventTime())

	pos := msg.ID().Serialize()

	return sdk.Util.Source.NewRecordCreate(
		pos,
		metadata,
		sdk.RawData(msg.Key()),
		sdk.RawData(msg.Payload()),
	)
}
