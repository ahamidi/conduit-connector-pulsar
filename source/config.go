package source

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

const (
	ConfigURL               = "url"
	ConfigOperationTimeout  = "operationTimeout"
	ConfigConnectionTimeout = "connectionTimeout"
	ConfigTopic             = "topic"
	ConfigSubscriptionName  = "subscriptionName"
	ConfigSubscriptionType  = "subscriptionType"
)

type Config struct {
	URL               string
	OperationTimeout  time.Duration
	ConnectionTimeout time.Duration
	Topic             string
	SubscriptionName  string
	SubscriptionType  pulsar.SubscriptionType
}

func Parse(cfg map[string]string) (Config, error) {
	var parsed Config

	// split URL in case there are multiple
	urls := strings.Split(cfg[ConfigURL], ",")
	if len(urls) == 0 {
		return Config{}, errors.New("URL is required")
	}

	// validate URLs
	for _, u := range urls {
		_, err := url.Parse(u)
		if err != nil {
			return Config{}, err
		}
	}

	parsed.URL = cfg[ConfigURL]

	// parse Topic
	topic, ok := cfg[ConfigTopic]
	if !ok {
		return Config{}, errors.New("topic is required")
	}
	parsed.Topic = topic

	// parse Subscription
	subName, ok := cfg[ConfigSubscriptionName]
	if !ok {
		return Config{}, errors.New("topic is required")
	}
	parsed.SubscriptionName = subName

	if subTypeConfig, ok := cfg[ConfigSubscriptionType]; ok {
		subType, err := parseSubType(subTypeConfig)
		if err != nil {
			return Config{}, err
		}
		parsed.SubscriptionType = subType
	}

	return parsed, nil
}

func parseSubType(t string) (pulsar.SubscriptionType, error) {
	switch strings.ToLower(t) {
	case "exclusive":
		return pulsar.Exclusive, nil
	case "shared":
		return pulsar.Shared, nil
	case "failover":
		return pulsar.Failover, nil
	case "keyshared":
		return pulsar.KeyShared, nil
	default:
		return 0, fmt.Errorf("unknown or unsupported Subscription Type %s", t)
	}
}
