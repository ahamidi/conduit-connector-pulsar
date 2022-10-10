package pulsar

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process (i.e. the Makefile).
// It follows Go's convention for module version, where the version
// starts with the letter v, followed by a semantic version.
var version = "v0.0.0-dev"

// Specification returns the Plugin's Specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "pulsar",
		Summary: "An Apache Pulsar source and destination plugin for Conduit, written in Go.",
		Description: "The Apache Pulsar connector is a  Conduit connector. " +
			"It provides both, a source and a destination NATS PubSub connector.",
		Version: version,
		Author:  "Ali Hamidi",
	}
}
