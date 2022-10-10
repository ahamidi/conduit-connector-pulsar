package pulsar

import (
	"github.com/ahamidi/conduit-connector-pulsar/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var Connector = sdk.Connector{
	NewSpecification: Specification,
	NewSource:        source.New,
}
