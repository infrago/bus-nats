package bus_nats

import (
	"github.com/infrago/bus"
	"github.com/infrago/infra"
)

func Driver() bus.Driver {
	return &natsBusDriver{}
}

func init() {
	infra.Register("nats", Driver())
}
