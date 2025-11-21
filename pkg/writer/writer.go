package writer

import (
	"context"
	"io"
)

type RemoteWriter interface {
	io.Writer

	// stop gracefully
	Stop(ctx context.Context) error

	// return monitor info
	Monitor() map[string]any
}
