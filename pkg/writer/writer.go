package writer

import (
	"context"
	"io"
)

type RemoteWriter interface {
	io.Writer

	Stop(ctx context.Context) error

	Monitor() string
}
