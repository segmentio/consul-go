package consul

import (
	"context"
	"time"
)

func errorContext(ctx context.Context, err error) (context.Context, context.CancelFunc) {
	return newErrorCtx(ctx, err), func() {}
}

func coalesceError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

type contextKey struct {
	name string
}

type errorCtx struct {
	ctx  context.Context
	err  error
	done chan struct{}
}

func newErrorCtx(ctx context.Context, err error) *errorCtx {
	done := make(chan struct{})
	close(done)
	return &errorCtx{
		ctx:  ctx,
		err:  err,
		done: done,
	}
}

func (*errorCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (e *errorCtx) Done() <-chan struct{} {
	return e.done
}

func (e *errorCtx) Err() error {
	return e.err
}

func (e *errorCtx) Value(key interface{}) interface{} {
	return e.ctx.Value(key)
}
