// Package opentracing provides wrappers for OpenTracing
package opentracing

import (
	"fmt"

	"context"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/server"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
)

type otWrapper struct {
	ot opentracing.Tracer
	client.Client
}

func traceIntoContext(ctx context.Context, tracer opentracing.Tracer, name string, tag opentracing.Tag) (context.Context, opentracing.Span, error) {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		md = make(map[string]string)
	}

	var sp opentracing.Span
	var parentSpanCtx opentracing.SpanContext
	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		parentSpanCtx = parent.Context()
	} else {
		wc, err := tracer.Extract(opentracing.TextMap, opentracing.TextMapCarrier(md))
		if err == nil {
			parentSpanCtx = wc
		}
	}
	if parentSpanCtx == nil {
		sp = tracer.StartSpan(name, tag)
	} else {
		sp = tracer.StartSpan(name, opentracing.ChildOf(parentSpanCtx), tag)
	}
	if err := sp.Tracer().Inject(sp.Context(), opentracing.TextMap, opentracing.TextMapCarrier(md)); err != nil {
		return nil, nil, err
	}
	ctx = opentracing.ContextWithSpan(ctx, sp)
	ctx = metadata.NewContext(ctx, md)
	return ctx, sp, nil
}

func (o *otWrapper) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) error {
	ctx, span, err := traceIntoContext(ctx, o.ot, req.Method(), ext.SpanKindRPCClient)
	if err != nil {
		return err
	}
	err = o.Client.Call(ctx, req, rsp, opts...)
	return finishSpan(span, err)
}

func (o *otWrapper) Publish(ctx context.Context, p client.Publication, opts ...client.PublishOption) error {
	name := fmt.Sprintf("Pub to %s", p.Topic())
	ctx, span, err := traceIntoContext(ctx, o.ot, name, ext.SpanKindProducer)
	if err != nil {
		return err
	}
	ext.MessageBusDestination.Set(span, p.Topic())
	err = o.Client.Publish(ctx, p, opts...)
	return finishSpan(span, err)
}

// NewClientWrapper accepts an open tracing Trace and returns a Client Wrapper
func NewClientWrapper(ot opentracing.Tracer) client.Wrapper {
	return func(c client.Client) client.Client {
		return &otWrapper{ot, c}
	}
}

// NewHandlerWrapper accepts an opentracing Tracer and returns a Handler Wrapper
func NewHandlerWrapper(ot opentracing.Tracer) server.HandlerWrapper {
	return func(h server.HandlerFunc) server.HandlerFunc {
		return func(ctx context.Context, req server.Request, rsp interface{}) error {
			ctx, span, err := traceIntoContext(ctx, ot, req.Method(), ext.SpanKindRPCServer)
			if err != nil {
				return err
			}
			err = h(ctx, req, rsp)
			return finishSpan(span, err)
		}
	}
}

// NewSubscriberWrapper accepts an opentracing Tracer and returns a Subscriber Wrapper
func NewSubscriberWrapper(ot opentracing.Tracer) server.SubscriberWrapper {
	return func(next server.SubscriberFunc) server.SubscriberFunc {
		return func(ctx context.Context, msg server.Publication) error {
			name := "Pub to " + msg.Topic()
			ctx, span, err := traceIntoContext(ctx, ot, name, ext.SpanKindConsumer)
			if err != nil {
				return err
			}
			err = next(ctx, msg)
			return finishSpan(span, err)
		}
	}
}

func finishSpan(span opentracing.Span, err error) error {
	if err != nil {
		ext.Error.Set(span, true)
		span.LogFields(log.String("event", "error"), log.String("message", err.Error()))
	}
	span.Finish()
	return err
}
