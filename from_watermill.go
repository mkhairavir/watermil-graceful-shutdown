package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

var (
	logger = watermill.NewStdLogger(false, false)
)

func main() {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddPlugin(plugin.SignalsHandler)

	router.AddMiddleware(

		middleware.CorrelationID,

		middleware.Retry{
			MaxRetries:      3,
			InitialInterval: time.Millisecond * 100,
			Logger:          logger,
		}.Middleware,
		middleware.NewThrottle(200, 1*time.Second).Middleware,

		middleware.Recoverer,
	)

	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	router.AddNoPublisherHandler(
		"helloworld",
		"hello_world",
		pubSub,
		helloWorld,
	)

	go publishMessages(pubSub)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := router.Run(ctx); err != nil {
		panic(err)
	}

	<-ctx.Done()
}

func publishMessages(publisher message.Publisher) {
	time.Sleep(100 * time.Millisecond)
	for i := range 1000 {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("hello world! %d", i)))
		middleware.SetCorrelationID(watermill.NewUUID(), msg)

		log.Printf("sending message %s, correlation id: %s\n", msg.UUID, middleware.MessageCorrelationID(msg))

		if err := publisher.Publish("hello_world", msg); err != nil {
			panic(err)
		}

	}
	fmt.Println("all message published")
}

func printMessages(msg *message.Message) error {
	fmt.Printf(
		"\n> Received message: %s\n> %s\n> metadata: %v\n\n",
		msg.UUID, string(msg.Payload), msg.Metadata,
	)
	return nil
}

type structHandler struct {
}

func helloWorld(msg *message.Message) error {
	fmt.Println(string(msg.Payload))
	return nil
}

func (s structHandler) Handler(msg *message.Message) ([]*message.Message, error) {
	log.Println("structHandler received message", msg.UUID)

	msg = message.NewMessage(watermill.NewUUID(), []byte("message produced by structHandler"))
	return message.Messages{msg}, nil
}
