package main

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/pubsub"
)

type SubscriptionMessageHandler interface {
	Handle(subsriptionId string, onReceiveHandler MessageHandler, onError ErrorHandler) error
}

type pubSubMessageHandler struct {
	context      context.Context
	pubsubClient *pubsub.Client
}

type Message struct {
	ID         string
	Data       []byte
	Attributes map[string]string
}

type MessageHandler func(ctx context.Context, msg Message) error
type ErrorHandler func(ctx context.Context, err error)

func NewPubSubMessageHandler(ctx context.Context, projectID string) (SubscriptionMessageHandler, error) {
	client, err := createPubSubClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return &pubSubMessageHandler{context: ctx, pubsubClient: client}, nil
}

func (msgHandler *pubSubMessageHandler) Handle(subsriptionId string, onReceiveHandler MessageHandler, onError ErrorHandler) error {
	sub := msgHandler.pubsubClient.Subscription(subsriptionId)
	exists, err := sub.Exists(msgHandler.context)
	if err != nil {
		return fmt.Errorf("handleMessagesFromSubscription: can't check if subscription exists; %w", err)
	}
	if !exists {
		return errors.New("handleMessagesFromSubscription: subscription doesn't exists")
	}
	err = sub.Receive(msgHandler.context, func(ctx context.Context, msg *pubsub.Message) {
		err := onReceiveHandler(ctx, Message{ID: msg.ID, Data: msg.Data, Attributes: msg.Attributes})
		if err != nil {
			onError(ctx, err)
			msg.Nack()
		} else {
			msg.Ack()
		}
	})
	if err != nil {
		return fmt.Errorf("handleMessagesFromSubscription: can't receive; %w", err)
	}
	return nil
}

func createPubSubClient(ctx context.Context, projectName string) (*pubsub.Client, error) {
	client, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		return nil, fmt.Errorf("createPubSubClient: can't create PubSub Client; %w", err)
	}
	return client, nil
}
