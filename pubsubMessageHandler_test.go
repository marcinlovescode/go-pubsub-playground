package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func emptyHandleError(ctx context.Context, err error) {}
func emptyHandle(ctx context.Context, msg Message) error {
	return nil
}
func TestCallsHandlerOnMessageReceive(t *testing.T) {
	//Arrange
	timeout := time.After(10 * time.Second)
	projectName := "my-project"
	topicName := fmt.Sprintf("topic-%d-1", uuid.New().ID())
	subscriptionName := fmt.Sprintf("sub-%d-1", uuid.New().ID())
	expectedMsg := uuid.New().String()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	receivedPayloadCh := make(chan string, 1)
	testFunc := func(ctx context.Context, msg Message) error {
		receivedPayloadCh <- string(msg.Data)
		return nil
	}

	requireNotError(t, setupEmulator())
	requireNotError(t, createTopicAndSubscription(ctx, projectName, topicName, subscriptionName))
	subHandler, err := NewPubSubMessageHandler(ctx, projectName)
	requireNotError(t, err)
	//Act
	go func() {
		requireNotError(t, subHandler.Handle(subscriptionName, testFunc, emptyHandleError))
		if err != nil {
			panic(err)
		}
	}()
	sendMessageToTopic(ctx, projectName, topicName, expectedMsg)
	//Assert
	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case msg := <-receivedPayloadCh:
		require.Equal(t, expectedMsg, msg)
	}
	cancel()
}

func TestCallsErrorHandlerOnMessageReceive(t *testing.T) {
	//Arrange
	timeout := time.After(10 * time.Second)
	projectName := "my-project"
	topicName := fmt.Sprintf("topic-%d-2", uuid.New().ID())
	subscriptionName := fmt.Sprintf("sub-%d-2", uuid.New().ID())
	expectedErrorMsg := uuid.New().String()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	receivedPayloadCh := make(chan string, 1)
	throwingFunc := func(ctx context.Context, msg Message) error {
		return errors.New(expectedErrorMsg)
	}
	errorHandlerFunc := func(ctx context.Context, err error) {
		receivedPayloadCh <- string(err.Error())
	}
	requireNotError(t, setupEmulator())
	requireNotError(t, createTopicAndSubscription(ctx, projectName, topicName, subscriptionName))
	subHandler, err := NewPubSubMessageHandler(ctx, projectName)
	requireNotError(t, err)
	//Act
	go func() {
		requireNotError(t, subHandler.Handle(subscriptionName, throwingFunc, errorHandlerFunc))
		if err != nil {
			panic(err)
		}
	}()
	sendMessageToTopic(ctx, projectName, topicName, "Who cares?")
	//Assert
	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case msg := <-receivedPayloadCh:
		require.Equal(t, expectedErrorMsg, msg)
	}
	cancel()
}

func TestMakesSureSubscriptionExists(t *testing.T) {
	//Arrange
	projectName := "my-project"
	topicName := fmt.Sprintf("topic-%d-3", uuid.New().ID())
	subscriptionName := fmt.Sprintf("sub-%d-3", uuid.New().ID())
	expectedErrorMsg := "subscription doesn't exists"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	requireNotError(t, setupEmulator())
	requireNotError(t, createTopicAndSubscription(ctx, projectName, topicName, subscriptionName))
	subHandler, err := NewPubSubMessageHandler(ctx, projectName)
	requireNotError(t, err)
	//Act
	err = subHandler.Handle("sub-4", emptyHandle, emptyHandleError)
	//Assert
	require.Contains(t, err.Error(), expectedErrorMsg)
	cancel()
}

func createTopicAndSubscription(ctx context.Context, projectName, topicName, subscriptionName string) error {
	client, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		return fmt.Errorf("createTopicAndSubscription: can't create PubSub Client; %w", err)
	}
	topic, err := client.CreateTopic(ctx, topicName)
	if err != nil {
		return fmt.Errorf("createTopicAndSubscription: can't create topic; %w", err)
	}
	_, err = client.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		return fmt.Errorf("createTopicAndSubscription: can't create topic; %w", err)
	}
	return nil
}

func sendMessageToTopic(ctx context.Context, projectName, topicName, message string) error {
	client, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		return fmt.Errorf("sendMessageToTopic: can't create PubSub Client; %w", err)
	}
	client.Topic(topicName).Publish(ctx, &pubsub.Message{Data: []byte(message)})
	return nil
}

func setupEmulator() error {
	return os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
}

func requireNotError(t *testing.T, err error) {
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
}
