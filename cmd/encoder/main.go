package main

import (
	"fmt"
	"os"
	"time"

	"github.com/PauloRVF/rabbitmq_poc_go/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ch, err := rabbitmq.OpenChannel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	msgs := make(chan amqp.Delivery)

	go rabbitmq.Consume(ch, msgs, "video_rendered")

	for msg := range msgs {
		videoEncoded := encodeVideo(string(msg.Body))
		err = rabbitmq.Produce(ch, videoEncoded, "video_pipeline", "encoded")
		if err == nil {
			msg.Ack(false)
		}
	}
}

func encodeVideo(video string) string {
	fmt.Fprintf(os.Stdout, "%s Encoding video %s\n", time.Now().Format(time.RFC3339), video)
	time.Sleep(3 * time.Second)
	fmt.Fprintf(os.Stdout, "%s Encoded video %s\n\n", time.Now().Format(time.RFC3339), video)

	return video + "_encoded"
}
