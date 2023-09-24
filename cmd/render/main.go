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

	go rabbitmq.Consume(ch, msgs, "video_created")

	for msg := range msgs {
		videoRendered := renderVideo(string(msg.Body))
		err = rabbitmq.Produce(ch, videoRendered, "video_pipeline", "rendered")
		if err == nil {
			msg.Ack(false)
		}
	}
}

func renderVideo(video string) string {
	fmt.Fprintf(os.Stdout, "%v Rendering video %s\n", time.Now().Format(time.RFC3339), video)
	time.Sleep(4 * time.Second)
	fmt.Fprintf(os.Stdout, "%v Rendered video %s\n\n", time.Now().Format(time.RFC3339), video)

	return video + "_rendered"
}
