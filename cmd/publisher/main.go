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

	go rabbitmq.Consume(ch, msgs, "video_encoded")

	for msg := range msgs {
		publishedVideo := publishVideo(string(msg.Body))
		err = rabbitmq.Produce(ch, publishedVideo, "video_pipeline", "published")
		if err == nil {
			msg.Ack(false)
		}
	}
}

func publishVideo(video string) string {
	fmt.Fprintf(os.Stdout, "%v Publishing video %s\n", time.Now().Format(time.RFC3339), video)
	time.Sleep(5 * time.Second)
	fmt.Fprintf(os.Stdout, "%v Published video %s\n\n", time.Now().Format(time.RFC3339), video)

	return video + "_published"
}
