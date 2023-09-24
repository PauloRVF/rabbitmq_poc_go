package main

import (
	"fmt"
	"os"
	"time"

	"github.com/PauloRVF/rabbitmq_poc_go/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Print("You must inform the video path. Example: myvideo.mp4")
		return
	}
	video := os.Args[1]

	ch, err := rabbitmq.OpenChannel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	rabbitmq.Produce(ch, video, "video_pipeline", "created")

	msgs := make(chan amqp.Delivery)
	go rabbitmq.Consume(ch, msgs, "video_published")
	select {
	case msg := <-msgs:
		fmt.Println("video has been published: " + string(msg.Body))
		msg.Ack(false)
	case <-time.After(20 * time.Second):
		fmt.Println("timeout")
	}
}
