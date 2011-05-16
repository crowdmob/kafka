/*
 * Copyright 2000-2011 NeuStar, Inc. All rights reserved.
 * NeuStar, the Neustar logo and related names and logos are registered
 * trademarks, service marks or tradenames of NeuStar, Inc. All other 
 * product names, company names, marks, logos and symbols may be trademarks
 * of their respective owners.  
 */

package main

import (
  "kafka"
  "flag"
  "fmt"
  "os"
  "strconv"
  "os/signal"
  "syscall"
)

var hostname string
var topic string
var partition int
var offset uint64
var maxSize uint
var writePayloadsTo string
var consumerForever bool

func init() {
  flag.StringVar(&hostname, "hostname", "localhost:9092", "host:port string for the kafka server")
  flag.StringVar(&topic, "topic", "test", "topic to publish to")
  flag.IntVar(&partition, "partition", 0, "partition to publish to")
  flag.Uint64Var(&offset, "offset", 0, "offset to start consuming from")
  flag.UintVar(&maxSize, "maxsize", 1048576, "offset to start consuming from")
  flag.StringVar(&writePayloadsTo, "writeto", "", "write payloads to this file")
  flag.BoolVar(&consumerForever, "consumeforever", false, "loop forever consuming")
}


func main() {
  flag.Parse()
  fmt.Println("Consuming Messages :")
  fmt.Printf("From: %s, topic: %s, partition: %d\n", hostname, topic, partition)
  fmt.Println(" ---------------------- ")
  broker := kafka.NewBrokerConsumer(hostname, topic, partition, offset, uint32(maxSize))

  var payloadFile *os.File = nil
  if len(writePayloadsTo) > 0 {
    var err os.Error
    payloadFile, err = os.Create(writePayloadsTo)
    if err != nil {
      fmt.Println("Error opening file: ", err)
      payloadFile = nil
    }
  }

  if consumerForever {
    quit := make(chan bool, 1)
    msgChan := make(chan *kafka.Message)
    go func() {
      for {
        sig := <-signal.Incoming
        if sig.(signal.UnixSignal) == syscall.SIGINT {
          quit <- true
        }
      }
    }()

    go broker.ConsumeOnChannel(msgChan, quit)
    for msg := range msgChan {
      if msg != nil {
        msg.Print()
      } else {
        break
      }
    }
  } else {
    broker.Consume(func(msg *kafka.Message) {
      msg.Print()
      if payloadFile != nil {
        payloadFile.Write([]byte("Message at: " + strconv.Uitoa64(msg.Offset()) + "\n"))
        payloadFile.Write(msg.Payload())
        payloadFile.Write([]byte("\n-------------------------------\n"))
      }
    })
  }

  if payloadFile != nil {
    payloadFile.Close()
  }

}