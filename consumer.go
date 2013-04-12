/*

github.com/jedsmith/kafka: Go bindings for Kafka

Copyright 2000-2011 NeuStar, Inc. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of NeuStar, Inc., Jed Smith, nor the names of
	  contributors may be used to endorse or promote products derived from
	  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL NEUSTAR OR JED SMITH BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

NeuStar, the Neustar logo and related names and logos are registered
trademarks, service marks or tradenames of NeuStar, Inc. All other 
product names, company names, marks, logos and symbols may be trademarks
of their respective owners.  

*/

package kafka

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"time"
  "os"
)

type BrokerConsumer struct {
	broker  *Broker
	offset  uint64
	maxSize uint32
}

// Create a new broker consumer
// hostname - host and optionally port, delimited by ':'
// topic to consume
// partition to consume from
// offset to start consuming from
// maxSize (in bytes) of the message to consume (this should be at least as big as the biggest message to be published)
func NewBrokerConsumer(hostname string, topic string, partition int, offset uint64, maxSize uint32) *BrokerConsumer {
	return &BrokerConsumer{broker: newBroker(hostname, topic, partition),
		offset:  offset,
		maxSize: maxSize}
}

// Simplified consumer that defaults the offset and maxSize to 0.
// hostname - host and optionally port, delimited by ':'
// topic to consume
// partition to consume from
func NewBrokerOffsetConsumer(hostname string, topic string, partition int) *BrokerConsumer {
	return &BrokerConsumer{broker: newBroker(hostname, topic, partition),
		offset:  0,
		maxSize: 0}
}

// Keeps consuming forward until quit, outputing errors, but not dying on them
func (consumer *BrokerConsumer) ConsumeUntilQuit(pollTimeoutMs int64, quit chan os.Signal, msgHandler func(*Message)) (int64, int64, error) {
	conn, err := consumer.broker.connect()
	if err != nil {
		return -1, 0, err
	}

	messageCount := int64(0)
	skippedMessageCount := int64(0)
  
  quitReceived := false
	done := make(chan bool, 1)
  
  go func() {
    <-quit
    quitReceived = true
  }()
  
  go func() {
    for !quitReceived {
      _, err := consumer.consumeWithConn(conn, msgHandler)
			if err != nil && err != io.EOF {
				log.Printf("ERROR: [%s] %#v\n",  consumer.broker.topic, err)
			}
      time.Sleep(time.Duration(pollTimeoutMs) * time.Millisecond)
    }
		done <- true
  }()
  
	<-done // wait until the last iteration finishes before returning
  return messageCount, skippedMessageCount, nil
}

func (consumer *BrokerConsumer) ConsumeOnChannel(msgChan chan *Message, pollTimeoutMs int64, quit chan bool) (int, error) {
	conn, err := consumer.broker.connect()
	if err != nil {
		return -1, err
	}

	num := 0
	done := make(chan bool, 1)
	go func() {
		for {
			_, err := consumer.consumeWithConn(conn, func(msg *Message) {
				msgChan <- msg
				num += 1
			})

			if err != nil {
				if err != io.EOF {
					log.Println("Fatal Error: ", err)
				}
				break
			}
			time.Sleep(time.Duration(pollTimeoutMs) * time.Millisecond)
		}
		done <- true
	}()

	// wait to be told to stop..
	<-quit
	conn.Close()
	close(msgChan)
	<-done
	return num, err
}

type MessageHandlerFunc func(msg *Message)

func (consumer *BrokerConsumer) Consume(handlerFunc MessageHandlerFunc) (int, error) {
	conn, err := consumer.broker.connect()
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	num, err := consumer.consumeWithConn(conn, handlerFunc)

	if err != nil {
		log.Println("Fatal Error: ", err)
	}

	return num, err
}

func (consumer *BrokerConsumer) consumeWithConn(conn *net.TCPConn, handlerFunc MessageHandlerFunc) (int, error) {
	_, err := conn.Write(consumer.broker.EncodeConsumeRequest(consumer.offset, consumer.maxSize))
	if err != nil {
		return -1, err
	}

	length, payload, err := consumer.broker.readResponse(conn)

	if err != nil {
		return -1, err
	}

	num := 0
	if length > 2 {
		// parse out the messages
		var currentOffset uint64 = 0
		for currentOffset <= uint64(length-4) {
			msg := Decode(payload[currentOffset:])
			if msg == nil {
				return num, errors.New("Error Decoding Message")
			}
			msg.offset = consumer.offset + currentOffset
			currentOffset += uint64(4 + msg.totalLength)
			handlerFunc(msg)
			num += 1
		}
		// update the broker's offset for next consumption
		consumer.offset += currentOffset
	}

	return num, err
}

// Get a list of valid offsets (up to maxNumOffsets) before the given time, where 
// time is in milliseconds (-1, from the latest offset available, -2 from the smallest offset available)
// The result is a list of offsets, in descending order.
func (consumer *BrokerConsumer) GetOffsets(time int64, maxNumOffsets uint32) ([]uint64, error) {
	offsets := make([]uint64, 0)

	conn, err := consumer.broker.connect()
	if err != nil {
		return offsets, err
	}

	defer conn.Close()

	_, err = conn.Write(consumer.broker.EncodeOffsetRequest(time, maxNumOffsets))
	if err != nil {
		return offsets, err
	}

	length, payload, err := consumer.broker.readResponse(conn)
	if err != nil {
		return offsets, err
	}

	if length > 4 {
		// get the number of offsets
		numOffsets := binary.BigEndian.Uint32(payload[0:])
		var currentOffset uint64 = 4
		for currentOffset < uint64(length-4) && uint32(len(offsets)) < numOffsets {
			offset := binary.BigEndian.Uint64(payload[currentOffset:])
			offsets = append(offsets, offset)
			currentOffset += 8 // offset size
		}
	}

	return offsets, err
}
