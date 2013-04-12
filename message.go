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
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"log"
)

type Message struct {
	magic       byte
	checksum    [4]byte
	payload     []byte
	offset      uint64 // only used after decoding
	totalLength uint32 // total length of the message (decoding)
}

func (m *Message) Offset() uint64 {
	return m.offset
}

func (m *Message) Payload() []byte {
	return m.payload
}

func (m *Message) PayloadString() string {
	return string(m.payload)
}

func NewMessage(payload []byte) *Message {
	message := &Message{}
	message.magic = byte(MAGIC_DEFAULT)
	binary.BigEndian.PutUint32(message.checksum[0:], crc32.ChecksumIEEE(payload))
	message.payload = payload
	return message
}

// MESSAGE SET: <MESSAGE LENGTH: uint32><MAGIC: 1 byte><CHECKSUM: uint32><MESSAGE PAYLOAD: bytes>
func (m *Message) Encode() []byte {
	msgLen := 1 + 4 + len(m.payload)
	msg := make([]byte, 4+msgLen)
	binary.BigEndian.PutUint32(msg[0:], uint32(msgLen))
	msg[4] = m.magic
	copy(msg[5:], m.checksum[0:])
	copy(msg[9:], m.payload)
	return msg
}

func Decode(packet []byte) *Message {
	length := binary.BigEndian.Uint32(packet[0:])
	if length > uint32(len(packet[4:])) {
		log.Printf("length mismatch, expected at least: %X, was: %X from packet %#v\n", length, len(packet[4:]), packet)
    return nil
	}
	msg := Message{}
	msg.totalLength = length
	msg.magic = packet[4]
	copy(msg.checksum[:], packet[5:9])
	payloadLength := length - 1 - 4
	msg.payload = packet[9 : 9+payloadLength]

	payloadChecksum := make([]byte, 4)
	binary.BigEndian.PutUint32(payloadChecksum, crc32.ChecksumIEEE(msg.payload))
	if !bytes.Equal(payloadChecksum, msg.checksum[:]) {
		log.Printf("checksum mismatch, expected: %X was: %X\n", payloadChecksum, msg.checksum[:])
		return nil
	}
	return &msg
}

func (msg *Message) Print() {
	log.Println("----- Begin Message ------")
	log.Printf("magic: %X\n", msg.magic)
	log.Printf("checksum: %X\n", msg.checksum)
	if len(msg.payload) < 1048576 { // 1 MB 
		log.Printf("payload: %X\n", msg.payload)
		log.Printf("payload(string): %s\n", msg.PayloadString())
	} else {
		log.Printf("long payload, length: %d\n", len(msg.payload))
	}
	log.Printf("offset: %d\n", msg.offset)
	log.Println("----- End Message ------")
}
