package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type Header struct {
	ServiceMethod string // "User.Get"
	Seq           uint64 // sequence number
	Error         string
}

// GobCodec using Go's gob serialization
type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

func NewGobCodec(conn io.ReadWriteCloser) *GobCodec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

// Write encodes and sends an RPC message
func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		if flushErr := c.buf.Flush(); flushErr != nil {
			if err == nil {
				err = flushErr
			}
		}
		if err != nil {
			_ = c.Close()
		}
	}()
	if err = c.enc.Encode(h); err != nil {
		log.Printf("encode error: %v", err)
		return
	}
	if err = c.enc.Encode(body); err != nil {
		log.Printf("encode error: %v", err)
		return
	}
	return
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}
