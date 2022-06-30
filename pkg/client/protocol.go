// Request and response parsing.
// use xxhash for server list hash.
// https://github.com/cespare/xxhash
// + optional crc32?
// consistent hashing?
// https://github.com/stathat/consistent MIT licensed.
// maybe just an example that uses it separately?

package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Define errors here to allow comparison.
var (
	ErrCorruptValue       = errors.New("corrupt value in response")
	ErrUnknownStatus      = errors.New("unknown status code in response")
	ErrKeyTooLong         = errors.New("key is too long")
	ErrKeyDoesNotMatch    = errors.New("response key does not match request key")
	ErrUnexpectedResponse = errors.New("unexpected response from server")
	ErrServerError        = errors.New("SERVER_ERROR received")
)

type mcConn struct {
	conn net.Conn
	b    *bufio.ReadWriter
}

func (c *Client) connectToMc() (*mcConn, error) {
	var conn net.Conn
	var err error
	if c.socket != "" {
		conn, err = net.DialTimeout("unix", c.socket, c.ConnectTimeout)
	} else {
		conn, err = net.DialTimeout("tcp", c.Host, c.ConnectTimeout)
	}
	if err != nil {
		return nil, err
	}
	cn := mcConn{conn: conn}
	cn.b = bufio.NewReadWriter(bufio.NewReaderSize(conn, c.RBufSize), bufio.NewWriterSize(conn, c.WBufSize))
	return &cn, err
}

type Client struct {
	ConnectTimeout time.Duration
	// read or write timeout
	NetTimeout time.Duration
	Host       string
	socket     string
	cn         *mcConn
	WBufSize   int
	RBufSize   int
	// any necessary locks? channels?
	// binprot structure cache.
	binpkt         *packet
	opaque         uint32 // just for binprot?
	pipelines      int
	keyPrefix      string
	stripKeyPrefix bool
}

func NewClient(host string, socket string, pipelines uint, keyPrefix string, stripKeyPrefix bool) (client *Client) {
	client = &Client{
		Host:           host,
		socket:         socket,
		pipelines:      int(pipelines),
		keyPrefix:      keyPrefix,
		stripKeyPrefix: stripKeyPrefix,
		binpkt:         &packet{},
	}
	//client.rs = rand.NewSource(time.Now().UnixNano())
	return client
}

//////////////////////

// no overflow/anything testing. numerics from memcached should make sense
// unless corrupted.
func ParseUint(part []byte) (n uint64, i int) {
	for i, b := range part {
		if b < '0' || b > '9' {
			return n, i
		}
		n *= 10
		n += uint64(b - '0')
	}
	return n, 0
}

/////////////////////////////////////
// TEXT META PROTOCOL
/////////////////////////////////////

// TODO: write out the full code?
type McCode int

const (
	_             = iota
	McCHECK_ERROR // bodge. guessing these should all be in err...
	McVA
	McOK
	McEN
	McME
	McHD
	McNS
	McEX
	McNF
	McMN
	McSE
	McER
	McCL
	McHIT
	McMISS
	McSTORED
	McNOT_STORED
	McSERVER_ERROR
	McDELETED
	McNOT_FOUND
	McERROR
)

// TODO: reverse lookup status codes?

func (c *Client) ParseMetaResponse() (rflags []byte, value []byte, code McCode, err error) {
	// look for response
	line, err := c.cn.b.ReadBytes('\n')
	if err != nil {
		return nil, nil, 0, err
	}

	// VA flags token token token
	// TODO: There _must_ be some way to switch the bytes directly?
	// TODO: Can avoid splitting the response flags here.
	// bytes.IndexByte() to find and parse the thinger.
	// could also just roll through it with range and roll up the parseUint(),
	// having it stop at a space or newline.
	switch string(line[0:2]) {
	case "VA":
		// VA [size] [flags]
		//parts := bytes.Split(line[:len(line)-2], []byte(" "))
		size, offset := ParseUint(line[3:])
		// FIXME: if offset is 0, we failed?
		rflags = line[4+offset : len(line)-2]
		// Have some value data to read. + 2 bytes for \r\n
		value = make([]byte, size+2)
		_, err := io.ReadFull(c.cn.b, value)
		if err != nil {
			return nil, nil, 0, err
		}
		// check for \r\n, cut extra bytes off.
		if !bytes.Equal(value[len(value)-2:], []byte("\r\n")) {
			return nil, nil, 0, ErrCorruptValue
		}
		value = value[:size]
		code = McVA
	case "OK":
		//parts := bytes.Split(line[:len(line)-2], []byte(" "))
		// Chop "HD " and rest are flags.
		rflags = line[3 : len(line)-2]
		// No value to read, so we're done parsing the response.
		code = McOK
	case "EN":
		// MetaGet miss
		code = McEN
	case "ME":
		// Meta Debug command
		value = line[3 : len(line)-2]
		code = McME
	case "HD":
		// Meta STORED/DELETED
		code = McHD
	case "NS":
		// Meta NOT_STORED
		rflags = line[3 : len(line)-2]
		code = McNS
	case "EX":
		// Meta EXISTS (set or delete)
		rflags = line[3 : len(line)-2]
		code = McEX
	case "NF":
		// Meta NOT_FOUND (set or delete)
		rflags = line[3 : len(line)-2]
		code = McNF
	case "MN":
		// Meta NOP (response flush marker)
		code = McMN
	case "SE":
		// Probably SERVER_ERRROR
		code = McSE
	case "ER":
		// Probably ERROR (client side)
		code = McER
	case "CL":
		// Probably CLIENT_ERROR (client side)
		code = McCL
	default:
		fmt.Printf("Unknown: %s\n", string(line[0:2]))
		// TODO: Try error wrapping?
		return nil, nil, 0, ErrUnknownStatus
	}

	return
}

// Closures are the main Go pattern due to lack of macros?
func (c *Client) runNow(key string, avail int, fn func() error) (err error) {
	// test key for faults
	// NOTE: skipping the non-ascii character scan test because this code is
	// mostly benchmark code with predictable inputs.
	if len(key) > 250 {
		return ErrKeyTooLong
	}

	if c.cn == nil {
		cn, err := c.connectToMc()
		if err != nil {
			fmt.Println("FAILED TO CONNECT")
			return err
		}
		c.cn = cn
	}

	b := c.cn.b
	// To avoid checking errors a bunch of times, ensure there's enough space
	// ahead of time. two char code plus two spaces plus \r\n for the tail.
	if b.Available() < avail {
		err = b.Flush()
		if err != nil {
			return err
		}
	}

	return fn()
}

func (c *Client) MetaGet(key string, flags string) (err error) {
	err = c.runNow(key, len(key)+len(flags)+6, func() error {
		b := c.cn.b
		b.WriteString("mg ")
		b.WriteString(key)
		b.WriteString(" ")
		b.WriteString(flags)
		b.WriteString("\r\n")
		return nil
	})

	return
}

func (c *Client) MetaSet(key string, flags string, value []byte) (err error) {
	err = c.runNow(key, len(key)+len(flags)+6, func() error {
		b := c.cn.b
		b.WriteString("ms ")
		b.WriteString(key)
		b.WriteString(" ")
		b.WriteString(strconv.FormatUint(uint64(len(value)), 10))
		b.WriteString(" ")
		b.WriteString(flags)
		b.WriteString("\r\n")
		// For large sets this ends up flushing twice.
		// Change the interface to require \r\n or append or what?
		_, err = b.Write(value)
		b.WriteString("\r\n")
		return nil
	})
	return
}

func (c *Client) MetaDelete(key string, flags string) (err error) {
	err = c.runNow(key, len(key)+len(flags)+6, func() error {
		b := c.cn.b
		b.WriteString("md ")
		b.WriteString(key)
		b.WriteString(" ")
		b.WriteString(flags)
		b.WriteString("\r\n")
		return nil
	})
	return
}

// TODO: MetaDebug can't pipe? doesn't take/return flags.

func (c *Client) MetaNoop() (err error) {
	err = c.runNow("", 4, func() error {
		b := c.cn.b
		b.WriteString("mn\r\n")
		return nil
	})
	return
}

func (c *Client) MetaFlush() (err error) {
	b := c.cn.b
	err = b.Flush()
	return err
}

// Note: User should stop pulling when they know no more responses will
// happen, else this will wait forever.
func (c *Client) MetaReceive() (rflags []byte, value []byte, code McCode, err error) {
	b := c.cn.b
	// Auto flush if there's something buffered.
	if b.Writer.Buffered() != 0 {
		if err := b.Flush(); err != nil {
			return nil, nil, 0, err
		}
	}
	rflags, value, code, err = c.ParseMetaResponse()
	return
}

// TODO: helper func for chopping up result?
func (c *Client) MetaDebug(key string) (err error) {
	err = c.runNow(key, len(key)+5, func() error {
		b := c.cn.b
		b.WriteString("me ")
		b.WriteString(key)
		b.WriteString("\r\n")

		return nil
	})
	return
}

//////////////////////////////////////////////
// TEXT PROTOCOL
//////////////////////////////////////////////

func (c *Client) Get(key string) (flags uint64, value []byte, code McCode, err error) {
	pipelines := c.pipelines
	// Expected key from response
	respKey := key
	if c.stripKeyPrefix {
		respKey = strings.TrimPrefix(key, c.keyPrefix)
	}

	err = c.runNow(key, len(key)+6, func() error {
		b := c.cn.b
		for i := 0; i < pipelines; i++ {
			b.WriteString("get ")
			b.WriteString(key)
			b.WriteString("\r\n")
		}
		err = b.Flush()
		if err != nil {
			return err
		}

		for i := 0; i < pipelines; i++ {
			line, err := b.ReadBytes('\n')
			if err != nil {
				return err
			}

			if bytes.Equal(line, []byte("END\r\n")) {
				code = McMISS
			} else {
				parts := bytes.Split(line[:len(line)-2], []byte(" "))
				if !bytes.Equal(parts[0], []byte("VALUE")) {
					// TODO: This should look for ERROR/SERVER_ERROR/etc
					fmt.Print("Unexpected Response: ", string(line), "\n")
					continue
				}
				if len(parts) != 4 {
					fmt.Print("Unexpected Response: ", "parts not 4", "\n")
					continue
				}
				if !bytes.Equal(parts[1], []byte(respKey)) {
					fmt.Print("Unmatched Key: ", string(parts[1]), " and ", respKey, "\n")
					// FIXME: how do we embed the received vs expected in here?
					// use the brand-new golang error wrapping thing?
					continue
				}
				flags, _ = ParseUint(parts[2])
				size, _ := ParseUint(parts[3])

				value = make([]byte, size+2)
				_, err := io.ReadFull(b, value)
				if err != nil {
					fmt.Print("io ReadFull error, return", "\n")
					return err
				}

				if !bytes.Equal(value[len(value)-2:], []byte("\r\n")) {
					fmt.Print("Unmatched Value", "\n")
					continue
				}
				code = McHIT
				value = value[:size]

				line, err = b.ReadBytes('\n')
				if !bytes.Equal(line, []byte("END\r\n")) {
					fmt.Print("Unmatched Reponse: ", string(line), " is not END\r\n")
					continue
				}
			}
		}

		return nil
	})
	return
}

func (c *Client) Set(key string, flags uint32, expiration uint32, value []byte) (code McCode, err error) {
	err = c.runNow(key, len(key)+6+len(value), func() error {
		b := c.cn.b
		b.WriteString("set ")
		b.WriteString(key)
		b.WriteString(" ")
		b.WriteString(strconv.FormatUint(uint64(flags), 10))
		b.WriteString(" ")
		b.WriteString(strconv.FormatUint(uint64(expiration), 10))
		b.WriteString(" ")
		b.WriteString(strconv.FormatUint(uint64(len(value)), 10))
		b.WriteString("\r\n")
		_, err := b.Write(value)
		if err != nil {
			return err
		}
		b.WriteString("\r\n")
		err = b.Flush()

		if err != nil {
			return err
		}

		line, err := b.ReadBytes('\n')
		if err != nil {
			return err
		}

		if bytes.Equal(line, []byte("STORED\r\n")) {
			code = McSTORED
		} else if bytes.HasPrefix(line, []byte("SERVER_ERROR")) {
			// usually this is an OOM
			// TODO: error wrapping.
			return ErrServerError
		} else {
			// TODO: error wrapping
			fmt.Printf("Got instead of STORED: %s\n", string(line))
			return ErrUnexpectedResponse
		}

		return nil
	})
	return
}

func (c *Client) Delete(key string) (code McCode, err error) {
	err = c.runNow(key, len(key)+6, func() error {
		b := c.cn.b
		b.WriteString("delete ")
		b.WriteString(key)
		b.WriteString("\r\n")
		err = b.Flush()

		if err != nil {
			return err
		}

		line, err := b.ReadBytes('\n')
		if err != nil {
			return err
		}

		if bytes.Equal(line, []byte("DELETED\r\n")) {
			code = McDELETED
		} else if bytes.Equal(line, []byte("NOT_FOUND\r\n")) {
			code = McNOT_FOUND
		} else {
			return ErrUnexpectedResponse
		}

		return nil
	})
	return
}

func (c *Client) Incr(key string, delta uint64) (result uint64, code McCode, err error) {
	number := strconv.FormatUint(delta, 10)
	err = c.runNow(key, len(key)+len(number)+8, func() error {
		b := c.cn.b
		b.WriteString("incr ")
		b.WriteString(key)
		b.WriteString(" ")
		b.WriteString(number)
		b.WriteString("\r\n")
		err = b.Flush()

		if err != nil {
			return err
		}

		line, err := b.ReadBytes('\n')
		if err != nil {
			return err
		}

		if bytes.Equal(line, []byte("NOT_FOUND\r\n")) {
			code = McNOT_FOUND
		} else {
			// FIXME: string conversion garbage...
			// my byte function has no error handling.
			result, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
			if err != nil {
				return ErrUnexpectedResponse
			}
			code = McOK
		}

		return nil
	})
	return
}

func (c *Client) Decr(key string, delta uint64) (result uint64, code McCode, err error) {
	number := strconv.FormatUint(delta, 10)
	err = c.runNow(key, len(key)+len(number)+8, func() error {
		b := c.cn.b
		b.WriteString("decr ")
		b.WriteString(key)
		b.WriteString(" ")
		b.WriteString(number)
		b.WriteString("\r\n")
		err = b.Flush()

		if err != nil {
			return err
		}

		line, err := b.ReadBytes('\n')
		if err != nil {
			return err
		}

		if bytes.Equal(line, []byte("NOT_FOUND\r\n")) {
			code = McNOT_FOUND
		} else {
			// FIXME: string conversion garbage...
			// my byte function has no error handling.
			result, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
			if err != nil {
				return ErrUnexpectedResponse
			}
			code = McOK
		}

		return nil
	})

	return
}

//////////////////////////////////
// BINARY PROTOCOL
//////////////////////////////////

// Heavily influenced by or directly copied from:
// https://github.com/zeayes/gomemcache/blob/master/binary_protocol.go

// Debating if the other interfaces should use these structs... they should be
// passed in rather than generated internally.
// Also; should we be using a static byte buffer for key?
// not much to be done about the garbage generated by value, we can slice into
// what's generated by receiving data at least.
type Item struct {
	Key        string
	Value      []byte
	Expiration uint32
	Flags      uint32
	CAS        uint64
	Opaque     uint32
}

var zeroItem = &Item{}

func (it *Item) Reset() {
	*it = *zeroItem
}

const (
	requestMagic  = 0x80
	responseMagic = 0x81
)

var (
	// ErrItemNotFound indicates the item was not found when command is cas/delete/incr/decr
	ErrItemNotFound = errors.New("item is not found")
	// ErrItemExists indicates the item has stored where command is cas
	ErrItemExists = errors.New("item exists")
	// ErrItemNotStored indicates the data not stored where command is add
	ErrItemNotStored = errors.New("item is not stored")
)

const (
	McOP_GET        = 0x00
	McOP_SET        = 0x01
	McOP_ADD        = 0x02
	McOP_REPLACE    = 0x03
	McOP_DELETE     = 0x04
	McOP_INCREMENT  = 0x05
	McOP_DECREMENT  = 0x06
	McOP_QUIT       = 0x07
	McOP_FLUSH      = 0x08
	McOP_GETQ       = 0x09
	McOP_NOOP       = 0x0a
	McOP_VERSION    = 0x0b
	McOP_GETK       = 0x0c
	McOP_GETKQ      = 0x0d
	McOP_APPEND     = 0x0e
	McOP_PREPEND    = 0x0f
	McOP_STAT       = 0x10
	McOP_SETQ       = 0x11
	McOP_ADDQ       = 0x12
	McOP_REPLACEQ   = 0x13
	McOP_DELETEQ    = 0x14
	McOP_INCREMENTQ = 0x15
	McOP_DECREMENTQ = 0x16
	McOP_QUITQ      = 0x17
	McOP_FLUSHQ     = 0x18
	McOP_APPENDQ    = 0x19
	McOP_PREPENDQ   = 0x1a
	McOP_TOUCH      = 0x1c
)

var (
	hdrSize = binary.Size(header{})

	errorMap = map[uint16]error{
		0x001: ErrItemNotFound,
		0x002: ErrItemExists,
		0x003: errors.New("Value too large"),
		0x004: errors.New("Invalid arguments"),
		0x005: ErrItemNotStored,
		0x006: errors.New("Incr/Decr on non-numeric value"),
		0x007: errors.New("The vbucket belongs to another server"),
		0x008: errors.New("Authentication error"),
		0x009: errors.New("Authentication continue"),
		0x081: errors.New("Unknown command"),
		0x082: errors.New("Out of memory"),
		0x083: errors.New("Not supported"),
		0x084: errors.New("Internal error"),
		0x085: errors.New("Busy"),
		0x086: errors.New("Temporary failure"),
	}
)

// Header for request and response
type header struct {
	magic        uint8
	opcode       uint8
	keyLength    uint16
	extrasLength uint8
	dataType     uint8
	status       uint16
	bodyLength   uint32
	opaque       uint32
	cas          uint64
}

func (hdr *header) read(reader io.Reader) error {
	// FIXME: stack var array?
	hdrBuf := make([]byte, hdrSize)
	if n, err := io.ReadFull(reader, hdrBuf); err != nil || n != hdrSize {
		return err
	}
	hdr.magic = hdrBuf[0]
	hdr.opcode = hdrBuf[1]
	hdr.keyLength = binary.BigEndian.Uint16(hdrBuf[2:4])
	hdr.extrasLength = hdrBuf[4]
	hdr.dataType = hdrBuf[5]
	hdr.status = binary.BigEndian.Uint16(hdrBuf[6:8])
	hdr.bodyLength = binary.BigEndian.Uint32(hdrBuf[8:12])
	hdr.opaque = binary.BigEndian.Uint32(hdrBuf[12:16])
	hdr.cas = binary.BigEndian.Uint64(hdrBuf[16:24])
	return nil
}

func (hdr *header) write(buf []byte) {
	buf[0] = hdr.magic
	buf[1] = hdr.opcode
	binary.BigEndian.PutUint16(buf[2:4], hdr.keyLength)
	buf[4] = hdr.extrasLength
	buf[5] = hdr.dataType
	binary.BigEndian.PutUint16(buf[6:8], hdr.status)
	binary.BigEndian.PutUint32(buf[8:12], hdr.bodyLength)
	binary.BigEndian.PutUint32(buf[12:16], hdr.opaque)
	binary.BigEndian.PutUint64(buf[16:24], hdr.cas)
}

// Packet for request and response
type packet struct {
	header
	extras []byte
	key    string
	value  []byte
}

// wipe your packet header with minimal garbage with this one ... weird trick.
var zeropacket = &packet{}

func (pkt *packet) Reset() {
	*pkt = *zeropacket
}

// FIXME: too much garbage.
func (pkt *packet) write(writer io.Writer) error {
	buf := make([]byte, hdrSize, uint32(hdrSize)+pkt.bodyLength)
	// if err := binary.Write(buffer, binary.BigEndian, pkt.header); err != nil {
	// return err
	// }
	pkt.header.write(buf)
	buf = append(buf, pkt.extras...)
	buf = append(buf, pkt.key...)
	buf = append(buf, pkt.value...)
	_, err := writer.Write(buf)
	return err
}

func (pkt *packet) read(reader io.Reader) error {
	if err := pkt.header.read(reader); err != nil {
		return err
	}
	// if err := binary.Read(reader, binary.BigEndian, &pkt.header); err != nil {
	// return err
	// }
	body := make([]byte, pkt.bodyLength)
	if n, err := io.ReadFull(reader, body); err != nil || uint32(n) != pkt.bodyLength {
		return err
	}
	keyOffset := uint16(pkt.extrasLength) + pkt.keyLength
	if pkt.keyLength != 0 {
		pkt.key = string(body[pkt.extrasLength:keyOffset])
	}
	if pkt.bodyLength-uint32(keyOffset) != 0 {
		pkt.value = body[keyOffset:]
	}
	if pkt.extrasLength != 0 {
		pkt.extras = body[:pkt.extrasLength]
	}
	if pkt.status == 0 {
		return nil
	}
	e, ok := errorMap[pkt.status]
	if ok {
		return e
	}
	return fmt.Errorf("server response status code error: %d", pkt.status)
}

func (c *Client) runBin(key string, avail int, fn func(pkt *packet) error) (opaque uint32, err error) {
	// test key for faults
	// NOTE: skipping the non-ascii character scan test because this code is
	// mostly benchmark code with predictable inputs.
	if len(key) > 250 {
		return 0, ErrKeyTooLong
	}

	if c.cn == nil {
		cn, err := c.connectToMc()
		if err != nil {
			fmt.Println("FAILED TO CONNECT")
			return 0, err
		}
		c.cn = cn
	}

	b := c.cn.b
	// To avoid checking errors a bunch of times, ensure there's enough space
	// ahead of time. two char code plus two spaces plus \r\n for the tail.
	if b.Available() < avail {
		err = b.Flush()
		if err != nil {
			return 0, err
		}
	}

	// binprot specific stuff... hooray mostly copypasta.
	pkt := c.binpkt
	pkt.Reset()
	pkt.header.magic = requestMagic
	c.opaque++
	pkt.opaque = c.opaque

	if err := fn(pkt); err != nil {
		return 0, err
	}

	return c.opaque, pkt.write(c.cn.b)
}

// binary command functions only queue requests.

// sends a corrupt/bad/evil packet in some form.
func (c *Client) BinCorrupt() (opaque uint32, err error) {
	opaque, err = c.runBin("", 48, func(pkt *packet) error {
		pkt.header.opcode = uint8(McOP_GET)
		pkt.header.magic = 3

		return nil
	})
	return
}

// TODO: opaque.
func (c *Client) BinGet(key string) (opaque uint32, err error) {
	opaque, err = c.runBin(key, len(key)+48, func(pkt *packet) error {
		pkt.header.opcode = uint8(McOP_GETK)
		pkt.header.keyLength = uint16(len(key))
		pkt.header.bodyLength = uint32(len(key))
		pkt.key = key

		return nil
	})
	return
}

func (c *Client) BinSet(item *Item) (opaque uint32, err error) {
	opaque, err = c.runBin(item.Key, len(item.Key)+len(item.Value)+48, func(pkt *packet) error {
		pkt.key = item.Key
		pkt.value = item.Value
		pkt.cas = item.CAS
		extrasLength := 8

		pkt.extras = make([]byte, extrasLength)
		pkt.extrasLength = uint8(extrasLength)
		binary.BigEndian.PutUint32(pkt.extras[:4], item.Flags)
		binary.BigEndian.PutUint32(pkt.extras[4:], item.Expiration)

		pkt.header.opcode = uint8(McOP_SET)
		pkt.header.keyLength = uint16(len(item.Key))
		pkt.header.bodyLength = uint32(pkt.keyLength) + uint32(len(pkt.value)) + uint32(extrasLength)
		return nil
	})
	return
}

func (c *Client) BinTouch(item *Item) (opaque uint32, err error) {
	opaque, err = c.runBin(item.Key, len(item.Key)+48, func(pkt *packet) error {
		pkt.key = item.Key
		pkt.cas = item.CAS
		extrasLength := 4

		// pretty sure this is garbage waste; can keep a static 48 byte buffer.
		pkt.extras = make([]byte, extrasLength)
		pkt.extrasLength = uint8(extrasLength)

		binary.BigEndian.PutUint32(pkt.extras[:4], item.Expiration)

		pkt.header.opcode = uint8(McOP_TOUCH)
		pkt.header.keyLength = uint16(len(item.Key))
		pkt.header.bodyLength = uint32(pkt.keyLength) + uint32(extrasLength)
		return nil
	})
	return
}

func (c *Client) BinQuit() (opaque uint32, err error) {
	opaque, err = c.runBin("", 48, func(pkt *packet) error {
		pkt.header.opcode = uint8(McOP_QUIT)

		return nil
	})
	return
}

func (c *Client) BinQuitQ() (opaque uint32, err error) {
	opaque, err = c.runBin("", 48, func(pkt *packet) error {
		pkt.header.opcode = uint8(McOP_QUITQ)

		return nil
	})
	return
}

// This _could_ just be Flush() and shared, but there might be reasons to hook
// something protocol specific in here.
func (c *Client) BinFlush() (err error) {
	b := c.cn.b
	err = b.Flush()
	return err
}

// don't run this without anything in the queue :P
func (c *Client) BinReceive(item *Item) (opcode uint8, code McCode, err error) {
	b := c.cn.b
	item.Reset()
	// Flush if there's anything in the write queue.
	// Simplifies the API slightly.
	// This wouldn't be ideal if someone were queueing work, then want to come
	// back later and receive it; so should also be separate func?
	if b.Writer.Buffered() != 0 {
		if err := b.Flush(); err != nil {
			return 0xff, 0, err
		}
	}

	pkt := c.binpkt
	err = pkt.read(b)
	item.Opaque = pkt.opaque
	if err != nil {
		if pkt.status != 0 {
			return pkt.header.opcode, McERROR, err
		}
		return 0xff, McCHECK_ERROR, err
	}

	// FIXME: I think we don't want this?
	if err == ErrItemNotFound {
		return pkt.header.opcode, McNOT_FOUND, nil
	}

	// TODO: Discover what kind of response it was here and parse
	// appropriately?
	// we have a pkt.header.opcode to work with.
	switch pkt.header.opcode {
	case McOP_SET:
		fallthrough
	case McOP_ADD:
		fallthrough
	case McOP_REPLACE:
		fallthrough
	case McOP_REPLACEQ:
		fallthrough
	case McOP_SETQ:
		fallthrough
	case McOP_ADDQ:
		// set/add/replace MUST have CAS, no extras, key, or value.
		item.CAS = pkt.cas
	case McOP_APPEND:
		fallthrough
	case McOP_PREPEND:
		fallthrough
	case McOP_APPENDQ:
		fallthrough
	case McOP_PREPENDQ:
		// append/prepend MUST have CAS, NOT extras/key/value.
		item.CAS = pkt.cas
	case McOP_GET:
		fallthrough
	case McOP_GETQ:
		fallthrough
	case McOP_GETK:
		fallthrough
	case McOP_GETKQ:
		if pkt.value != nil {
			var flags uint32
			if pkt.extras != nil {
				flags = binary.BigEndian.Uint32(pkt.extras)
			}
			item.Key = pkt.key
			item.Value = pkt.value
			item.Flags = flags
			item.CAS = pkt.cas
		}
	case McOP_DELETE:
		fallthrough
	case McOP_DELETEQ:
		// delete MUST not have... anything.
	case McOP_QUIT:
		fallthrough
	case McOP_QUITQ:
		// quit MUST not have anything.
	case McOP_TOUCH:
		// might... not actually set the CAS, should check.
		item.CAS = pkt.cas
	default:
		return pkt.header.opcode, McCHECK_ERROR, ErrUnknownStatus
	}

	return pkt.header.opcode, McOK, nil
}
