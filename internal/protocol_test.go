package internal

import (
	"bytes"
	"testing"
	"time"
)

// NOTE: these tests are just bare minimum hackery to validate what I'm doing.
// as the API settles we should obviously tabulate and write more of them :)

// tests expect a recent memcached to be running at this address.
const hostname = "127.0.0.1:11211"
const socket = ""
const pipelines = 1
const keyPrefix = "mctester:"
const stripKeyPrefix = false

func newcli() *Client {
	mc := NewClient(hostname, socket, pipelines, keyPrefix, stripKeyPrefix)
	mc.ConnectTimeout = 3 * time.Second
	mc.NetTimeout = time.Second
	mc.WBufSize = 64 * 1024
	mc.RBufSize = 128 * 1024
	return mc
}

func TestMeta(t *testing.T) {
	mc := newcli()
	{
		err := mc.MetaSet("doob", "S4 T300", []byte("foop"))
		if err != nil {
			t.Fatalf("metaset error: %v", err)
		}
		_, _, c, err := mc.MetaReceive()
		if c != McOK {
			t.Fatalf("metaset not stored: %d", c)
		}
	}

	{
		err := mc.MetaGet("doob", "f v")
		if err != nil {
			t.Fatalf("metaget error: %v", err)
		}
		_, v, _, err := mc.MetaReceive()
		if !bytes.Equal(v, []byte("foop")) {
			t.Fatalf("metaget bad value: %s", string(v))
		}
	}
}

func TestText(t *testing.T) {
	mc := newcli()

	{
		if _, err := mc.Set("flarb", 0, 0, []byte("stuff")); err != nil {
			t.Fatalf("error setting flarb: %v", err)
		}
	}

	{
		_, v, _, err := mc.Get("flarb")
		if err != nil {
			t.Fatalf("error getting flarb: %v", err)
		}
		if !bytes.Equal(v, []byte("stuff")) {
			t.Fatalf("get bad value: %s", string(v))
		}
	}

	{
		c, err := mc.Delete("doob")

		if err != nil {
			t.Fatalf("error deleting: %v", err)
		}

		if c != McDELETED {
			t.Fatalf("text delete: wrong error code: %d", c)
		}
	}

	{
		if _, err := mc.Set("number", 0, 0, []byte("0")); err != nil {
			t.Fatalf("error setting number: %v", err)
		} else {
			r, c, err := mc.Incr("number", 7)
			if err != nil {
				t.Fatalf("error incrementing number: %v", err)
			} else if c != McOK {
				t.Fatalf("increment result: %d\n", c)
			} else if r != 7 {
				t.Fatalf("increment wrong result: %d", r)
			}

			r, c, err = mc.Decr("number", 3)
			if err != nil {
				t.Fatalf("error incrementing number: %v", err)
			} else if c != McOK {
				t.Fatalf("decrement result: %d\n", c)
			} else if r != 4 {
				t.Fatalf("increment wrong result: %d", r)
			}
		}
	}
}

func TestBinary(t *testing.T) {
	mcb := newcli()

	it := &Item{Key: "binset",
		Value:      []byte("yupyup"),
		Expiration: 90,
	}
	// using a new variable, but in reality clients could reuse easily.
	it2 := &Item{}

	mcb.BinSet(it)
	mcb.BinGet("binset")
	// first one gets the result of the set op from above :P
	mcb.BinReceive(it2)
	// this one should get the BinGet.
	mcb.BinReceive(it2)

	if !bytes.Equal(it2.Value, []byte("yupyup")) {
		t.Fatalf("binary get bad value: %s", string(it2.Value))
	}

	// ***** response object pipelining tests *****

	// issue a handful of gets, and then a QUITQ
	mcb.BinGet("tood")
	mcb.BinGet("food")
	mcb.BinGet("mood")
	mcb.BinGet("dood")
	mcb.BinQuitQ()
	mcb.BinReceive(it2)

	// handful of gets, then a QUIT
	mcb = newcli()
	mcb.BinGet("tood")
	mcb.BinGet("food")
	mcb.BinGet("mood")
	mcb.BinGet("dood")
	mcb.BinQuit()
	mcb.BinReceive(it2)

	// send a few good items, then a corrupt one.
	mcb = newcli()
	mcb.BinGet("one")
	mcb.BinGet("two")
	mcb.BinGet("three")
	mcb.BinCorrupt()
	mcb.BinReceive(it2)
}
