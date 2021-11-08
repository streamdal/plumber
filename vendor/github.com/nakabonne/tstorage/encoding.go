// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package tstorage

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
)

type seriesEncoder interface {
	encodePoint(point *DataPoint) error
	flush() error
}

func newSeriesEncoder(w io.Writer) seriesEncoder {
	return &gorillaEncoder{
		w:   w,
		buf: &bstream{stream: make([]byte, 0)},
	}
}

// gorillaEncoder implements the Gorilla's time-series data compression.
// See: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
type gorillaEncoder struct {
	// backend stream writer
	w io.Writer

	// buffer to be used while encoding
	buf *bstream

	// Calculate the delta of delta:
	// D = (t_n − t_n−1) − (t_n−1 − t_n−2)
	//
	// t_0, starting timestamp t_0
	// immutable
	t0 int64
	// t_1, the next to starting timestamp
	// immutable
	t1 int64
	// t_n, timestamp of the Nth data point
	// mutable
	t int64
	// delta of t_n
	tDelta uint64

	// v_n, value of the Nth data point
	v        float64
	leading  uint8
	trailing uint8
}

// encodePoints is not goroutine safe. It's caller's responsibility to lock it.
func (e *gorillaEncoder) encodePoint(point *DataPoint) error {
	var tDelta uint64

	// Borrowed from https://github.com/prometheus/prometheus/blob/39d79c3cfb86c47d6bc06a9e9317af582f1833bb/tsdb/chunkenc/xor.go#L150
	switch {
	case e.t0 == 0:
		// Write timestamp directly.
		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutVarint(buf, point.Timestamp)] {
			e.buf.writeByte(b)
		}
		// Write value directly.
		e.buf.writeBits(math.Float64bits(point.Value), 64)
		e.t0 = point.Timestamp
	case e.t1 == 0:
		// Write delta of timestamp.
		tDelta = uint64(point.Timestamp - e.t0)

		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutUvarint(buf, tDelta)] {
			e.buf.writeByte(b)
		}
		// Write value delta.
		e.writeVDelta(point.Value)
		e.t1 = point.Timestamp
	default:
		// Write delta-of-delta of timestamp.
		tDelta = uint64(point.Timestamp - e.t)
		deltaOfDelta := int64(tDelta - e.tDelta)
		switch {
		case deltaOfDelta == 0:
			e.buf.writeBit(zero)
		case -63 <= deltaOfDelta && deltaOfDelta <= 64:
			e.buf.writeBits(0x02, 2) // '10'
			e.buf.writeBits(uint64(deltaOfDelta), 7)
		case -255 <= deltaOfDelta && deltaOfDelta <= 256:
			e.buf.writeBits(0x06, 3) // '110'
			e.buf.writeBits(uint64(deltaOfDelta), 9)
		case -2047 <= deltaOfDelta && deltaOfDelta <= 2048:
			e.buf.writeBits(0x0e, 4) // '1110'
			e.buf.writeBits(uint64(deltaOfDelta), 12)
		default:
			e.buf.writeBits(0x0f, 4) // '1111'
			e.buf.writeBits(uint64(deltaOfDelta), 64)
		}
		// Write value delta.
		e.writeVDelta(point.Value)
	}

	e.t = point.Timestamp
	e.v = point.Value
	e.tDelta = tDelta
	return nil
}

// flush writes the buffered-bytes into the backend io.Writer
// and resets everything used for computation.
func (e *gorillaEncoder) flush() error {
	// TODO: Compress with ZStandard
	_, err := e.w.Write(e.buf.bytes())
	if err != nil {
		return fmt.Errorf("failed to flush buffered bytes: %w", err)
	}

	e.buf.reset()
	e.t0 = 0
	e.t1 = 0
	e.t = 0
	e.tDelta = 0
	e.v = 0
	e.v = 0
	e.leading = 0
	e.trailing = 0

	return nil
}

func (e *gorillaEncoder) writeVDelta(v float64) {
	vDelta := math.Float64bits(v) ^ math.Float64bits(e.v)

	if vDelta == 0 {
		e.buf.writeBit(zero)
		return
	}
	e.buf.writeBit(one)

	leading := uint8(bits.LeadingZeros64(vDelta))
	trailing := uint8(bits.TrailingZeros64(vDelta))

	// Clamp number of leading zeros to avoid overflow when encoding.
	if leading >= 32 {
		leading = 31
	}

	if e.leading != 0xff && leading >= e.leading && trailing >= e.trailing {
		e.buf.writeBit(zero)
		e.buf.writeBits(vDelta>>e.trailing, 64-int(e.leading)-int(e.trailing))
	} else {
		e.leading, e.trailing = leading, trailing

		e.buf.writeBit(one)
		e.buf.writeBits(uint64(leading), 5)

		// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
		// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
		// So instead we write out a 0 and adjust it back to 64 on unpacking.
		sigbits := 64 - leading - trailing
		e.buf.writeBits(uint64(sigbits), 6)
		e.buf.writeBits(vDelta>>trailing, int(sigbits))
	}
}

type seriesDecoder interface {
	decodePoint(dst *DataPoint) error
}

// newSeriesDecoder decompress data from the given Reader, then holds the decompressed data
func newSeriesDecoder(r io.Reader) (seriesDecoder, error) {
	// TODO: Stop copying entire bytes, then make it possible to to make bstreamReader from io.Reader
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read all bytes: %w", err)
	}
	return &gorillaDecoder{
		br: newBReader(b),
	}, nil
}

type gorillaDecoder struct {
	br      bstreamReader
	numRead uint16

	// timestamp of the Nth data point
	t      int64
	tDelta uint64

	// value of the Nth data point
	v        float64
	leading  uint8
	trailing uint8
}

func (d *gorillaDecoder) decodePoint(dst *DataPoint) error {
	if d.numRead == 0 {
		t, err := binary.ReadVarint(&d.br)
		if err != nil {
			return fmt.Errorf("failed to read Timestamp of T0: %w", err)
		}
		v, err := d.br.readBits(64)
		if err != nil {
			return fmt.Errorf("failed to read Value of T0: %w", err)
		}
		d.t = t
		d.v = math.Float64frombits(v)

		d.numRead++
		dst.Timestamp = d.t
		dst.Value = d.v
		return nil
	}
	if d.numRead == 1 {
		tDelta, err := binary.ReadUvarint(&d.br)
		if err != nil {
			return err
		}
		d.tDelta = tDelta
		d.t = d.t + int64(d.tDelta)

		if err := d.readValue(); err != nil {
			return err
		}
		d.numRead++
		dst.Timestamp = d.t
		dst.Value = d.v
		return nil
	}

	var delimiter byte
	// read delta-of-delta
	for i := 0; i < 4; i++ {
		delimiter <<= 1
		bit, err := d.br.readBitFast()
		if err != nil {
			bit, err = d.br.readBit()
		}
		if err != nil {
			return err
		}
		if bit == zero {
			break
		}
		delimiter |= 1
	}
	var sz uint8
	var deltaOfDelta int64
	switch delimiter {
	case 0x00:
		// deltaOfDelta == 0
	case 0x02:
		sz = 7
	case 0x06:
		sz = 9
	case 0x0e:
		sz = 12
	case 0x0f:
		// Do not use fast because d's very unlikely d will succeed.
		bits, err := d.br.readBits(64)
		if err != nil {
			return err
		}

		deltaOfDelta = int64(bits)
	default:
		return fmt.Errorf("unknown delimiter found: %v", delimiter)
	}

	if sz != 0 {
		bits, err := d.br.readBitsFast(sz)
		if err != nil {
			bits, err = d.br.readBits(sz)
		}
		if err != nil {
			return err
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		deltaOfDelta = int64(bits)
	}

	d.tDelta = uint64(int64(d.tDelta) + deltaOfDelta)
	d.t = d.t + int64(d.tDelta)

	if err := d.readValue(); err != nil {
		return err
	}
	dst.Timestamp = d.t
	dst.Value = d.v
	return nil
}

func (d *gorillaDecoder) readValue() error {
	bit, err := d.br.readBitFast()
	if err != nil {
		bit, err = d.br.readBit()
	}
	if err != nil {
		return err
	}

	if bit == zero {
		// d.val = d.val
	} else {
		bit, err := d.br.readBitFast()
		if err != nil {
			bit, err = d.br.readBit()
		}
		if err != nil {
			return err
		}
		if bit == zero {
			// reuse leading/trailing zero bits
			// d.leading, d.trailing = d.leading, d.trailing
		} else {
			bits, err := d.br.readBitsFast(5)
			if err != nil {
				bits, err = d.br.readBits(5)
			}
			if err != nil {
				return err
			}
			d.leading = uint8(bits)

			bits, err = d.br.readBitsFast(6)
			if err != nil {
				bits, err = d.br.readBits(6)
			}
			if err != nil {
				return err
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			d.trailing = 64 - d.leading - mbits
		}

		mbits := 64 - d.leading - d.trailing
		bits, err := d.br.readBitsFast(mbits)
		if err != nil {
			bits, err = d.br.readBits(mbits)
		}
		if err != nil {
			return err
		}
		vbits := math.Float64bits(d.v)
		vbits ^= bits << d.trailing
		d.v = math.Float64frombits(vbits)
	}

	return nil
}

func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}
