// Package agentwire implements a minimal protobuf wire codec for Cursor agent.v1 Run.
// Field layout follows the OmniRoute cursorAgentProtobuf reference (MIT).
package agentwire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	wireVarint = 0
	wire64Bit  = 1
	wireLength = 2
	wire32Bit  = 5
)

/**
 * @brief One decoded protobuf field on the wire.
 */
type Field struct {
	Number   int
	WireType int
	Varint   uint64
	Bytes    []byte
}

func encodeVarint(v uint64) []byte {
	if v == 0 {
		return []byte{0}
	}
	var buf []byte
	for v > 0x7f {
		buf = append(buf, byte(v&0x7f|0x80))
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

func encodeTag(fieldNumber int, wireType int) []byte {
	return encodeVarint(uint64((fieldNumber << 3) | wireType))
}

func encodeBytes(fieldNumber int, value []byte) []byte {
	out := encodeTag(fieldNumber, wireLength)
	out = append(out, encodeVarint(uint64(len(value)))...)
	out = append(out, value...)
	return out
}

func encodeString(fieldNumber int, value string) []byte {
	return encodeBytes(fieldNumber, []byte(value))
}

func encodeMessage(fieldNumber int, parts ...[]byte) []byte {
	var inner []byte
	for _, part := range parts {
		inner = append(inner, part...)
	}
	return encodeBytes(fieldNumber, inner)
}

/**
 * @brief Encode the empty AgentClientMessage client heartbeat.
 * @return Complete Connect data frame matching Cursor's client_heartbeat field.
 */
func EncodeClientHeartbeat() []byte {
	return EncodeConnectDataFrame(encodeMessage(7))
}

/**
 * @brief Encode an exec stream-close message for one completed operation.
 * @param id Cursor exec operation identifier.
 * @return Complete Connect data frame.
 */
func EncodeExecStreamClose(id uint32) []byte {
	return EncodeConnectDataFrame(encodeMessage(5, encodeMessage(1, encodeUInt32Field(1, id))))
}

/**
 * @brief Encode a heartbeat for one active exec operation.
 * @param id Cursor exec operation identifier.
 * @return Complete Connect data frame.
 */
func EncodeExecHeartbeat(id uint32) []byte {
	return EncodeConnectDataFrame(encodeMessage(5, encodeMessage(3, encodeUInt32Field(1, id))))
}

/**
 * @brief Encode a failure for an unsupported native exec operation.
 * @param id Cursor exec operation identifier.
 * @param message Public diagnostic for the upstream agent.
 * @return Complete Connect data frame.
 */
func EncodeExecThrow(id uint32, message string) []byte {
	return EncodeConnectDataFrame(encodeMessage(5, encodeMessage(2, encodeUInt32Field(1, id), encodeString(2, message))))
}

func encodeUInt32Field(fieldNumber int, value uint32) []byte {
	out := encodeTag(fieldNumber, wireVarint)
	out = append(out, encodeVarint(uint64(value))...)
	return out
}

func decodeVarint(buf []byte, offset int) (uint64, int, error) {
	var result uint64
	var shift uint
	pos := offset
	for pos < len(buf) {
		b := buf[pos]
		pos++
		if shift == 63 && b > 1 {
			return 0, offset, errors.New("varint overflows uint64")
		}
		result |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return result, pos, nil
		}
		shift += 7
		if shift > 64 {
			break
		}
	}
	return 0, offset, errors.New("varint truncated")
}

func checkedLen(length uint64, pos, buflen int) (int, error) {
	if length > uint64(math.MaxInt) || int(length) > buflen-pos {
		return 0, fmt.Errorf("length-delimited field overruns buffer (len=%d, remaining=%d)", length, buflen-pos)
	}
	return int(length), nil
}

/**
 * @brief Decode all fields from a protobuf message buffer.
 */
func decodeFields(buf []byte) ([]Field, error) {
	var fields []Field
	pos := 0
	for pos < len(buf) {
		tag, next, err := decodeVarint(buf, pos)
		if err != nil {
			return nil, err
		}
		pos = next
		if tag>>3 == 0 || tag>>3 > (1<<29)-1 {
			return nil, errors.New("invalid protobuf field number")
		}
		fieldNumber := int(tag >> 3)
		wireType := int(tag & 0x7)
		switch wireType {
		case wireVarint:
			v, np, err := decodeVarint(buf, pos)
			if err != nil {
				return nil, err
			}
			pos = np
			fields = append(fields, Field{Number: fieldNumber, WireType: wireType, Varint: v})
		case wireLength:
			length, np, err := decodeVarint(buf, pos)
			if err != nil {
				return nil, err
			}
			pos = np
			ln, err := checkedLen(length, pos, len(buf))
			if err != nil {
				return nil, err
			}
			fields = append(fields, Field{Number: fieldNumber, WireType: wireType, Bytes: append([]byte(nil), buf[pos:pos+ln]...)})
			pos += ln
		case wire32Bit:
			if len(buf)-pos < 4 {
				return nil, errors.New("fixed32 field overruns buffer")
			}
			pos += 4
		case wire64Bit:
			if len(buf)-pos < 8 {
				return nil, errors.New("fixed64 field overruns buffer")
			}
			pos += 8
		default:
			return nil, fmt.Errorf("unsupported wire type %d", wireType)
		}
	}
	return fields, nil
}

func findField(fields []Field, number int) (Field, bool) {
	for _, f := range fields {
		if f.Number == number {
			return f, true
		}
	}
	return Field{}, false
}

func decodeStringField(buf []byte, number int) string {
	f, ok := findField(decodeAll(buf), number)
	if !ok || f.WireType != wireLength {
		return ""
	}
	return string(f.Bytes)
}

func decodeAll(buf []byte) []Field {
	fields, err := decodeFields(buf)
	if err != nil {
		return nil
	}
	return fields
}

// EncodeConnectDataFrame prefixes a Connect binary data frame.
func EncodeConnectDataFrame(payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)))
	copy(out[5:], payload)
	return out
}

// DecodeFieldsExported exposes wire decoding to cursorapi.
func DecodeFieldsExported(buf []byte) ([]Field, error) {
	return decodeFields(buf)
}

// FindFieldExported looks up a decoded protobuf field.
func FindFieldExported(fields []Field, number int) (Field, bool) {
	return findField(fields, number)
}
