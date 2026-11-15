package agentwire

import (
	"bytes"
	"testing"
)

/**
 * @brief Reject truncated fixed-width fields and overflowing protobuf varints.
 * @param t Test harness.
 */
func TestDecodeFieldsRejectsMalformedWireData(t *testing.T) {
	cases := map[string][]byte{
		"truncated fixed32":      {0x0d, 1, 2, 3},
		"truncated fixed64":      {0x09, 1, 2, 3, 4, 5, 6, 7},
		"overflowing varint":     append([]byte{0x08}, append(bytes.Repeat([]byte{0xff}, 9), 0x02)...),
		"zero field number":      {0x00, 0x01},
		"excessive field number": {0x80, 0x80, 0x80, 0x80, 0x10, 0x01},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeFields(data); err == nil {
				t.Fatal("malformed protobuf was accepted")
			}
		})
	}
}

/**
 * @brief Accept the largest valid protobuf unsigned varint.
 * @param t Test harness.
 */
func TestDecodeFieldsAcceptsMaxUint64(t *testing.T) {
	data := append([]byte{0x08}, append(bytes.Repeat([]byte{0xff}, 9), 0x01)...)
	fields, err := decodeFields(data)
	if err != nil || len(fields) != 1 || fields[0].Varint != ^uint64(0) {
		t.Fatalf("valid maximum varint was rejected: %v", err)
	}
}

/**
 * @brief Skip complete unknown fixed-width fields while retaining known fields.
 * @param t Test harness.
 */
func TestDecodeFieldsSkipsCompleteFixedWidthFields(t *testing.T) {
	data := []byte{0x0d, 1, 2, 3, 4, 0x11, 1, 2, 3, 4, 5, 6, 7, 8, 0x18, 42}
	fields, err := decodeFields(data)
	if err != nil {
		t.Fatal(err)
	}
	field, ok := findField(fields, 3)
	if !ok || field.Varint != 42 {
		t.Fatal("known field following fixed-width fields was lost")
	}
}
