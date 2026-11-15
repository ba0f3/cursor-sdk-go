package agentwire

import (
	"bytes"
	"testing"
)

func TestImageIsEncodedInSelectedContext(t *testing.T) {
	payload := EncodeRunRequest(RunInput{UserText: "describe", Images: []InputImage{{Data: []byte{1, 2, 3}, MimeType: "image/png"}}})
	navigate := func(data []byte, field int) []byte {
		t.Helper()
		fields, err := decodeFields(data)
		if err != nil {
			t.Fatal(err)
		}
		value, ok := findField(fields, field)
		if !ok {
			t.Fatalf("missing field %d", field)
		}
		return value.Bytes
	}
	run := navigate(payload, 1)
	fields, err := decodeFields(run)
	if err != nil {
		t.Fatal(err)
	}
	capability, ok := findField(fields, 19)
	if !ok || capability.Varint != 1 {
		t.Fatal("inline image support was not advertised to Cursor")
	}
	action := navigate(run, 2)
	userAction := navigate(navigate(action, 1), 1)
	context := navigate(userAction, 3)
	image := navigate(context, 1)
	if got := navigate(image, 7); string(got) != "image/png" {
		t.Fatalf("mime %q", got)
	}
	if got := navigate(image, 8); !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("image bytes %v", got)
	}
}
