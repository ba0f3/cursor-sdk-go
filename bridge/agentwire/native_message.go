package agentwire

import (
	"errors"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
)

/**
 * @brief Decode a bounded native boundary without copying every nested payload before selection.
 * @param data One native protobuf message, borrowed for this decoder invocation.
 * @return Ordered fields whose byte values borrow data; callers clone bytes they retain.
 * @return Size/count exhaustion, malformed protobuf or unsupported group error.
 */
func nativeBoundaryFields(data []byte) ([]Field, error) {
	if len(data) > 8<<20 {
		return nil, errors.New("cursor native message exceeds size limit")
	}
	var fields []Field
	for len(data) > 0 {
		if len(fields) >= 100000 {
			return nil, errors.New("cursor native message exceeds field limit")
		}
		number, wire, used := protowire.ConsumeTag(data)
		if used < 0 {
			return nil, errors.New("invalid cursor native field tag")
		}
		data = data[used:]
		field := Field{Number: int(number), WireType: int(wire)}
		switch wire {
		case protowire.VarintType:
			field.Varint, used = protowire.ConsumeVarint(data)
		case protowire.BytesType:
			field.Bytes, used = protowire.ConsumeBytes(data)
		case protowire.Fixed32Type:
			_, used = protowire.ConsumeFixed32(data)
		case protowire.Fixed64Type:
			_, used = protowire.ConsumeFixed64(data)
		default:
			return nil, errors.New("unsupported cursor native field wire type")
		}
		if used < 0 {
			return nil, errors.New("truncated cursor native field")
		}
		data = data[used:]
		fields = append(fields, field)
	}
	return fields, nil
}

/**
 * @brief Select a protobuf message oneof in linear time, merging repeated occurrences of its active member.
 * @param fields Ordered native fields.
 * @param member Predicate identifying only known members of this oneof, excluding independent metadata.
 * @return Active member with owned merged bytes; empty when absent.
 * @return Whether a recognized member was present, including an empty message.
 * @return Wrong known-member wire type or merged size/count limit error.
 * @note Unknown fields never replace a known active member. Different known members discard earlier bytes.
 */
func nativeMessageOneof(fields []Field, member func(int) bool) (Field, bool, error) {
	if len(fields) > 100000 {
		return Field{}, false, errors.New("cursor native message exceeds field limit")
	}
	var active Field
	var merged []byte
	found := false
	for _, field := range fields {
		if !member(field.Number) {
			continue
		}
		if field.WireType != wireLength {
			return Field{}, false, errors.New("invalid cursor native message member wire type")
		}
		if !found || active.Number != field.Number {
			merged = merged[:0]
		}
		if len(merged)+len(field.Bytes) > 8<<20 {
			return Field{}, false, errors.New("cursor native merged message exceeds size limit")
		}
		merged = append(merged, field.Bytes...)
		active, found = field, true
	}
	active.Bytes = merged
	return active, found, nil
}

/**
 * @brief Identify known AgentServerMessage message-oneof tags from the pinned descriptor.
 * @param number Native field number.
 * @return Whether the field is a message-oneof member; TTFT metadata tag 8 is independent.
 */
func nativeAgentMember(number int) bool {
	return number == 1 || number == 2 || number == 3 || number == 4 || number == 5 || number == 7
}

/**
 * @brief Identify pinned exec operation tags without treating correlation metadata as operations.
 * @param number Native field number.
 * @return Whether the field belongs to the pinned 33-member exec operation oneof.
 */
func nativeExecMember(number int) bool { _, ok := nativeExecVariants[number]; return ok }

/**
 * @brief Identify the native KV request oneof.
 * @param number Native field number.
 * @return Whether this is get_blob_args or set_blob_args.
 */
func nativeKVMember(number int) bool { return number == 2 || number == 3 }

/**
 * @brief Identify known interaction updates, excluding independent timestamp metadata and reserved tags.
 * @param number Native field number.
 * @return Whether the field belongs to InteractionUpdate.message.
 */
func nativeInteractionMember(number int) bool {
	return number > 0 && number <= 30 && number != 18 && number != 25 && number != 29
}

/**
 * @brief Identify the pinned interaction query oneof.
 * @param number Native field number.
 * @return Whether the query is a known message member.
 */
func nativeQueryMember(number int) bool {
	return number >= 2 && number <= 14 && number != 5 && number != 6
}

/**
 * @brief Select the canonical active outer message before interpreting any operation or callback.
 * @param payload Native AgentServerMessage bytes.
 * @return Active outer message with merged bytes.
 * @return Presence flag, including empty selected messages.
 * @return Malformed envelope, wrong known-member type or decoding-limit error.
 */
func nativeAgentMessage(payload []byte) (Field, bool, error) {
	fields, err := nativeBoundaryFields(payload)
	if err != nil {
		return Field{}, false, err
	}
	for _, field := range fields {
		if field.Number == 8 && field.WireType != wireLength {
			return Field{}, false, errors.New("invalid cursor native TTFT metadata wire type")
		}
	}
	return nativeMessageOneof(fields, nativeAgentMember)
}

/**
 * @brief Merge one ordinary singular message field without applying unrelated oneof replacement rules.
 * @param fields Decoded message fields.
 * @param number Singular message field identity.
 * @return Merged message and presence flag.
 * @return Wrong known wire type or merged size limit error.
 */
func nativeSingularMessage(fields []Field, number int) (Field, bool, error) {
	return nativeMessageOneof(fields, func(candidate int) bool { return candidate == number })
}

/**
 * @brief Validate exec metadata and select the sole active operation before authorizing callbacks or replies.
 * @param data Active ExecServerMessage bytes.
 * @return Ordered exec fields, preserving last scalar values.
 * @return Active operation with merged argument bytes, or an empty field when absent.
 * @return Malformed fields, wrong known metadata/member wire type or invalid identifier text.
 */
func nativeExecOperation(data []byte) ([]Field, Field, error) {
	fields, err := nativeBoundaryFields(data)
	if err != nil {
		return nil, Field{}, err
	}
	for _, field := range fields {
		switch field.Number {
		case 1, 55:
			if field.WireType != wireVarint {
				return nil, Field{}, errors.New("invalid cursor native exec numeric wire type")
			}
		case 15, 19, 57:
			if field.WireType != wireLength {
				return nil, Field{}, errors.New("invalid cursor native exec metadata wire type")
			}
			if field.Number != 19 && !utf8.Valid(field.Bytes) {
				return nil, Field{}, errors.New("invalid cursor native exec identifier UTF-8")
			}
		}
	}
	operation, _, err := nativeMessageOneof(fields, nativeExecMember)
	return fields, operation, err
}

/**
 * @brief Validate KV envelope metadata and select the active get/set request with message merging.
 * @param data Active KVServerMessage bytes.
 * @return Ordered KV envelope fields.
 * @return Active get or set arguments, or an empty field when absent.
 * @return Malformed metadata, wrong known field type or decoding-limit error.
 */
func nativeKVOperation(data []byte) ([]Field, Field, error) {
	fields, err := nativeBoundaryFields(data)
	if err != nil {
		return nil, Field{}, err
	}
	for _, field := range fields {
		if (field.Number == 1 && field.WireType != wireVarint) || (field.Number == 4 && field.WireType != wireLength) {
			return nil, Field{}, errors.New("invalid cursor native KV metadata wire type")
		}
	}
	operation, _, err := nativeMessageOneof(fields, nativeKVMember)
	return fields, operation, err
}
