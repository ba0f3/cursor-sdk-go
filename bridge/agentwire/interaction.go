package agentwire

import (
	"fmt"
	"math"
)

/**
 * @brief Answer all pinned interaction variants using the official local SDK policy.
 * @param payload One upstream AgentServerMessage.
 * @return Framed interaction response, or nil for unsupported queries and other messages.
 * @return Error for malformed interaction envelopes or identifiers.
 * @details Cursor executes the search remotely; this does not execute a host or client tool.
 */
func ReplyInteraction(payload []byte) ([]byte, error) {
	top, ok, err := nativeAgentMessage(payload)
	if err != nil || !ok || top.Number != 7 {
		return nil, err
	}
	fields, err := nativeBoundaryFields(top.Bytes)
	if err != nil {
		return nil, err
	}
	var id uint32
	for _, field := range fields {
		if field.Number == 1 {
			if field.WireType != wireVarint || field.Varint > math.MaxUint32 {
				return nil, fmt.Errorf("invalid cursor interaction identifier")
			}
			id = uint32(field.Varint)
		}
	}
	query, ok, err := nativeMessageOneof(fields, nativeQueryMember)
	if err != nil || !ok {
		return nil, err
	}
	if _, err := nativeBoundaryFields(query.Bytes); err != nil {
		return nil, err
	}
	var result []byte
	switch query.Number {
	case 2, 4, 8, 9:
		result = encodeMessage(1)
	case 3:
		result = encodeMessage(1, encodeMessage(3, encodeString(1, "Interactive questions are not supported in local SDK runs")))
	case 7:
		result = encodeMessage(1, encodeMessage(1))
	case 10:
		result = encodeMessage(2, encodeString(1, "PR management is only available in cloud agents"))
	case 11:
		result = encodeMessage(2, encodeString(1, "MCP authentication is not supported in local SDK runs"))
	case 12:
		fields, _ := nativeBoundaryFields(query.Bytes)
		args, ok, err := nativeSingularMessage(fields, 1)
		if err != nil {
			return nil, err
		}
		description := ""
		if ok {
			if args.WireType != wireLength {
				return nil, fmt.Errorf("invalid cursor image generation arguments wire type: %d", args.WireType)
			}
			fields, err := nativeBoundaryFields(args.Bytes)
			if err != nil {
				return nil, err
			}
			if value, ok := lastSDKField(fields, 1); ok {
				if value.WireType != wireLength {
					return nil, fmt.Errorf("invalid cursor image description wire type: %d", value.WireType)
				}
				description = string(value.Bytes)
			}
		}
		result = encodeMessage(1, encodeString(1, description))
	case 13:
		result = encodeMessage(2, encodeString(1, "Environment replacement is not supported in local SDK runs"))
	case 14:
		result = encodeMessage(2, encodeString(1, "Connecting GitHub is not supported in local SDK runs"))
	}
	response := encodeMessage(6, encodeUInt32Field(1, id), encodeMessage(query.Number, result))
	return EncodeConnectDataFrame(response), nil
}
