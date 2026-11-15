package agentwire

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

/**
 * @brief Tool describes one client-owned MCP tool.
 */
type Tool struct {
	Name        string ///< Client tool name.
	Description string ///< Client tool purpose.
	Schema      []byte ///< Protobuf Value containing the JSON input schema.
}

/**
 * @brief Encode the native MCP tool declaration envelope.
 * @param tools Client-declared tools in stable order.
 * @return Field 4 of AgentRunRequest, including an empty envelope when no tools exist.
 */
func EncodeTools(tools []Tool) []byte {
	var entries [][]byte
	for _, t := range tools {
		entries = append(entries, encodeMessage(1, encodeString(1, t.Name), encodeString(2, t.Description), encodeBytes(3, t.Schema), encodeString(4, "ccload"), encodeString(5, t.Name)))
	}
	return encodeMessage(4, entries...)
}

/**
 * @brief ToolCall preserves the exec correlation IDs returned by Cursor.
 */
type ToolCall struct {
	ID     uint32           ///< Exec message correlation number.
	ExecID string           ///< Native execution identifier.
	Name   string           ///< Client-declared tool name.
	CallID string           ///< Tool call ID returned to the downstream client.
	Args   *structpb.Struct ///< Typed JSON arguments.
}

/**
 * @brief Parse native MCP invocations; assistant text never becomes executable.
 * @param payload One AgentServerMessage protobuf payload.
 * @return Correlated tool call, or nil when this frame contains no MCP call.
 * @return Malformed protobuf or argument error; nil on success.
 */
func DecodeToolCall(payload []byte) (*ToolCall, error) {
	top, ok, err := nativeAgentMessage(payload)
	if err != nil || !ok || top.Number != 2 {
		return nil, err
	}
	entries, operation, err := nativeExecOperation(top.Bytes)
	if err != nil || operation.Number != 11 {
		return nil, err
	}
	sdkMetadataSchemaOnce.Do(initSDKMetadataSchema)
	budget := 100000
	args, err := decodeSDKMetadata("McpArgs", operation.Bytes, 0, &budget)
	if err != nil {
		return nil, fmt.Errorf("decode native MCP arguments: %w", err)
	}
	call := &ToolCall{Args: &structpb.Struct{Fields: map[string]*structpb.Value{}}, Name: args.text("name"), CallID: args.text("toolCallId")}
	if args.present["toolName"] {
		call.Name = args.text("toolName")
	}
	if id, ok := lastSDKField(entries, 1); ok {
		call.ID = uint32(id.Varint)
	}
	if id, ok := lastSDKField(entries, 15); ok {
		call.ExecID = string(id.Bytes)
	}
	for key, raw := range args.values["args"].(map[string]any) {
		value := raw.(*sdkMetadataMessage).wellKnown.(*structpb.Value)
		if err := validateSDKMetadataValue(value); err != nil {
			return nil, fmt.Errorf("decode tool argument: %w", err)
		}
		call.Args.Fields[key] = value
	}
	return call, nil
}

/**
 * @brief Return typed client tool content on the originating exec channel.
 * @param call Native correlation IDs of the pending invocation.
 * @param value Client result object, optionally containing MCP content blocks.
 * @param failed Whether the tool reported an execution error.
 * @return Framed ExecClientMessage, or nil for invalid content.
 * @return Invalid image, unsupported content or JSON encoding error; nil on success.
 */
func EncodeToolResult(call *ToolCall, value map[string]any, failed bool) ([]byte, error) {
	var contents [][]byte
	if blocks, ok := value["content"].([]any); ok {
		for _, item := range blocks {
			block, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid tool content block")
			}
			kind, _ := block["type"].(string)
			switch kind {
			case "text", "input_text":
				text, _ := block["text"].(string)
				contents = append(contents, encodeMessage(1, encodeMessage(1, encodeString(1, text))))
			case "image", "image_url", "input_image":
				data, _ := block["data"].(string)
				mime, _ := block["mimeType"].(string)
				if source, ok := block["source"].(map[string]any); ok {
					data, _ = source["data"].(string)
					mime, _ = source["media_type"].(string)
				}
				if kind != "image" {
					uri, _ := block["image_url"].(string)
					if obj, ok := block["image_url"].(map[string]any); ok {
						uri, _ = obj["url"].(string)
					}
					head, body, ok := strings.Cut(uri, ",")
					if !ok || !strings.HasPrefix(head, "data:") || !strings.HasSuffix(head, ";base64") {
						return nil, fmt.Errorf("tool image requires inline base64 data")
					}
					data = body
					mime = strings.TrimSuffix(strings.TrimPrefix(head, "data:"), ";base64")
				}
				if !strings.HasPrefix(mime, "image/") || len(data) > base64.StdEncoding.EncodedLen(1<<20) {
					return nil, fmt.Errorf("invalid tool image MIME type or size")
				}
				decoded, err := base64.StdEncoding.DecodeString(data)
				if err != nil || len(decoded) == 0 || len(decoded) > 1<<20 {
					return nil, fmt.Errorf("invalid tool image data")
				}
				contents = append(contents, encodeMessage(1, encodeMessage(2, encodeBytes(1, decoded), encodeString(2, mime))))
			default:
				return nil, fmt.Errorf("unsupported tool content type %q", kind)
			}
		}
	} else {
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		contents = append(contents, encodeMessage(1, encodeMessage(1, encodeString(1, string(data)))))
	}
	if failed {
		contents = append(contents, encodeUInt32Field(2, 1))
	}
	for _, extra := range []struct {
		key   string
		field int
	}{{"structuredContent", 3}, {"_meta", 5}} {
		object, ok := value[extra.key].(map[string]any)
		if !ok {
			continue
		}
		structured, err := structpb.NewStruct(object)
		if err != nil {
			return nil, fmt.Errorf("invalid tool %s: %w", extra.key, err)
		}
		encoded, err := proto.Marshal(structured)
		if err != nil {
			return nil, fmt.Errorf("encode tool %s: %w", extra.key, err)
		}
		contents = append(contents, encodeBytes(extra.field, encoded))
	}
	result := encodeMessage(1, contents...)
	return EncodeConnectDataFrame(encodeMessage(2, encodeUInt32Field(1, call.ID), encodeString(15, call.ExecID), encodeMessage(11, result))), nil
}
