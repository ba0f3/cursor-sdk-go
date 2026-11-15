package agentwire

import (
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

/**
 * @brief Native signed 64-bit scalar whose pinned JavaScript representation is BigInt.
 */
type sdkMetadataInt64 int64

/**
 * @brief Native unsigned 64-bit scalar whose pinned JavaScript representation is BigInt.
 */
type sdkMetadataUint64 uint64

/**
 * @brief Descriptor-derived protobuf field used only for SDK metadata conversion.
 */
type sdkMetadataField struct {
	number   int    ///< Native field tag.
	name     string ///< Camel-case JavaScript property name.
	kind     string ///< Scalar number or referenced symbol with a leading hash.
	group    string ///< Oneof property, absent for ordinary fields.
	optional bool   ///< Explicit scalar presence.
	repeated bool   ///< Repeated field, including packed scalars.
	mapKey   string ///< Map key scalar type, absent for ordinary fields.
}

/**
 * @brief Detached native metadata with protobuf constructor defaults and active oneof members.
 */
type sdkMetadataMessage struct {
	symbol    string            ///< Descriptor identity, preserving same-name native types.
	values    map[string]any    ///< Defaults plus present message, optional and oneof values.
	groups    map[string]string ///< Active member name for each protobuf oneof.
	special   any               ///< Decoded google.protobuf.Value or Struct JSON value.
	present   map[string]bool   ///< Fields actually encountered, needed for singular-message merge semantics.
	wellKnown proto.Message     ///< Parsed Value/Struct used when duplicate message fields merge.
}

/**
 * @brief Shared immutable descriptor cache, initialized from generated trusted data.
 */
var sdkMetadataSchemaOnce sync.Once

/**
 * @brief Parsed immutable field declarations published by sdkMetadataSchemaOnce.
 */
var sdkMetadataSchema map[string][]sdkMetadataField

/**
 * @brief Convert a protobuf snake-case identifier to its default JSON and JS property name.
 * @param name Native identifier.
 * @return Camel-case property name.
 */
func sdkMetadataCamel(name string) string {
	parts := strings.Split(name, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

/**
 * @brief Materialize the generated trusted descriptor field inventory once.
 */
func initSDKMetadataSchema() {
	sdkMetadataSchema = make(map[string][]sdkMetadataField, len(sdkToolMetadataDescriptors))
	for symbol, descriptor := range sdkToolMetadataDescriptors {
		sdkMetadataSchema[symbol] = nil
		for _, part := range strings.Split(descriptor, "|")[1:] {
			tokens := strings.Fields(part)
			number, _ := strconv.Atoi(tokens[0])
			kind := tokens[2]
			field := sdkMetadataField{number: number, name: sdkMetadataCamel(tokens[1]), kind: kind}
			if len(tokens) > 3 {
				field.group = tokens[3]
			}
			if strings.HasSuffix(kind, "?") || strings.HasSuffix(kind, "*") {
				field.optional = strings.HasSuffix(kind, "?")
				field.repeated = strings.HasSuffix(kind, "*")
				field.kind = kind[:len(kind)-1]
				field.group = ""
			}
			if strings.HasPrefix(field.group, "=") {
				field.group = ""
			}
			if n := strings.Index(field.kind, ","); n >= 0 {
				field.mapKey = field.kind[:n]
				field.kind = field.kind[n+1:]
				field.group = ""
			}
			sdkMetadataSchema[symbol] = append(sdkMetadataSchema[symbol], field)
		}
	}
}

/**
 * @brief Return the constructor default for a trusted scalar or enum field.
 * @param kind Descriptor scalar number or enum symbol.
 * @return Scalar default; nil for absent message references.
 */
func sdkMetadataDefault(kind string) any {
	if strings.HasPrefix(kind, "#") {
		if _, ok := sdkToolMetadataEnums[kind[1:]]; ok {
			return int64(0)
		}
		return nil
	}
	switch kind {
	case "9":
		return ""
	case "12":
		return []byte{}
	case "8":
		return false
	case "1", "2":
		return float64(0)
	case "4", "6":
		return sdkMetadataUint64(0)
	case "3", "16", "18":
		return sdkMetadataInt64(0)
	case "7", "13":
		return uint64(0)
	default:
		return int64(0)
	}
}

/**
 * @brief Decode bounded protobuf metadata without credentials, host services or execution side effects.
 * @param payload Native ToolCall bytes, independent of thinking/interaction envelopes.
 * @return Struct-safe normalized SDK metadata object, or nil for official null variants or missing arguments.
 * @return Malformed protobuf, aggregate/depth limit or unsupported recording-mode error.
 */
func DecodeSDKToolCall(payload []byte) (map[string]any, error) {
	if len(payload) > 8<<20 {
		return nil, errors.New("cursor SDK tool metadata exceeds size limit")
	}
	sdkMetadataSchemaOnce.Do(initSDKMetadataSchema)
	budget := 100000
	native, err := decodeSDKMetadata("ToolCall", payload, 0, &budget)
	if err != nil {
		return nil, err
	}
	name := native.groups["tool"]
	if name == "" {
		return nil, nil
	}
	var family string
	for _, field := range sdkMetadataSchema["ToolCall"] {
		if field.name == name {
			family = sdkToolMetadataVariants[field.number]
			break
		}
	}
	if family == "" {
		return nil, nil
	}
	call := native.message(name)
	args := call.message("args")
	if args == nil {
		return nil, nil
	}
	normalizedArgs, err := sdkMetadataArgs(family, args)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"type": family, "args": normalizedArgs}
	if family == "task" {
		result := call.message("result")
		if result != nil && result.groups["result"] == "success" {
			for _, step := range result.message("success").list("conversationSteps") {
				if err := validateSDKMetadataJSON(step.(*sdkMetadataMessage)); err != nil {
					return nil, err
				}
			}
		}
	}
	if result := sdkMetadataResult(family, call.message("result")); result != nil {
		out["result"] = result
	}
	safe, err := SDKMetadataForStruct(out)
	if err != nil {
		return nil, err
	}
	return safe.(map[string]any), nil
}

/**
 * @brief Parse one schema-derived message while enforcing recursion and aggregate field bounds.
 * @param symbol Pinned message identity.
 * @param data Native protobuf bytes.
 * @param depth Current message depth.
 * @param budget Shared remaining field count.
 * @return Detached metadata node.
 * @return Malformed known field or decoding limit error.
 */
func decodeSDKMetadata(symbol string, data []byte, depth int, budget *int) (*sdkMetadataMessage, error) {
	if depth > 64 {
		return nil, errors.New("cursor SDK tool metadata exceeds depth limit")
	}
	node := &sdkMetadataMessage{symbol: symbol, values: map[string]any{}, groups: map[string]string{}, present: map[string]bool{}}
	if symbol == "Value" {
		value := &structpb.Value{}
		if err := (proto.UnmarshalOptions{RecursionLimit: 64 - depth}).Unmarshal(data, value); err != nil {
			return nil, errors.New("invalid cursor SDK metadata Value")
		}
		node.special = value.AsInterface()
		node.wellKnown = value
		if err := boundSDKMetadataJSON(node.special, depth, budget); err != nil {
			return nil, err
		}
		return node, nil
	}
	if symbol == "Struct" {
		value := &structpb.Struct{}
		if err := (proto.UnmarshalOptions{RecursionLimit: 64 - depth}).Unmarshal(data, value); err != nil {
			return nil, errors.New("invalid cursor SDK metadata Struct")
		}
		node.special = value.AsMap()
		node.wellKnown = value
		if err := boundSDKMetadataJSON(node.special, depth, budget); err != nil {
			return nil, err
		}
		return node, nil
	}
	schema, known := sdkMetadataSchema[symbol]
	if !known {
		return nil, errors.New("unknown cursor SDK metadata schema")
	}
	for _, field := range schema {
		if field.mapKey != "" {
			node.values[field.name] = map[string]any{}
		} else if field.repeated {
			node.values[field.name] = []any{}
		} else if !field.optional && field.group == "" {
			if value := sdkMetadataDefault(field.kind); value != nil {
				node.values[field.name] = value
			}
		}
	}
	for len(data) > 0 {
		*budget--
		if *budget < 0 {
			return nil, errors.New("cursor SDK tool metadata exceeds field limit")
		}
		number, wire, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, errors.New("invalid cursor SDK tool metadata tag")
		}
		data = data[n:]
		if wire == protowire.StartGroupType || wire == protowire.EndGroupType {
			return nil, errors.New("unsupported cursor SDK metadata group")
		}
		var definition *sdkMetadataField
		for i := range schema {
			if schema[i].number == int(number) {
				definition = &schema[i]
				break
			}
		}
		if definition == nil {
			n = protowire.ConsumeFieldValue(number, wire, data)
			if n < 0 {
				return nil, errors.New("invalid unknown cursor SDK metadata field")
			}
			data = data[n:]
			continue
		}
		field := *definition
		if field.mapKey != "" {
			if wire != protowire.BytesType {
				return nil, errors.New("invalid cursor SDK metadata map wire type")
			}
			entry, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return nil, errors.New("invalid cursor SDK metadata map")
			}
			data = data[size:]
			key, value, err := decodeSDKMetadataMap(field, entry, depth+1, budget)
			if err != nil {
				return nil, err
			}
			node.values[field.name].(map[string]any)[key] = value
			node.present[field.name] = true
			continue
		}
		if field.repeated && wire == protowire.BytesType && field.kind != "9" && field.kind != "12" && (!strings.HasPrefix(field.kind, "#") || sdkToolMetadataEnums[field.kind[1:]] != nil) {
			packed, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return nil, errors.New("invalid packed cursor SDK metadata")
			}
			data = data[size:]
			for len(packed) > 0 {
				*budget--
				if *budget < 0 {
					return nil, errors.New("cursor SDK tool metadata exceeds field limit")
				}
				value, used, err := decodeSDKMetadataValue(field.kind, sdkMetadataWireType(field.kind), packed, depth+1, budget)
				if err != nil {
					return nil, err
				}
				packed = packed[used:]
				node.values[field.name] = append(node.values[field.name].([]any), value)
				node.present[field.name] = true
			}
			continue
		}
		value, size, err := decodeSDKMetadataValue(field.kind, wire, data, depth+1, budget)
		if err != nil {
			return nil, err
		}
		data = data[size:]
		if field.group != "" && node.groups[field.group] != field.name {
			delete(node.values, node.groups[field.group])
			delete(node.present, node.groups[field.group])
		}
		if field.group != "" {
			node.groups[field.group] = field.name
		}
		if field.repeated {
			node.values[field.name] = append(node.values[field.name].([]any), value)
		} else {
			if next, ok := value.(*sdkMetadataMessage); ok {
				if previous, ok := node.values[field.name].(*sdkMetadataMessage); ok {
					mergeSDKMetadata(previous, next)
					value = previous
				}
			}
			node.values[field.name] = value
		}
		node.present[field.name] = true
	}
	return node, nil
}

/**
 * @brief Decode one protobuf map entry using its pinned key/value types.
 * @param field Pinned map declaration.
 * @param data Entry bytes.
 * @param depth Current message depth.
 * @param budget Shared remaining field count.
 * @return Canonical string key.
 * @return Decoded value with constructor defaults.
 * @return Invalid entry or nested value error.
 */
func decodeSDKMetadataMap(field sdkMetadataField, data []byte, depth int, budget *int) (string, any, error) {
	key := sdkMetadataDefault(field.mapKey)
	value := sdkMetadataDefault(field.kind)
	for len(data) > 0 {
		*budget--
		if *budget < 0 {
			return "", nil, errors.New("cursor SDK tool metadata exceeds field limit")
		}
		number, wire, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", nil, errors.New("invalid cursor SDK metadata map tag")
		}
		data = data[n:]
		if wire == protowire.StartGroupType || wire == protowire.EndGroupType {
			return "", nil, errors.New("unsupported cursor SDK metadata map group")
		}
		kind := field.mapKey
		if number == 2 {
			kind = field.kind
		} else if number != 1 {
			size := protowire.ConsumeFieldValue(number, wire, data)
			if size < 0 {
				return "", nil, errors.New("invalid cursor SDK metadata map field")
			}
			data = data[size:]
			continue
		}
		decoded, size, err := decodeSDKMetadataValue(kind, wire, data, depth, budget)
		if err != nil {
			return "", nil, err
		}
		data = data[size:]
		if number == 1 {
			key = decoded
		} else {
			value = decoded
		}
	}
	if value == nil && strings.HasPrefix(field.kind, "#") {
		var err error
		value, err = decodeSDKMetadata(field.kind[1:], nil, depth, budget)
		if err != nil {
			return "", nil, err
		}
	}
	return fmt.Sprint(key), value, nil
}

/**
 * @brief Determine a pinned scalar's protobuf wire type.
 * @param kind Pinned scalar number or reference.
 * @return Expected wire type.
 */
func sdkMetadataWireType(kind string) protowire.Type {
	switch kind {
	case "1", "6", "16":
		return protowire.Fixed64Type
	case "2", "7", "15":
		return protowire.Fixed32Type
	case "9", "12":
		return protowire.BytesType
	}
	if strings.HasPrefix(kind, "#") && sdkToolMetadataEnums[kind[1:]] == nil {
		return protowire.BytesType
	}
	return protowire.VarintType
}

/**
 * @brief Decode a typed scalar or nested message without guessing from its wire tag.
 * @param kind Pinned scalar number or referenced symbol.
 * @param wire Actual wire type.
 * @param data Bytes after the field tag.
 * @param depth Nested depth.
 * @param budget Shared remaining field count.
 * @return Owned typed value.
 * @return Consumed byte count.
 * @return Malformed or mismatching wire error.
 */
func decodeSDKMetadataValue(kind string, wire protowire.Type, data []byte, depth int, budget *int) (any, int, error) {
	if wire != sdkMetadataWireType(kind) {
		return nil, 0, errors.New("invalid cursor SDK metadata field wire type")
	}
	switch wire {
	case protowire.BytesType:
		value, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, 0, errors.New("truncated cursor SDK metadata bytes")
		}
		if strings.HasPrefix(kind, "#") {
			node, err := decodeSDKMetadata(kind[1:], value, depth, budget)
			return node, n, err
		}
		if kind == "9" {
			if !utf8.Valid(value) {
				return nil, 0, errors.New("invalid cursor SDK metadata UTF-8")
			}
			return string(value), n, nil
		}
		return append([]byte{}, value...), n, nil
	case protowire.Fixed64Type:
		value, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return nil, 0, errors.New("truncated cursor SDK metadata fixed64")
		}
		if kind == "1" {
			return math.Float64frombits(value), n, nil
		}
		if kind == "16" {
			return sdkMetadataInt64(value), n, nil
		}
		return sdkMetadataUint64(value), n, nil
	case protowire.Fixed32Type:
		value, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return nil, 0, errors.New("truncated cursor SDK metadata fixed32")
		}
		if kind == "2" {
			return float64(math.Float32frombits(value)), n, nil
		}
		if kind == "15" {
			return int64(int32(value)), n, nil
		}
		return uint64(value), n, nil
	default:
		value, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return nil, 0, errors.New("truncated cursor SDK metadata varint")
		}
		switch kind {
		case "8":
			return value != 0, n, nil
		case "4":
			return sdkMetadataUint64(value), n, nil
		case "13":
			return uint64(uint32(value)), n, nil
		case "17":
			return int64(int32(protowire.DecodeZigZag(value))), n, nil
		case "18":
			return sdkMetadataInt64(protowire.DecodeZigZag(value)), n, nil
		case "3":
			return sdkMetadataInt64(value), n, nil
		default:
			return int64(int32(value)), n, nil
		}
	}
}

/**
 * @brief Read a nested metadata message, accepting an absent receiver.
 * @param name Camel-case field name.
 * @return Present message, or nil.
 */
func (m *sdkMetadataMessage) message(name string) *sdkMetadataMessage {
	if m == nil {
		return nil
	}
	v, _ := m.values[name].(*sdkMetadataMessage)
	return v
}

/**
 * @brief Copy present scalar fields while retaining optional zero values.
 * @param names Camel-case field names.
 * @return Detached map containing only present fields.
 */
func (m *sdkMetadataMessage) copy(names ...string) map[string]any {
	out := map[string]any{}
	if m != nil {
		for _, name := range names {
			if value, ok := m.values[name]; ok {
				out[name] = value
			}
		}
	}
	return out
}

/**
 * @brief Read a repeated metadata field with an empty-list fallback.
 * @param name Camel-case field name.
 * @return Present list or a non-nil empty list.
 */
func (m *sdkMetadataMessage) list(name string) []any {
	if m != nil {
		if value, ok := m.values[name].([]any); ok {
			return value
		}
	}
	return []any{}
}

/**
 * @brief Read a string metadata field with its protobuf zero fallback.
 * @param name Camel-case field name.
 * @return String value, or empty for absent/non-string fields.
 */
func (m *sdkMetadataMessage) text(name string) string {
	if m == nil {
		return ""
	}
	value, _ := m.values[name].(string)
	return value
}

/**
 * @brief Read an enum metadata field using its constructor zero value.
 * @param name Camel-case field name.
 * @return Enum number, or zero.
 */
func (m *sdkMetadataMessage) enum(name string) int64 {
	if m == nil {
		return 0
	}
	value, _ := m.values[name].(int64)
	return value
}

/**
 * @brief Match JavaScript truthiness for optional normalized SDK properties.
 * @param value Detached scalar, list or message.
 * @return Whether a conditional spread retains the property.
 */
func sdkMetadataTruthy(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case string:
		return v != ""
	case bool:
		return v
	case int64:
		return v != 0
	case uint64:
		return v != 0
	case sdkMetadataInt64:
		return v != 0
	case sdkMetadataUint64:
		return v != 0
	case float64:
		return v != 0 && !math.IsNaN(v)
	}
	return true
}

/**
 * @brief Add only truthy present fields as required by the official converter's conditional spreads.
 * @param out Normalized destination.
 * @param message Decoded source.
 * @param names Camel-case fields.
 */
func sdkMetadataTruthyFields(out map[string]any, message *sdkMetadataMessage, names ...string) {
	for _, name := range names {
		if value, ok := message.values[name]; ok && sdkMetadataTruthy(value) {
			out[name] = value
		}
	}
}

/**
 * @brief Normalize supported native tool arguments exactly as the pinned SDK converter.
 * @param family Supported SDK family.
 * @param args Present native argument message.
 * @return Normalized arguments without executable callbacks.
 * @return Unknown recording-mode error.
 */
func sdkMetadataArgs(family string, args *sdkMetadataMessage) (map[string]any, error) {
	var out map[string]any
	switch family {
	case "shell":
		out = args.copy("command")
		sdkMetadataTruthyFields(out, args, "workingDirectory", "timeout")
	case "delete", "read", "edit":
		out = args.copy("path")
	case "glob":
		out = args.copy("globPattern")
		sdkMetadataTruthyFields(out, args, "targetDirectory")
	case "grep":
		out = args.copy("pattern")
		sdkMetadataTruthyFields(out, args, "path", "glob", "outputMode", "contextBefore", "contextAfter", "context", "caseInsensitive", "type", "headLimit", "multiline", "sort", "sortAscending")
		if value, ok := args.values["offset"]; ok {
			out["offset"] = value
		}
	case "ls":
		out = args.copy("path")
		if len(args.list("ignore")) > 0 {
			out["ignore"] = args.list("ignore")
		}
	case "readLints":
		out = map[string]any{"paths": args.list("paths")}
	case "mcp":
		out = map[string]any{"args": map[string]any{}}
		sdkMetadataTruthyFields(out, args, "providerIdentifier", "toolName")
		if values, ok := args.values["args"].(map[string]any); ok {
			converted := map[string]any{}
			for key, value := range values {
				if err := validateSDKMetadataJSON(value.(*sdkMetadataMessage)); err != nil {
					return nil, err
				}
				converted[key] = value.(*sdkMetadataMessage).special
			}
			out["args"] = converted
		}
	case "generateImage":
		out = args.copy("description")
		sdkMetadataTruthyFields(out, args, "filePath")
	case "recordScreen":
		mode := map[int64]string{1: "START_RECORDING", 2: "SAVE_RECORDING", 3: "DISCARD_RECORDING"}[args.enum("mode")]
		if mode == "" {
			return nil, errors.New("unknown cursor SDK recording mode")
		}
		out = map[string]any{"mode": mode}
	case "semSearch":
		out = args.copy("query")
		if len(args.list("targetDirectories")) > 0 {
			out["targetDirectories"] = args.list("targetDirectories")
		}
		sdkMetadataTruthyFields(out, args, "explanation")
	case "createPlan":
		out = args.copy("plan")
	case "updateTodos":
		out = map[string]any{"todos": sdkMetadataTodos(args.list("todos"))}
	case "task":
		out = args.copy("description", "prompt")
		sdkMetadataTruthyFields(out, args, "model", "resume", "agentId")
		for _, name := range []string{"attachments", "respondingToMessageIds"} {
			if len(args.list(name)) > 0 {
				out[name] = args.list(name)
			}
		}
		out["mode"] = sdkMetadataEnum(args.enum("mode"), map[int64]string{1: "agent", 2: "plan"}, "unspecified")
		if typ := args.message("subagentType"); typ != nil {
			kind := typ.groups["type"]
			if kind == "" {
				kind = "unspecified"
			}
			subagent := map[string]any{"kind": kind}
			if kind == "custom" {
				subagent["name"] = typ.message(kind).text("name")
			}
			out["subagentType"] = subagent
		}
	case "computerUse":
		actions := []any{}
		for _, value := range args.list("actions") {
			if action := sdkMetadataComputerAction(value.(*sdkMetadataMessage)); action != nil {
				actions = append(actions, action)
			}
		}
		out = args.copy("description")
		out["actions"] = actions
	}
	return out, nil
}

/**
 * @brief Match normalized SDK enum fallbacks without inventing unknown variants.
 * @param value Native enum number.
 * @param names Supported normalized names.
 * @param fallback Default name.
 * @return Normalized enum string.
 */
func sdkMetadataEnum(value int64, names map[int64]string, fallback string) string {
	if name, ok := names[value]; ok {
		return name
	}
	return fallback
}

/**
 * @brief Normalize native todo metadata while excluding internal IDs and timestamps.
 * @param values Native todo messages.
 * @return Non-nil normalized todo list.
 */
func sdkMetadataTodos(values []any) []any {
	out := []any{}
	for _, value := range values {
		todo := value.(*sdkMetadataMessage)
		item := todo.copy("content")
		item["status"] = sdkMetadataEnum(todo.enum("status"), map[int64]string{2: "inProgress", 3: "completed", 4: "cancelled"}, "pending")
		out = append(out, item)
	}
	return out
}

/**
 * @brief Select a normalized result envelope or omit an officially undefined result.
 * @param family Supported SDK family.
 * @param result Present result wrapper, or nil.
 * @return Normalized success/error object, or nil for absent/undefined branches.
 */
func sdkMetadataResult(family string, result *sdkMetadataMessage) map[string]any {
	if result == nil {
		return nil
	}
	branch := result.groups["result"]
	if branch == "" {
		return nil
	}
	value := result.message(branch)
	if value == nil {
		return nil
	}
	success := func(v map[string]any) map[string]any { return map[string]any{"status": "success", "value": v} }
	failure := func(v map[string]any) map[string]any { return map[string]any{"status": "error", "error": v} }
	message := func(keys ...string) string {
		for _, key := range keys {
			if field, ok := value.values[key]; ok {
				if text, ok := field.(string); ok {
					return text
				}
			}
		}
		return branch
	}
	if family == "shell" {
		switch branch {
		case "success", "failure":
			return success(value.copy("exitCode", "signal", "stdout", "stderr", "executionTime"))
		case "timeout":
			detail := value.copy("command", "workingDirectory")
			detail["message"] = fmt.Sprintf("Command timed out after %vms", value.values["timeoutMs"])
			return failure(detail)
		case "rejected", "spawnError":
			detail := value.copy("command", "workingDirectory")
			if branch == "rejected" {
				detail["message"] = message("reason")
			} else {
				detail["message"] = message("error")
			}
			return failure(detail)
		default:
			return nil
		}
	}
	if family == "recordScreen" {
		switch branch {
		case "startSuccess":
			return success(value.copy("wasPriorRecordingCancelled"))
		case "saveSuccess":
			out := value.copy("path", "recordingDurationMs")
			sdkMetadataNumberFields(out, "recordingDurationMs")
			return success(out)
		case "discardSuccess":
			return success(map[string]any{})
		case "failure":
			return failure(map[string]any{"message": message("error")})
		default:
			return nil
		}
	}
	if family == "computerUse" {
		if branch == "success" {
			out := value.copy("actionCount", "durationMs", "settleMs", "screenshotPath", "log")
			if position := value.message("cursorPosition"); position != nil {
				out["cursorPosition"] = position.copy("x", "y")
			}
			return success(out)
		}
		if branch == "error" {
			out := value.copy("actionCount", "durationMs", "screenshotPath", "log")
			out["message"] = message("error")
			return failure(out)
		}
		return nil
	}
	if family == "task" {
		switch branch {
		case "success":
			out := value.copy("isBackground", "durationMs")
			sdkMetadataNumberFields(out, "durationMs")
			sdkMetadataTruthyFields(out, value, "agentId", "resultSuffix", "transcriptPath")
			out["backgroundReason"] = sdkMetadataEnum(value.enum("backgroundReason"), map[int64]string{1: "agentRequest", 2: "userRequest", 3: "queuedFollowUp"}, "unspecified")
			if steps := value.list("conversationSteps"); len(steps) > 0 {
				converted := make([]any, 0, len(steps))
				for _, step := range steps {
					converted = append(converted, sdkMetadataProtoJSON(step.(*sdkMetadataMessage)))
				}
				out["conversationSteps"] = converted
			}
			return success(out)
		case "error":
			return failure(map[string]any{"message": message("error")})
		default:
			return nil
		}
	}
	if branch != "success" {
		out := map[string]any{}
		switch family {
		case "delete":
			out = value.copy("path")
			out["message"] = message("error", "reason", "clientVisibleError")
		case "read", "edit":
			out = value.copy("path")
			out["message"] = message("error", "reason")
		case "glob", "mcp":
			out["message"] = message("error", "reason")
		default:
			out["message"] = message("error")
		}
		return failure(out)
	}
	switch family {
	case "delete":
		out := value.copy("fileSize")
		sdkMetadataNumberFields(out, "fileSize")
		return success(out)
	case "glob":
		return success(value.copy("files", "totalFiles", "clientTruncated", "ripgrepTruncated"))
	case "grep":
		out := map[string]any{}
		workspace := map[string]any{}
		if values, ok := value.values["workspaceResults"].(map[string]any); ok {
			for key, item := range values {
				if normalized := sdkMetadataGrepUnion(item.(*sdkMetadataMessage)); normalized != nil {
					workspace[key] = normalized
				}
			}
		}
		if len(workspace) > 0 {
			out["workspaceResults"] = workspace
		}
		if editor := value.message("activeEditorResult"); editor != nil {
			if normalized := sdkMetadataGrepUnion(editor); normalized != nil {
				out["activeEditorResult"] = normalized
			}
		}
		return success(out)
	case "read":
		out := value.copy("totalLines", "fileSize")
		out["content"] = ""
		if value.groups["output"] == "content" {
			out["content"] = value.values["content"]
		}
		return success(out)
	case "edit":
		out := value.copy("linesAdded", "linesRemoved")
		sdkMetadataTruthyFields(out, value, "diffString")
		return success(out)
	case "ls":
		out := map[string]any{}
		if tree := value.message("directoryTreeRoot"); tree != nil {
			out["directoryTreeRoot"] = sdkMetadataDirectory(tree)
		}
		return success(out)
	case "readLints":
		files := []any{}
		for _, item := range value.list("fileDiagnostics") {
			file := item.(*sdkMetadataMessage)
			out := file.copy("path", "diagnosticsCount")
			diagnostics := []any{}
			for _, item := range file.list("diagnostics") {
				diagnostic := item.(*sdkMetadataMessage)
				d := diagnostic.copy("message", "source", "code")
				d["severity"] = sdkMetadataEnum(diagnostic.enum("severity"), map[int64]string{2: "warning", 3: "information", 4: "hint"}, "error")
				if position := diagnostic.message("range"); position != nil {
					r := map[string]any{}
					for _, endpoint := range []string{"start", "end"} {
						if point := position.message(endpoint); point != nil {
							r[endpoint] = point.copy("line", "character")
						}
					}
					d["range"] = r
				}
				diagnostics = append(diagnostics, d)
			}
			out["diagnostics"] = diagnostics
			files = append(files, out)
		}
		out := value.copy("totalFiles", "totalDiagnostics")
		out["fileDiagnostics"] = files
		return success(out)
	case "mcp":
		content := []any{}
		for _, item := range value.list("content") {
			node := item.(*sdkMetadataMessage)
			entry := map[string]any{}
			kind := node.groups["content"]
			switch kind {
			case "text":
				entry["text"] = node.message(kind).copy("text")
			case "image":
				image := node.message(kind)
				out := image.copy("data")
				sdkMetadataTruthyFields(out, image, "mimeType")
				entry["image"] = out
			}
			content = append(content, entry)
		}
		out := value.copy("isError")
		out["content"] = content
		return success(out)
	case "generateImage":
		return success(value.copy("filePath"))
	case "semSearch":
		return success(value.copy("results"))
	case "createPlan":
		return success(map[string]any{})
	case "updateTodos":
		out := value.copy("totalCount")
		out["todos"] = sdkMetadataTodos(value.list("todos"))
		return success(out)
	}
	return nil
}

/**
 * @brief Preserve the pinned grep converter's property selection and zero fallbacks.
 * @param union Native grep union message.
 * @return Normalized output, or nil for an unselected result.
 */
func sdkMetadataGrepUnion(union *sdkMetadataMessage) map[string]any {
	kind := union.groups["result"]
	value := union.message(kind)
	if value == nil {
		return nil
	}
	out := map[string]any{}
	switch kind {
	case "content":
		matches := []any{}
		for _, item := range value.list("matches") {
			match := item.(*sdkMetadataMessage)
			out := match.copy("file", "lineNumber", "line")
			sdkMetadataTruthyFields(out, match, "beforeContext", "afterContext")
			matches = append(matches, out)
		}
		out["matches"] = matches
		out["totalMatches"] = int64(0)
		if number, ok := value.values["totalMatches"]; ok {
			out["totalMatches"] = number
		}
	case "files":
		out["files"] = value.list("files")
		out["count"] = int64(0)
		if number, ok := value.values["count"]; ok {
			out["count"] = number
		}
	case "count":
		counts := []any{}
		for _, item := range value.list("counts") {
			counts = append(counts, item.(*sdkMetadataMessage).copy("file", "count"))
		}
		out["counts"] = counts
		out["total"] = int64(0)
		if number, ok := value.values["total"]; ok {
			out["total"] = number
		}
	default:
		return nil
	}
	return map[string]any{"type": kind, "output": out}
}

/**
 * @brief Normalize an already-decoded directory tree without reading a filesystem.
 * @param node Native directory metadata.
 * @return SDK directory shape with actual recorded children.
 */
func sdkMetadataDirectory(node *sdkMetadataMessage) map[string]any {
	out := node.copy("absPath", "childrenWereProcessed", "fullSubtreeExtensionCounts", "numFiles")
	dirs := []any{}
	for _, value := range node.list("childrenDirs") {
		dirs = append(dirs, sdkMetadataDirectory(value.(*sdkMetadataMessage)))
	}
	files := []any{}
	for _, value := range node.list("childrenFiles") {
		files = append(files, value.(*sdkMetadataMessage).copy("name"))
	}
	out["childrenDirs"], out["childrenFiles"] = dirs, files
	return out
}

/**
 * @brief Normalize supported computer-use metadata actions without making any client action executable.
 * @param action Native action oneof.
 * @return SDK action object, or nil for unknown/unselected variants.
 */
func sdkMetadataComputerAction(action *sdkMetadataMessage) map[string]any {
	kind := action.groups["action"]
	value := action.message(kind)
	if value == nil {
		return nil
	}
	out := map[string]any{"type": kind}
	switch kind {
	case "mouseMove", "click", "scroll":
		if point := value.message("coordinate"); point != nil {
			out["coordinate"] = point.copy("x", "y")
		}
	case "mouseDown", "mouseUp", "drag", "type", "key", "wait", "screenshot", "cursorPosition":
	default:
		return nil
	}
	switch kind {
	case "click":
		maps.Copy(out, value.copy("count", "modifierKeys"))
	case "scroll":
		maps.Copy(out, value.copy("amount", "modifierKeys"))
		out["direction"] = sdkMetadataEnum(value.enum("direction"), map[int64]string{1: "up", 2: "down", 3: "left", 4: "right"}, "unspecified")
	case "drag":
		path := []any{}
		for _, point := range value.list("path") {
			path = append(path, point.(*sdkMetadataMessage).copy("x", "y"))
		}
		out["path"] = path
		maps.Copy(out, value.copy("modifierKeys"))
	case "type":
		out["text"] = value.values["text"]
	case "key":
		maps.Copy(out, value.copy("key", "holdDurationMs"))
		out["stroke"] = sdkMetadataEnum(value.enum("stroke"), map[int64]string{1: "tap", 2: "down", 3: "up"}, "unspecified")
	case "wait":
		out["durationMs"] = value.values["durationMs"]
	}
	if kind == "click" || kind == "mouseDown" || kind == "mouseUp" || kind == "drag" {
		out["button"] = sdkMetadataEnum(value.enum("button"), map[int64]string{1: "left", 2: "right", 3: "middle", 4: "back", 5: "forward"}, "unspecified")
	}
	return out
}

/**
 * @brief Serialize recorded task conversation steps with protobuf JSON omission, byte and enum rules.
 * @param node Decoded native message.
 * @return Ordinary JSON-compatible object or special google.protobuf value.
 */
func sdkMetadataProtoJSON(node *sdkMetadataMessage) any {
	if node.symbol == "Value" || node.symbol == "Struct" {
		return node.special
	}
	out := map[string]any{}
	for _, field := range sdkMetadataSchema[node.symbol] {
		value, present := node.values[field.name]
		if !present {
			continue
		}
		if field.mapKey != "" {
			items := value.(map[string]any)
			if len(items) == 0 {
				continue
			}
			values := map[string]any{}
			for key, item := range items {
				values[key] = sdkMetadataJSONScalar(field.kind, item)
			}
			out[field.name] = values
			continue
		}
		if field.repeated {
			items := value.([]any)
			if len(items) == 0 {
				continue
			}
			values := make([]any, 0, len(items))
			for _, item := range items {
				values = append(values, sdkMetadataJSONScalar(field.kind, item))
			}
			out[field.name] = values
			continue
		}
		if field.group == "" && !field.optional && sdkMetadataIsDefault(value) {
			continue
		}
		out[field.name] = sdkMetadataJSONScalar(field.kind, value)
	}
	return out
}

/**
 * @brief Match protobuf JSON's omission of ordinary scalar defaults.
 * @param value Typed scalar or message.
 * @return Whether the value is an ordinary protobuf scalar zero.
 */
func sdkMetadataIsDefault(value any) bool {
	if bytes, ok := value.([]byte); ok {
		return len(bytes) == 0
	}
	if _, ok := value.(*sdkMetadataMessage); ok {
		return false
	}
	return !sdkMetadataTruthy(value)
}

/**
 * @brief Render one protobuf JSON scalar or nested message.
 * @param kind Pinned scalar number or reference.
 * @param value Decoded value.
 * @return JSON-compatible value with 64-bit strings and named enums.
 */
func sdkMetadataJSONScalar(kind string, value any) any {
	if node, ok := value.(*sdkMetadataMessage); ok {
		return sdkMetadataProtoJSON(node)
	}
	if strings.HasPrefix(kind, "#") {
		if name, ok := sdkToolMetadataEnums[kind[1:]][int32(value.(int64))]; ok {
			return name
		}
		return value
	}
	switch kind {
	case "1", "2":
		if number, ok := value.(float64); ok {
			switch {
			case math.IsNaN(number):
				return "NaN"
			case math.IsInf(number, 1):
				return "Infinity"
			case math.IsInf(number, -1):
				return "-Infinity"
			}
		}
	case "3", "4", "6", "16", "18":
		return fmt.Sprint(value)
	case "12":
		return base64.StdEncoding.EncodeToString(value.([]byte))
	}
	return value
}

/**
 * @brief Merge duplicate singular message fields using actual wire presence rather than constructor defaults.
 * @param destination Earlier same-type message, mutated in place.
 * @param source Later decoded message.
 */
func mergeSDKMetadata(destination, source *sdkMetadataMessage) {
	if destination.wellKnown != nil {
		proto.Merge(destination.wellKnown, source.wellKnown)
		switch value := destination.wellKnown.(type) {
		case *structpb.Value:
			destination.special = value.AsInterface()
		case *structpb.Struct:
			destination.special = value.AsMap()
		}
		return
	}
	for _, field := range sdkMetadataSchema[source.symbol] {
		if !source.present[field.name] {
			continue
		}
		value := source.values[field.name]
		if field.group != "" {
			if previous := destination.groups[field.group]; previous != field.name {
				delete(destination.values, previous)
				delete(destination.present, previous)
			}
			destination.groups[field.group] = field.name
		}
		switch {
		case field.mapKey != "":
			out := destination.values[field.name].(map[string]any)
			maps.Copy(out, value.(map[string]any))
		case field.repeated:
			destination.values[field.name] = append(destination.values[field.name].([]any), value.([]any)...)
		default:
			if next, ok := value.(*sdkMetadataMessage); ok {
				if previous, ok := destination.values[field.name].(*sdkMetadataMessage); ok {
					mergeSDKMetadata(previous, next)
					value = previous
				}
			}
			destination.values[field.name] = value
		}
		destination.present[field.name] = true
	}
}

/**
 * @brief Bound nested google.protobuf JSON values using the same shared metadata field budget.
 * @param value Decoded JSON value.
 * @param depth Current message depth.
 * @param budget Shared remaining field count.
 * @return Aggregate count or depth limit error.
 */
func boundSDKMetadataJSON(value any, depth int, budget *int) error {
	*budget--
	if depth > 64 || *budget < 0 {
		return errors.New("cursor SDK JSON metadata exceeds decoding limits")
	}
	switch value := value.(type) {
	case map[string]any:
		for _, child := range value {
			if err := boundSDKMetadataJSON(child, depth+1, budget); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := boundSDKMetadataJSON(child, depth+1, budget); err != nil {
				return err
			}
		}
	}
	return nil
}

/**
 * @brief Decode recorded native shell output into the pinned SDK's raw event metadata shape.
 * @param payload Native ShellOutputDeltaUpdate bytes, without an interaction envelope.
 * @return Event case and protobuf-JSON value, or an empty map when no event is selected.
 * @return Malformed protobuf or metadata decoding-limit error.
 * @note Metadata only; this never executes shell commands or creates client actions.
 */
func DecodeSDKShellOutput(payload []byte) (map[string]any, error) {
	if len(payload) > 8<<20 {
		return nil, errors.New("cursor SDK shell metadata exceeds size limit")
	}
	sdkMetadataSchemaOnce.Do(initSDKMetadataSchema)
	budget := 100000
	node, err := decodeSDKMetadata("ShellOutputDeltaUpdate", payload, 0, &budget)
	if err != nil {
		return nil, err
	}
	kind := node.groups["event"]
	if kind == "" {
		return map[string]any{}, nil
	}
	return map[string]any{"case": kind, "value": sdkMetadataProtoJSON(node.message(kind))}, nil
}

/**
 * @brief Match explicit JavaScript Number coercion in the pinned SDK result adapters.
 * @param out Normalized result fields, mutated in place.
 * @param names Fields which the source converter explicitly coerces with Number.
 */
func sdkMetadataNumberFields(out map[string]any, names ...string) {
	for _, name := range names {
		switch value := out[name].(type) {
		case int64:
			out[name] = float64(value)
		case uint64:
			out[name] = float64(value)
		case sdkMetadataInt64:
			out[name] = float64(value)
		case sdkMetadataUint64:
			out[name] = float64(value)
		}
	}
}

/**
 * @brief Reject protobuf Value objects which the pinned JavaScript toJson rejects.
 * @param node Recorded subtree actually serialized by the SDK converter.
 * @return Missing-value or non-finite JSON number error.
 */
func validateSDKMetadataJSON(node *sdkMetadataMessage) error {
	if node.wellKnown != nil {
		switch value := node.wellKnown.(type) {
		case *structpb.Value:
			return validateSDKMetadataValue(value)
		case *structpb.Struct:
			for _, child := range value.Fields {
				if err := validateSDKMetadataValue(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, value := range node.values {
		switch value := value.(type) {
		case *sdkMetadataMessage:
			if err := validateSDKMetadataJSON(value); err != nil {
				return err
			}
		case []any:
			for _, child := range value {
				if child, ok := child.(*sdkMetadataMessage); ok {
					if err := validateSDKMetadataJSON(child); err != nil {
						return err
					}
				}
			}
		case map[string]any:
			for _, child := range value {
				if child, ok := child.(*sdkMetadataMessage); ok {
					if err := validateSDKMetadataJSON(child); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

/**
 * @brief Match pinned google.protobuf.Value.toJson finite-number and selected-kind requirements.
 * @param value Decoded well-known JSON value.
 * @return Invalid or non-finite JSON value error.
 */
func validateSDKMetadataValue(value *structpb.Value) error {
	if value == nil || value.Kind == nil {
		return errors.New("cursor SDK protobuf Value must have a value")
	}
	switch kind := value.Kind.(type) {
	case *structpb.Value_NumberValue:
		if math.IsNaN(kind.NumberValue) || math.IsInf(kind.NumberValue, 0) {
			return errors.New("cursor SDK protobuf Value cannot be NaN or Infinity")
		}
	case *structpb.Value_StructValue:
		for _, child := range kind.StructValue.GetFields() {
			if err := validateSDKMetadataValue(child); err != nil {
				return err
			}
		}
	case *structpb.Value_ListValue:
		for _, child := range kind.ListValue.GetValues() {
			if err := validateSDKMetadataValue(child); err != nil {
				return err
			}
		}
	}
	return nil
}

/**
 * @brief Match the pinned SDK's toJsonValue2 conversion before encoding a protobuf Struct.
 * @param value Normalized metadata, including native BigInt markers and Uint8Array-equivalent byte slices.
 * @return Detached Struct-compatible value; ordinary Go integer fields remain numbers.
 * @return Unsupported values, non-finite numbers, decoding limits or projected Struct output above 32 MiB.
 * @details Native BigInts become decimal strings. Byte arrays become numeric-key objects, preserving the SDK wire contract.
 */
func SDKMetadataForStruct(value any) (any, error) {
	if _, err := projectSDKMetadataStruct(value); err != nil {
		return nil, err
	}
	budget := sdkMetadataStructBudget{values: 100000, bytes: 8 << 20}
	return sdkMetadataForStruct(value, 0, &budget)
}

/**
 * @brief Protobuf byte sizes after SDK normalization, without materializing byte-array objects.
 */
type sdkMetadataProjection struct {
	value  int64 ///< Encoded google.protobuf.Value bytes, including its selected-kind envelope.
	object int64 ///< Encoded google.protobuf.Struct bytes, or -1 when this value is not an object.
}

/**
 * @brief Project the complete normalized protobuf output before allocating numeric-key byte maps.
 * @param value Input metadata, including native BigInt markers and byte arrays.
 * @return Exact Struct size for object roots, or Value size for other roots.
 * @return Unsupported input, decoding limit or normalized output above 32 MiB.
 */
func projectSDKMetadataStruct(value any) (int64, error) {
	budget := sdkMetadataStructBudget{values: 100000, bytes: 8 << 20}
	projected, err := projectSDKMetadataValue(value, 0, &budget)
	if err != nil {
		return 0, err
	}
	if projected.object >= 0 {
		return projected.object, nil
	}
	return projected.value, nil
}

/**
 * @brief Compute one single-byte-tag length-delimited protobuf field's encoded size.
 * @param length Message, map entry or string byte length.
 * @return Tag, length prefix and payload bytes.
 */
func sdkMetadataDelimitedSize(length int64) int64 {
	return 1 + int64(protowire.SizeVarint(uint64(length))) + length
}

/**
 * @brief Project a Uint8Array numeric-key Struct in logarithmic time without allocating keys or entries.
 * @param length Bounded raw byte count.
 * @return Exact Struct body size, including all map-entry and Value envelopes.
 * @details Each numeric-double entry costs 15 plus the decimal index's digit count; the bounded indexes fit one-byte key-length prefixes.
 */
func sdkMetadataByteObjectSize(length int) int64 {
	var size int64
	for start, end, digits := 0, 10, int64(1); start < length; start, end, digits = end, end*10, digits+1 {
		count := min(length, end) - start
		size += int64(count) * (15 + digits)
	}
	return size
}

/**
 * @brief Traverse ordinary metadata while projecting exact protobuf envelopes and bounded byte expansion.
 * @param value Metadata value before allocation of normalized containers.
 * @param depth Current object depth.
 * @param budget Shared ordinary-value and raw-byte bounds.
 * @return Value and optional Struct-body byte sizes.
 * @return Unsupported data, malformed strings/numbers, decoding limits or output above 32 MiB.
 */
func projectSDKMetadataValue(value any, depth int, budget *sdkMetadataStructBudget) (sdkMetadataProjection, error) {
	budget.values--
	if depth > 64 || budget.values < 0 {
		return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata exceeds decoding limits")
	}
	projected := sdkMetadataProjection{object: -1}
	switch value := value.(type) {
	case sdkMetadataInt64:
		projected.value = sdkMetadataDelimitedSize(int64(len(strconv.FormatInt(int64(value), 10))))
	case sdkMetadataUint64:
		projected.value = sdkMetadataDelimitedSize(int64(len(strconv.FormatUint(uint64(value), 10))))
	case []byte:
		if len(value) > budget.bytes {
			return sdkMetadataProjection{}, errors.New("cursor SDK byte metadata exceeds Struct value limit")
		}
		budget.bytes -= len(value)
		projected.object = sdkMetadataByteObjectSize(len(value))
		projected.value = sdkMetadataDelimitedSize(projected.object)
	case map[string]any:
		if len(value) > budget.values {
			return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata exceeds value count limit")
		}
		projected.object = 0
		for key, child := range value {
			if !utf8.ValidString(key) {
				return sdkMetadataProjection{}, errors.New("invalid cursor SDK Struct metadata key UTF-8")
			}
			if len(key) > 32<<20 {
				return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata exceeds 32 MiB output limit")
			}
			nested, err := projectSDKMetadataValue(child, depth+1, budget)
			if err != nil {
				return sdkMetadataProjection{}, err
			}
			entry := sdkMetadataDelimitedSize(int64(len(key))) + sdkMetadataDelimitedSize(nested.value)
			projected.object += sdkMetadataDelimitedSize(entry)
			if projected.object > 32<<20 {
				return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata exceeds 32 MiB output limit")
			}
		}
		projected.value = sdkMetadataDelimitedSize(projected.object)
	case []any:
		if len(value) > budget.values {
			return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata exceeds value count limit")
		}
		var list int64
		for _, child := range value {
			nested, err := projectSDKMetadataValue(child, depth+1, budget)
			if err != nil {
				return sdkMetadataProjection{}, err
			}
			list += sdkMetadataDelimitedSize(nested.value)
			if list > 32<<20 {
				return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata exceeds 32 MiB output limit")
			}
		}
		projected.value = sdkMetadataDelimitedSize(list)
	case string:
		if len(value) > 32<<20 {
			return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata exceeds 32 MiB output limit")
		}
		if !utf8.ValidString(value) {
			return sdkMetadataProjection{}, errors.New("invalid cursor SDK Struct metadata string UTF-8")
		}
		projected.value = sdkMetadataDelimitedSize(int64(len(value)))
	case nil, bool:
		projected.value = 2
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata contains a non-finite number")
		}
		projected.value = 9
	case float32:
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata contains a non-finite number")
		}
		projected.value = 9
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		projected.value = 9
	default:
		return sdkMetadataProjection{}, errors.New("unsupported cursor SDK Struct metadata value")
	}
	if (projected.object >= 0 && projected.object > 32<<20) || (projected.object < 0 && projected.value > 32<<20) {
		return sdkMetadataProjection{}, errors.New("cursor SDK Struct metadata exceeds 32 MiB output limit")
	}
	return projected, nil
}

/**
 * @brief Separate bounded byte-array expansion from ordinary metadata field traversal.
 */
type sdkMetadataStructBudget struct {
	values int ///< Remaining ordinary values, matching the native metadata decoder's field bound.
	bytes  int ///< Remaining byte-array bytes, matching the native message byte bound.
}

/**
 * @brief Convert one bounded metadata value without mutating caller-owned maps, slices or bytes.
 * @param value Metadata value to normalize.
 * @param depth Current object depth.
 * @param budget Shared remaining ordinary-value and byte-array limits.
 * @return Struct-compatible detached value.
 * @return Unsupported type, non-finite number or resource-bound error.
 */
func sdkMetadataForStruct(value any, depth int, budget *sdkMetadataStructBudget) (any, error) {
	budget.values--
	if depth > 64 || budget.values < 0 {
		return nil, errors.New("cursor SDK Struct metadata exceeds decoding limits")
	}
	switch value := value.(type) {
	case sdkMetadataInt64:
		return strconv.FormatInt(int64(value), 10), nil
	case sdkMetadataUint64:
		return strconv.FormatUint(uint64(value), 10), nil
	case []byte:
		if len(value) > budget.bytes {
			return nil, errors.New("cursor SDK byte metadata exceeds Struct value limit")
		}
		budget.bytes -= len(value)
		out := make(map[string]any, len(value))
		for index, number := range value {
			out[strconv.Itoa(index)] = float64(number)
		}
		return out, nil
	case map[string]any:
		if len(value) > budget.values {
			return nil, errors.New("cursor SDK Struct metadata exceeds value count limit")
		}
		out := make(map[string]any, len(value))
		for key, child := range value {
			converted, err := sdkMetadataForStruct(child, depth+1, budget)
			if err != nil {
				return nil, err
			}
			out[key] = converted
		}
		return out, nil
	case []any:
		if len(value) > budget.values {
			return nil, errors.New("cursor SDK Struct metadata exceeds value count limit")
		}
		out := make([]any, 0, len(value))
		for _, child := range value {
			converted, err := sdkMetadataForStruct(child, depth+1, budget)
			if err != nil {
				return nil, err
			}
			out = append(out, converted)
		}
		return out, nil
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, errors.New("cursor SDK Struct metadata contains a non-finite number")
		}
		return value, nil
	case float32:
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, errors.New("cursor SDK Struct metadata contains a non-finite number")
		}
		return value, nil
	case nil, string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return value, nil
	default:
		return nil, errors.New("unsupported cursor SDK Struct metadata value")
	}
}
