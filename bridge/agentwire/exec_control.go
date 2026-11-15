package agentwire

/**
 * @brief One pinned native exec request and its corresponding client result schema.
 */
type nativeExecVariant struct {
	name        string ///< Native request oneof name, excluding correlation metadata.
	arguments   string ///< Authoritative argument descriptor type.
	resultField int    ///< Corresponding ExecClientMessage oneof tag; may differ from the request tag.
	resultType  string ///< Authoritative client result descriptor type.
}

/**
 * @brief Answer every pinned native exec operation according to this runtime's actual local capabilities.
 * @param entries Decoded ExecServerMessage including its correlation fields.
 * @param tools Client-owned custom tools available through the dedicated native MCP callback path.
 * @return Framed typed result, or nil for callbacks, unknown operations or result schemas requiring exec throw/close.
 * @note Never executes host tools, reads host files or starts proxy-owned resource services.
 */
func (s *ControlState) replyExecControl(entries []Field, tools []Tool) []byte {
	var id uint32
	var execID string
	var operation Field
	for _, field := range entries {
		switch field.Number {
		case 1:
			id = uint32(field.Varint)
		case 15:
			execID = string(field.Bytes)
		default:
			if _, ok := nativeExecVariants[field.Number]; ok && field.WireType == wireLength {
				operation = field // The last recognized protobuf oneof member wins.
			}
		}
	}
	variant, known := nativeExecVariants[operation.Number]
	if !known {
		return nil
	}
	args := decodeAll(operation.Bytes)
	argument := func(number int) string {
		field, _ := lastSDKField(args, number)
		return string(field.Bytes)
	}
	const unavailable = "Use the declared client MCP tools; proxy-host built-in execution is unavailable"
	var result []byte
	switch operation.Number {
	case 2, 52: // ShellResult.rejected, including mini_swe's distinct outer result tag 55.
		result = encodeMessage(4, encodeString(1, argument(1)), encodeString(2, argument(2)), encodeString(3, unavailable))
	case 3: // WriteResult.error -> WriteError.
		result = encodeMessage(5, encodeString(1, argument(1)), encodeString(2, unavailable))
	case 4: // DeleteResult.error -> DeleteError.
		result = encodeMessage(7, encodeString(1, argument(1)), encodeString(2, unavailable))
	case 5: // GrepResult.error -> GrepError.error.
		result = encodeMessage(2, encodeString(1, unavailable))
	case 7, 29: // ReadResult; only explicitly attached virtual image bytes are locally owned.
		if image, ok := s.Images[argument(1)]; ok {
			result = encodeMessage(1, encodeString(1, image.Path), encodeBytes(5, image.Data), encodeUInt32Field(4, uint32(len(image.Data))))
		} else {
			result = encodeMessage(2, encodeString(1, argument(1)), encodeString(2, unavailable))
		}
	case 8, 9: // LsError and DiagnosticsError share path/error tags.
		result = encodeMessage(2, encodeString(1, argument(1)), encodeString(2, unavailable))
	case 10: // Empty request context accurately describes a bridge without client workspace services.
		result = encodeMessage(1, encodeMessage(1))
	case 11: // Only DecodeToolCall may authorize a native client MCP callback.
		return nil
	case 14: // ShellStream.rejected is tag 5, unlike ShellResult.rejected tag 4.
		result = encodeMessage(5, encodeString(1, argument(1)), encodeString(2, argument(2)), encodeString(3, unavailable))
	case 16: // BackgroundShellSpawnError retains command and working-directory correlation.
		result = encodeMessage(2, encodeString(1, argument(1)), encodeString(2, argument(2)), encodeString(3, unavailable))
	case 17: // Custom MCP tools do not register resource providers in this runtime.
		result = encodeMessage(1)
	case 18: // ReadMcpResourceError.uri comes from argument tag 2, not the server name at tag 1.
		result = encodeMessage(2, encodeString(1, argument(2)), encodeString(2, unavailable))
	case 20: // FetchError.url/error; the proxy never fetches a model-requested URL.
		result = encodeMessage(2, encodeString(1, argument(1)), encodeString(2, unavailable))
	case 21: // RecordScreenResult.failure -> RecordScreenFailure.error.
		result = encodeMessage(4, encodeString(1, unavailable))
	case 22, 23: // ComputerUseError and WriteShellStdinError.error.
		result = encodeMessage(2, encodeString(1, unavailable))
	case 27: // ExecuteHookResult has no failure branch; the engine emits typed exec throw/close.
		return nil
	case 28: // SubagentError has an optional agent_id and its error string at tag 2.
		result = encodeMessage(2, encodeString(2, unavailable))
	case 30, 31: // Neither shell nor subagent execution is started here: NOT_FOUND is exactly enum value 2.
		result = encodeUInt32Field(1, 2)
	case 36:
		server := [][]byte{encodeString(1, "ccload"), encodeString(2, "ccload"), encodeString(7, "ready")}
		for _, tool := range tools {
			server = append(server, encodeMessage(5, encodeString(1, tool.Name), encodeString(2, tool.Description), encodeBytes(3, tool.Schema), encodeString(4, "ccload"), encodeString(5, tool.Name)))
		}
		result = encodeMessage(1, encodeMessage(1, server...))
	case 37: // No local subagent can be owned by this no-execution runtime.
		result = encodeMessage(3, encodeString(1, argument(1)))
	case 38: // SmartModeClassifierError.error; no local approval classifier is installed.
		result = encodeMessage(2, encodeString(1, unavailable))
	case 40: // CanvasDiagnosticsError.path/error.
		result = encodeMessage(2, encodeString(1, argument(1)), encodeString(2, unavailable))
	case 41, 42, 43: // No automatic shell, MCP or web-fetch permission is allowlisted locally.
		result = encodeUInt32Field(1, 0)
	case 44: // GetDiffResponse has no failure branch; never represent an unread workspace as an empty diff.
		return nil
	case 53, 54: // ConversationSearchError and AgentStoreConflictError.error.
		result = encodeMessage(2, encodeString(1, unavailable))
	default:
		return nil
	}
	return EncodeConnectDataFrame(encodeMessage(2, encodeUInt32Field(1, id), encodeString(15, execID), encodeMessage(variant.resultField, result)))
}
