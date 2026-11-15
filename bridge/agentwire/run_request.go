package agentwire

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/google/uuid"
)

/**
 * @brief AgentRunRequest field numbers verified against the pinned Cursor descriptors.
 */
const (
	agentRunRequestConversationState = 1 ///< Conversation state envelope.
	agentRunRequestAction            = 2 ///< User action envelope.
	agentRunConversationID           = 5 ///< Conversation identifier.
	agentRunRequestedModel           = 9 ///< Requested model selection.

	conversationActionUserMessage = 1
	userMessageText               = 1
	userMessageMessageID          = 2
	userMessageSelectedContext    = 3
	userMessageMode               = 4

	requestedModelID         = 1
	requestedModelParameters = 3
	modelParameterID         = 1
	modelParameterValue      = 2
)

/**
 * @brief Image bytes attached to a Cursor agent turn.
 */
type InputImage struct {
	Data     []byte ///< Decoded image bytes.
	MimeType string ///< Image media type.
	Width    uint32 ///< Optional pixel width.
	Height   uint32 ///< Optional pixel height.
	Path     string ///< Virtual read-only attachment path, never a host filesystem path.
}

/**
 * @brief One opaque model parameter accepted by Cursor's native model selection.
 */
type ModelParameter struct {
	ID    string ///< Catalog parameter identity.
	Value string ///< Exact selected catalog value.
}

/**
 * @brief Inputs for one Cursor user turn with client-owned tools and images.
 */
type RunInput struct {
	RunID          string           ///< Unique native generation UUID; retries of the same generation may reuse it.
	Checkpoint     []byte           ///< Opaque ConversationStateStructure from the preceding native turn.
	ConversationID string           ///< Conversation identifier; generated when empty.
	MessageID      string           ///< User message identifier; generated when empty.
	UserText       string           ///< Ordered conversation text from the client adapter.
	Images         []InputImage     ///< In-memory image attachments.
	Tools          []Tool           ///< Client-owned MCP declarations.
	ModelID        string           ///< Cursor model identifier; default when empty.
	Parameters     []ModelParameter ///< Preserve the complete selected native model parameters.
	Mode           uint32           ///< Explicit native agent mode; zero retains the checkpoint/default mode.
}

/**
 * @brief Encode the first AgentClientMessage run_request frame for a text turn.
 * @param in Turn content, model selection and client capabilities.
 * @return Unframed AgentClientMessage protobuf payload.
 */
func EncodeRunRequest(in RunInput) []byte {
	runID := in.RunID
	if runID == "" {
		runID = uuid.NewString()
	}
	conversationID := in.ConversationID
	if conversationID == "" {
		conversationID = uuid.NewString()
	}
	messageID := in.MessageID
	if messageID == "" {
		messageID = uuid.NewString()
	}
	modelID := in.ModelID
	if modelID == "" {
		modelID = "default"
	}

	userMessage := encodeString(userMessageText, in.UserText)
	userMessage = append(userMessage, encodeString(userMessageMessageID, messageID)...)
	var images [][]byte
	for _, image := range in.Images {
		fields := [][]byte{encodeString(2, uuid.NewString()), encodeString(7, image.MimeType), encodeBytes(8, image.Data)}
		if image.Path != "" {
			fields = append(fields, encodeString(3, image.Path))
		}
		if image.Width > 0 && image.Height > 0 {
			fields = append(fields, encodeMessage(4, encodeUInt32Field(1, image.Width), encodeUInt32Field(2, image.Height)))
		}
		images = append(images, encodeMessage(1, fields...))
	}
	userMessage = append(userMessage, encodeMessage(userMessageSelectedContext, images...)...)
	if in.Mode != 0 {
		userMessage = append(userMessage, encodeUInt32Field(userMessageMode, in.Mode)...)
	}

	userAction := encodeMessage(conversationActionUserMessage, encodeMessage(1, userMessage))
	action := encodeMessage(agentRunRequestAction, userAction)
	conversationState := encodeBytes(agentRunRequestConversationState, in.Checkpoint)

	rmParts := [][]byte{encodeString(requestedModelID, modelID)}
	for _, parameter := range in.Parameters {
		rmParts = append(rmParts,
			encodeMessage(requestedModelParameters,
				encodeString(modelParameterID, parameter.ID),
				encodeString(modelParameterValue, parameter.Value),
			),
		)
	}
	requestedModel := encodeMessage(agentRunRequestedModel, rmParts...)

	return encodeMessage(1,
		conversationState,
		action,
		EncodeTools(in.Tools),
		encodeString(agentRunConversationID, conversationID),
		requestedModel,
		encodeUInt32Field(12, 0),
		encodeString(16, conversationID),
		encodeUInt32Field(19, 1), // client_supports_inline_images, pinned SDK v1.2.0 descriptor.
		encodeString(25, runID),  // Official SDK always supplies the original per-generation UUID.
	)
}

// RandomExecID returns a hex identifier for exec acknowledgements.
func RandomExecID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
