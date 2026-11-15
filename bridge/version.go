package cursorbridgeruntime

// Native bridge build metadata exposed through SdkBridgeControlService.GetVersion.
const (
	BridgeVersion   = "1.2.0-go"
	ProtocolVersion = "sdk.v1"
)

var bridgeCapabilities = []string{
	"agent.create",
	"agent.send",
	"run.cancel",
	"agent.management",
}

// defaultBridgeModels mirrors cursorauth.DefaultModels without an import cycle.
var defaultBridgeModels = []string{
	"default",
	"composer-2.5",
	"composer-2.5-fast",
	"grok-4.7",
	"grok-4.7-fast",
}
