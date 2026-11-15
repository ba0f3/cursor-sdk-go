package cursorbridgeruntime

import (
	"net/http"

	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
)

func sdkAgentHandler(services *bridgeServices) (string, http.Handler) {
	path, handler := sdkv1connect.NewSdkAgentServiceHandler(services)
	return path, handler
}

func sdkCursorHandler(services *bridgeServices) (string, http.Handler) {
	path, handler := sdkv1connect.NewSdkCursorServiceHandler(services)
	return path, handler
}

func sdkControlHandler(services *bridgeServices) (string, http.Handler) {
	path, handler := sdkv1connect.NewSdkBridgeControlServiceHandler(services)
	return path, handler
}
