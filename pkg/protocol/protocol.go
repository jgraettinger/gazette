// Package protocol defines core Gazette types and APIs.

//go:generate protoc -I . -I ../../vendor  --gogo_out=plugins=grpc:. protocol.proto
package protocol
