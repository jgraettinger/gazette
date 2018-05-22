// Package protocol defines core Gazette types and APIs.

//go:generate protoc -I . -I ../../vendor  --gogo_out=plugins=grpc:. protocol.proto
//go:generate mockery -name=BrokerServer
package protocol
