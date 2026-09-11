// Package grpcx provides the repo's shared gRPC dial helper.
//
// LB policy (docs/research/0002): the default grpc-go policy is
// pick_first — with DNS returning several replica addresses, every RPC
// would still go to the first reachable one. Setting round_robin in the
// service config makes each client keep a subchannel per replica and
// distribute RPCs per-call, which is the documented client-side
// load-balancing path for trusted in-mesh clients.
package grpcx

import (
	"google.golang.org/grpc"
)

// roundRobinConfig distributes RPCs across every address the resolver
// returns for the target. The default dns resolver re-resolves
// immediately on subchannel failure and periodically (30 min default)
// after that, so scaled-down replicas drop out fast; scale-up discovery
// waits for the next periodic re-resolve (acceptable for this stack).
const roundRobinConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`

// Dial connects to addr with client-side round_robin load balancing.
// Extra opts (credentials, resolvers, interceptors) are caller-owned so
// services keep control of their security posture.
func Dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append([]grpc.DialOption{
		grpc.WithDefaultServiceConfig(roundRobinConfig),
	}, opts...)
	return grpc.NewClient(addr, opts...)
}
