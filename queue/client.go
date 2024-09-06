package queue

import (
	aPb "github.com/c12s/scheme/apollo"
	cPb "github.com/c12s/scheme/celestial"
	mPb "github.com/c12s/scheme/meridian"
	"google.golang.org/grpc"
	"log"
)

func NewCelestialClient(address string) cPb.CelestialServiceClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to start gRPC connection to celestial service: %v", err)
	}
	return cPb.NewCelestialServiceClient(conn)
}

func NewApolloClient(address string) aPb.ApolloServiceClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to start gRPC connection to celestial service: %v", err)
	}
	return aPb.NewApolloServiceClient(conn)
}

func NewMeridianClient(address string) mPb.MeridianServiceClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to start gRPC connection to apollo service: %v", err)
	}

	return mPb.NewMeridianServiceClient(conn)
}
