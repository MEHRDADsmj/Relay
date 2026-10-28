package grpc

import (
	"context"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

type apiKeyServer struct {
	relayv1.UnimplementedAPIKeyServiceServer
	apiKeys *app.APIKeyService
}

func (s *apiKeyServer) CreateAPIKey(ctx context.Context, req *relayv1.CreateAPIKeyRequest) (*relayv1.CreateAPIKeyResponse, error) {
	created, err := s.apiKeys.Create(ctx, createAPIKeyCommand(req))
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.CreateAPIKeyResponse{
		Key:    apiKeyToProto(created.Key),
		Secret: created.Secret,
	}, nil
}

func (s *apiKeyServer) ListAPIKeys(ctx context.Context, _ *relayv1.ListAPIKeysRequest) (*relayv1.ListAPIKeysResponse, error) {
	keys, err := s.apiKeys.List(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	if keys == nil {
		keys = []domain.APIKey{}
	}
	return &relayv1.ListAPIKeysResponse{Keys: apiKeysToProto(keys)}, nil
}

func (s *apiKeyServer) RevokeAPIKey(ctx context.Context, req *relayv1.RevokeAPIKeyRequest) (*relayv1.RevokeAPIKeyResponse, error) {
	key, err := s.apiKeys.Revoke(ctx, req.GetId())
	if err != nil {
		return nil, mapError(err)
	}
	return &relayv1.RevokeAPIKeyResponse{Key: apiKeyToProto(key)}, nil
}
