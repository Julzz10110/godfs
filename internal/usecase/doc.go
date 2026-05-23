// Package usecase is application orchestration on top of domain + ports.
// Master gRPC handlers delegate validation and orchestration here; adapters map domain errors to gRPC status.
package usecase
