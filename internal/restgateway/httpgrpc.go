package restgateway

import (
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func statusFromGRPC(err error) int {
	if err == nil {
		return http.StatusOK
	}
	st, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError
	}
	switch st.Code() {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		// Best-effort mapping; some proxies use 499, but that's non-standard.
		return http.StatusRequestTimeout
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.OutOfRange:
		return http.StatusBadRequest
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.Aborted:
		return http.StatusConflict
	case codes.DataLoss:
		return http.StatusInternalServerError
	case codes.Internal:
		return http.StatusInternalServerError
	case codes.Unknown:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

func grpcCodeToRESTCode(grpcCode string) string {
	grpcCode = strings.TrimSpace(grpcCode)
	if grpcCode == "" {
		return ""
	}
	return strings.ToLower(grpcCode)
}
