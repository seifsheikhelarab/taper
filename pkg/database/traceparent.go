package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"

	obs "github.com/seifsheikhelarab/taper/pkg/observability"
)

// TraceparentText returns ctx's W3C traceparent as a pgtype.Text for the
// outbox traceparent column: "" becomes NULL so Debezium simply omits the
// Kafka header for events written without an active span.
func TraceparentText(ctx context.Context) pgtype.Text {
	tp := obs.Traceparent(ctx)
	return pgtype.Text{String: tp, Valid: tp != ""}
}
