package replicache

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Replicache struct {
	logger              *slog.Logger
	db                  *sql.DB
	handler             Handler
	clientOnPush        bool
	clientOnPull        bool
	clientPurgeDuration time.Duration
}

func (rep *Replicache) PushHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := struct {
			PushVersion   int        `json:"pushVersion"`
			ClientGroupID string     `json:"clientGroupID"`
			Mutations     []Mutation `json:"mutations"`
			ProfileID     string     `json:"profileID"`
			SchemaVersion string     `json:"schemaVersion"`
		}{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.PushVersion != 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := rep.handlePush(r.Context(), ClientInfo{
			Auth:          r.Header.Get("Authorization"),
			ClientGroupID: req.ClientGroupID,
			ProfileID:     req.ProfileID,
			SchemaVersion: req.SchemaVersion,
		},
			req.Mutations,
		); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

func (rep *Replicache) handlePush(ctx context.Context, info ClientInfo, mutations []Mutation) error {

	tx, err := rep.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// TODO: check all clientIDs for mutations exist in client group
	// or add them if createOnPush is true

	if err := rep.handler.HandlePush(ctx, PushRequest{
		ClientInfo: info,
		Mutations:  mutations,
		Tx:         tx,
	}); err != nil {
		// TODO: inspect error to see if it's an auth error
		return err
	}

	// TODO: update client state

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func newDefaultInstance(db *sql.DB, handler Handler) *Replicache {
	return &Replicache{
		db:      db,
		handler: handler,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

type Option func(r *Replicache) error

func NewReplicache(db *sql.DB, handler Handler, options ...Option) (*Replicache, error) {
	r := newDefaultInstance(db, handler)
	for _, opt := range options {
		if err := opt(r); err != nil {
			return nil, err
		}
	}
	return r, nil
}

type PushHandler interface {
	HandlePush(ctx context.Context, pr PushRequest) error
}

type PullHandler interface {
	HandlePull(ctx context.Context, pr PullRequest) (any, error)
}

type PushRequest struct {
	ClientInfo
	Mutations []Mutation
	Tx        *sql.Tx
}

type PullRequest struct {
	ClientInfo
	Tx *sql.Tx
}

type ClientInfo struct {
	Auth          string
	ClientGroupID string
	ProfileID     string
	SchemaVersion string
}

type Mutation struct {
	ClientID string          `json:"clientID"`
	ID       int             `json:"id"`
	Name     string          `json:"name"`
	Args     json.RawMessage `json:"args"`

	// DOMHighResTimeStamp (not used by the protocol)
	Timestamp float64 `json:"timestamp"`
}

type Handler interface {
	PushHandler
	PullHandler
}
