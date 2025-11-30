package pgdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/arkade-os/arkd/internal/core/domain"
	"github.com/arkade-os/arkd/internal/infrastructure/db/postgres/sqlc/queries"
)

type offchainTxRepository struct {
	db      *sql.DB
	querier *queries.Queries
}

func NewOffchainTxRepository(config ...interface{}) (domain.OffchainTxRepository, error) {
	if len(config) != 1 {
		return nil, fmt.Errorf("invalid config")
	}
	db, ok := config[0].(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("cannot open offchain tx repository: invalid config")
	}

	return &offchainTxRepository{
		db:      db,
		querier: queries.New(db),
	}, nil
}

func (v *offchainTxRepository) AddOrUpdateOffchainTx(
	ctx context.Context, offchainTx *domain.OffchainTx,
) error {
	txBody := func(querierWithTx *queries.Queries) error {
		if err := querierWithTx.UpsertOffchainTx(ctx, queries.UpsertOffchainTxParams{
			Txid:              offchainTx.ArkTxid,
			Tx:                offchainTx.ArkTx,
			StartingTimestamp: offchainTx.StartingTimestamp,
			EndingTimestamp:   offchainTx.EndingTimestamp,
			ExpiryTimestamp:   offchainTx.ExpiryTimestamp,
			StageCode:         int32(offchainTx.Stage.Code),
			FailReason: sql.NullString{
				String: offchainTx.FailReason, Valid: offchainTx.FailReason != "",
			},
		}); err != nil {
			return err
		}

		for checkpointTxid, commitmentTxid := range offchainTx.CommitmentTxids {
			checkpointTx, ok := offchainTx.CheckpointTxs[checkpointTxid]
			if !ok {
				continue
			}
			isRoot := commitmentTxid == offchainTx.RootCommitmentTxId
			err := querierWithTx.UpsertCheckpointTx(ctx, queries.UpsertCheckpointTxParams{
				Txid:                 checkpointTxid,
				Tx:                   checkpointTx,
				CommitmentTxid:       commitmentTxid,
				IsRootCommitmentTxid: isRoot,
				OffchainTxid:         offchainTx.ArkTxid,
			})
			if err != nil {
				return err
			}
		}
		return nil
	}
	return execTx(ctx, v.db, txBody)
}

func (v *offchainTxRepository) GetOffchainTx(
	ctx context.Context, txid string,
) (*domain.OffchainTx, error) {
	rows, err := v.querier.SelectOffchainTx(ctx, txid)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("offchain tx %s not found", txid)
	}
	vt := rows[0].OffchainTxVw
	checkpointTxs := make(map[string]string)
	commitmentTxids := make(map[string]string)
	rootCommitmentTxId := ""
	for _, row := range rows {
		vw := row.OffchainTxVw
		if vw.CheckpointTxid.Valid && vw.CheckpointTx.Valid {
			checkpointTxs[vw.CheckpointTxid.String] = vw.CheckpointTx.String
			commitmentTxids[vw.CheckpointTxid.String] = vw.CommitmentTxid.String
			if vw.IsRootCommitmentTxid.Valid && vw.IsRootCommitmentTxid.Bool {
				rootCommitmentTxId = vw.CommitmentTxid.String
			}
		}
	}
	stage := domain.Stage{Code: int(vt.StageCode)}
	if vt.FailReason.String != "" {
		stage.Failed = true
	}
	if domain.OffchainTxStage(vt.StageCode) == domain.OffchainTxFinalizedStage {
		stage.Ended = true
	}
	return &domain.OffchainTx{
		ArkTxid:            vt.Txid,
		ArkTx:              vt.Tx,
		StartingTimestamp:  vt.StartingTimestamp,
		EndingTimestamp:    vt.EndingTimestamp,
		ExpiryTimestamp:    vt.ExpiryTimestamp,
		FailReason:         vt.FailReason.String,
		Stage:              stage,
		CheckpointTxs:      checkpointTxs,
		CommitmentTxids:    commitmentTxids,
		RootCommitmentTxId: rootCommitmentTxId,
	}, nil
}

func (v *offchainTxRepository) GetOffchainTxsWithSpentInputsAndNoOuts(
	ctx context.Context,
) (map[string][]domain.Event, error) {
	rows, err := v.db.QueryContext(ctx, selectOffchainTxEventsForSpentInsAndNoOuts)
	if err != nil {
		return nil, err
	}
	var items []struct {
		PayloadId    string
		Type100Count int64
		Type101Count int64
		Type102Count int64
		Type103Count int64
		AllEvents    string
	}
	for rows.Next() {
		var i struct {
			PayloadId    string
			Type100Count int64
			Type101Count int64
			Type102Count int64
			Type103Count int64
			AllEvents    string
		}
		if err := rows.Scan(
			&i.PayloadId,
			&i.Type100Count,
			&i.Type101Count,
			&i.Type102Count,
			&i.Type103Count,
			&i.AllEvents,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make(map[string][]domain.Event)
	for _, i := range items {
		events, err := parseEvents([]byte(i.AllEvents))
		if err != nil {
			return nil, err
		}
		result[i.PayloadId] = events
	}
	return result, nil
}

func (v *offchainTxRepository) GetOffchainTxsWithUnspentOrDoubleSpentInputsAndNoOuts(
	ctx context.Context,
) (map[string][]domain.Event, error) {
	rows, err := v.db.QueryContext(ctx, selectOffchainTxEventsForUnspentOrDoubleSpentInsAndNoOuts)
	if err != nil {
		return nil, err
	}
	var items []struct {
		PayloadId    string
		Type100Count int64
		Type101Count int64
		Type102Count int64
		Type103Count int64
		AllEvents    string
	}
	for rows.Next() {
		var i struct {
			PayloadId    string
			Type100Count int64
			Type101Count int64
			Type102Count int64
			Type103Count int64
			AllEvents    string
		}
		if err := rows.Scan(
			&i.PayloadId,
			&i.Type100Count,
			&i.Type101Count,
			&i.Type102Count,
			&i.Type103Count,
			&i.AllEvents,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make(map[string][]domain.Event)
	for _, i := range items {
		events, err := parseEvents([]byte(i.AllEvents))
		if err != nil {
			return nil, err
		}
		result[i.PayloadId] = events
	}
	return result, nil
}

func (v *offchainTxRepository) DeleteTxs(ctx context.Context, txids []string) error {
	return v.querier.DeleteOffchainTxs(ctx, txids)
}

func (v *offchainTxRepository) Close() {
	_ = v.db.Close()
}

const selectOffchainTxEventsForSpentInsAndNoOuts = `
SELECT payload_id, type_100_count, type_101_count, type_102_count, type_103_count, all_events
FROM (
  SELECT 
    (o.payload::jsonb ->> 'Id') AS payload_id,
    SUM(CASE WHEN o.payload::jsonb ->> 'Type' = '100' THEN 1 ELSE 0 END) AS type_100_count,
    SUM(CASE WHEN o.payload::jsonb ->> 'Type' = '101' THEN 1 ELSE 0 END) AS type_101_count,
    SUM(CASE WHEN o.payload::jsonb ->> 'Type' = '102' THEN 1 ELSE 0 END) AS type_102_count,
    SUM(CASE WHEN o.payload::jsonb ->> 'Type' = '103' THEN 1 ELSE 0 END) AS type_103_count,
    json_agg(o.payload::jsonb ORDER BY o.offset ASC) AS all_events
  FROM watermill_offchain_tx AS o
  GROUP BY (o.payload::jsonb ->> 'Id')
) sub
WHERE type_100_count >= 1 -- more than 1 submit event
  AND type_101_count >= 1 -- at least 1 accepted event
  AND type_102_count >= 1 -- at least 1 finalized event
  AND type_103_count >= 0 -- at least 1 failed event
  AND NOT EXISTS (
    SELECT 1
    FROM vtxo
    WHERE vtxo.txid = payload_id
  )
  AND EXISTS (
    SELECT 1
    FROM vtxo
    WHERE vtxo.ark_txid = payload_id
    AND vtxo.unrolled = FALSE
  )
  AND EXISTS (
    SELECT 1 FROM offchain_tx WHERE offchain_tx.txid = payload_id 
  );`

const selectOffchainTxEventsForUnspentOrDoubleSpentInsAndNoOuts = `
SELECT payload_id, type_100_count, type_101_count, type_102_count, type_103_count, all_events
FROM (
  SELECT 
    (o.payload::jsonb ->> 'Id') AS payload_id,
    SUM(CASE WHEN o.payload::jsonb ->> 'Type' = '100' THEN 1 ELSE 0 END) AS type_100_count,
    SUM(CASE WHEN o.payload::jsonb ->> 'Type' = '101' THEN 1 ELSE 0 END) AS type_101_count,
    SUM(CASE WHEN o.payload::jsonb ->> 'Type' = '102' THEN 1 ELSE 0 END) AS type_102_count,
    SUM(CASE WHEN o.payload::jsonb ->> 'Type' = '103' THEN 1 ELSE 0 END) AS type_103_count,
    json_agg(o.payload::jsonb ORDER BY o.offset ASC) AS all_events
  FROM watermill_offchain_tx AS o
  GROUP BY (o.payload::jsonb ->> 'Id')
) sub
WHERE type_100_count >= 1 -- more than 1 submit event
  AND type_101_count >= 1 -- at least 1 accepted event
  AND type_102_count >= 1 -- at least 1 finalized event
  AND type_103_count >= 0 -- at least 1 failed event
  AND NOT EXISTS (
    SELECT 1
    FROM vtxo
    WHERE vtxo.txid = payload_id
  )
  AND NOT EXISTS (
    SELECT 1
    FROM vtxo
    WHERE vtxo.ark_txid = payload_id
    AND vtxo.unrolled = FALSE
    AND spent = TRUE
  )
  AND EXISTS (
    SELECT 1 FROM offchain_tx WHERE offchain_tx.txid = payload_id 
  );`

const deleteTxs = `DELETE FROM offchain_tx WHERE txid = ANY($1)`

func parseEvents(data []byte) ([]domain.Event, error) {
	// First, unmarshal into a slice of raw JSON values.
	var rawEvents []json.RawMessage
	if err := json.Unmarshal(data, &rawEvents); err != nil {
		return nil, fmt.Errorf("unmarshal events array: %w", err)
	}

	events := make([]domain.Event, 0, len(rawEvents))

	for _, raw := range rawEvents {
		// Minimal header to detect type
		var hdr struct {
			Type domain.EventType `json:"Type"`
		}
		if err := json.Unmarshal(raw, &hdr); err != nil {
			return nil, fmt.Errorf("unmarshal event header: %w", err)
		}

		switch hdr.Type {
		case domain.EventTypeOffchainTxRequested:
			var ev domain.OffchainTxRequested
			if err := json.Unmarshal(raw, &ev); err != nil {
				return nil, fmt.Errorf("unmarshal OffchainTxRequested: %w", err)
			}
			events = append(events, ev)

		case domain.EventTypeOffchainTxAccepted:
			var ev domain.OffchainTxAccepted
			if err := json.Unmarshal(raw, &ev); err != nil {
				return nil, fmt.Errorf("unmarshal OffchainTxAccepted: %w", err)
			}
			events = append(events, ev)

		case domain.EventTypeOffchainTxFinalized:
			var ev domain.OffchainTxFinalized
			if err := json.Unmarshal(raw, &ev); err != nil {
				return nil, fmt.Errorf("unmarshal OffchainTxFinalized: %w", err)
			}
			events = append(events, ev)

		case domain.EventTypeOffchainTxFailed:
			var ev domain.OffchainTxFailed
			if err := json.Unmarshal(raw, &ev); err != nil {
				return nil, fmt.Errorf("unmarshal OffchainTxFailed: %w", err)
			}
			events = append(events, ev)

		default:
			return nil, fmt.Errorf("unknown event type: %d", hdr.Type)
		}
	}

	return events, nil
}
