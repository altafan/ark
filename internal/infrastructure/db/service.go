package db

import (
	"bytes"
	"context"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/arkade-os/arkd/internal/core/domain"
	"github.com/arkade-os/arkd/internal/core/ports"
	badgerdb "github.com/arkade-os/arkd/internal/infrastructure/db/badger"
	pgdb "github.com/arkade-os/arkd/internal/infrastructure/db/postgres"
	sqlitedb "github.com/arkade-os/arkd/internal/infrastructure/db/sqlite"
	"github.com/arkade-os/arkd/pkg/ark-lib/script"
	"github.com/arkade-os/arkd/pkg/ark-lib/tree"
	"github.com/arkade-os/arkd/pkg/ark-lib/txutils"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/btcsuite/btcd/btcutil/psbt"
	"github.com/golang-migrate/migrate/v4"
	migratepg "github.com/golang-migrate/migrate/v4/database/postgres"
	sqlitemigrate "github.com/golang-migrate/migrate/v4/database/sqlite"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	log "github.com/sirupsen/logrus"
)

//go:embed sqlite/migration/*
var migrations embed.FS

//go:embed postgres/migration/*
var pgMigration embed.FS

var arkRepo badgerdb.ArkRepository

var (
	eventStoreTypes = map[string]func(...interface{}) (domain.EventRepository, error){
		"badger":   badgerdb.NewEventRepository,
		"postgres": pgdb.NewEventRepository,
	}
	roundStoreTypes = map[string]func(...interface{}) (domain.RoundRepository, error){
		"badger":   newBadgerRoundRepository,
		"sqlite":   sqlitedb.NewRoundRepository,
		"postgres": pgdb.NewRoundRepository,
	}
	vtxoStoreTypes = map[string]func(...interface{}) (domain.VtxoRepository, error){
		"badger":   badgerdb.NewVtxoRepository,
		"sqlite":   sqlitedb.NewVtxoRepository,
		"postgres": pgdb.NewVtxoRepository,
	}
	scheduledSessionStoreTypes = map[string]func(...interface{}) (domain.ScheduledSessionRepo, error){
		"badger":   badgerdb.NewScheduledSessionRepository,
		"sqlite":   sqlitedb.NewScheduledSessionRepository,
		"postgres": pgdb.NewScheduledSessionRepository,
	}
	offchainTxStoreTypes = map[string]func(...interface{}) (domain.OffchainTxRepository, error){
		"badger":   newBadgerOffchainTxRepository,
		"sqlite":   sqlitedb.NewOffchainTxRepository,
		"postgres": pgdb.NewOffchainTxRepository,
	}
	convictionStoreTypes = map[string]func(...interface{}) (domain.ConvictionRepository, error){
		"badger":   badgerdb.NewConvictionRepository,
		"sqlite":   sqlitedb.NewConvictionRepository,
		"postgres": pgdb.NewConvictionRepository,
	}
)

const (
	sqliteDbFile = "sqlite.db"
)

type ServiceConfig struct {
	EventStoreType string
	DataStoreType  string

	EventStoreConfig []interface{}
	DataStoreConfig  []interface{}
}

type service struct {
	eventStore            domain.EventRepository
	roundStore            domain.RoundRepository
	vtxoStore             domain.VtxoRepository
	scheduledSessionStore domain.ScheduledSessionRepo
	offchainTxStore       domain.OffchainTxRepository
	convictionStore       domain.ConvictionRepository
	txDecoder             ports.TxDecoder
}

func NewService(config ServiceConfig, txDecoder ports.TxDecoder) (ports.RepoManager, error) {
	eventStoreFactory, ok := eventStoreTypes[config.EventStoreType]
	if !ok {
		return nil, fmt.Errorf("event store type not supported")
	}
	roundStoreFactory, ok := roundStoreTypes[config.DataStoreType]
	if !ok {
		return nil, fmt.Errorf("round store type not supported")
	}
	vtxoStoreFactory, ok := vtxoStoreTypes[config.DataStoreType]
	if !ok {
		return nil, fmt.Errorf("vtxo store type not supported")
	}
	scheduledSessionStoreFactory, ok := scheduledSessionStoreTypes[config.DataStoreType]
	if !ok {
		return nil, fmt.Errorf("invalid data store type: %s", config.DataStoreType)
	}
	offchainTxStoreFactory, ok := offchainTxStoreTypes[config.DataStoreType]
	if !ok {
		return nil, fmt.Errorf("invalid data store type: %s", config.DataStoreType)
	}
	convictionStoreFactory, ok := convictionStoreTypes[config.DataStoreType]
	if !ok {
		return nil, fmt.Errorf("invalid data store type: %s", config.DataStoreType)
	}

	var eventStore domain.EventRepository
	var roundStore domain.RoundRepository
	var vtxoStore domain.VtxoRepository
	var scheduledSessionStore domain.ScheduledSessionRepo
	var offchainTxStore domain.OffchainTxRepository
	var convictionStore domain.ConvictionRepository
	var err error

	switch config.EventStoreType {
	case "badger":
		eventStore, err = eventStoreFactory(config.EventStoreConfig...)
		if err != nil {
			return nil, fmt.Errorf("failed to open event store: %s", err)
		}
	case "postgres":
		if len(config.DataStoreConfig) != 1 {
			return nil, fmt.Errorf("invalid data store config for postgres")
		}

		dsn, ok := config.DataStoreConfig[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid DSN for postgres")
		}

		db, err := pgdb.OpenDb(dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open postgres db: %s", err)
		}

		eventStore, err = eventStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to open event store: %s", err)
		}
	default:
		return nil, fmt.Errorf("unknown event store db type")
	}

	switch config.DataStoreType {
	case "badger":
		roundStore, err = roundStoreFactory(config.DataStoreConfig...)
		if err != nil {
			return nil, fmt.Errorf("failed to open round store: %s", err)
		}
		vtxoStore, err = vtxoStoreFactory(config.DataStoreConfig...)
		if err != nil {
			return nil, fmt.Errorf("failed to open vtxo store: %s", err)
		}
		scheduledSessionStore, err = scheduledSessionStoreFactory(config.DataStoreConfig...)
		if err != nil {
			return nil, fmt.Errorf("failed to create scheduled session store: %w", err)
		}
		offchainTxStore, err = offchainTxStoreFactory(config.DataStoreConfig...)
		if err != nil {
			return nil, fmt.Errorf("failed to create offchain tx store: %w", err)
		}
		convictionStore, err = convictionStoreFactory(config.DataStoreConfig...)
		if err != nil {
			return nil, fmt.Errorf("failed to create conviction store: %w", err)
		}
	case "postgres":
		if len(config.DataStoreConfig) != 1 {
			return nil, fmt.Errorf("invalid data store config for postgres")
		}

		dsn, ok := config.DataStoreConfig[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid DSN for postgres")
		}

		db, err := pgdb.OpenDb(dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open postgres db: %s", err)
		}

		pgDriver, err := migratepg.WithInstance(db, &migratepg.Config{})
		if err != nil {
			return nil, fmt.Errorf("failed to init postgres migration driver: %s", err)
		}

		source, err := iofs.New(pgMigration, "postgres/migration")
		if err != nil {
			return nil, fmt.Errorf("failed to embed postgres migrations: %s", err)
		}

		m, err := migrate.NewWithInstance("iofs", source, "postgres", pgDriver)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgres migration instance: %s", err)
		}

		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return nil, fmt.Errorf("failed to run postgres migrations: %s", err)
		}

		roundStore, err = roundStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to open round store: %s", err)
		}

		vtxoStore, err = vtxoStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to open vtxo store: %s", err)
		}

		scheduledSessionStore, err = scheduledSessionStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to create scheduled session store: %w", err)
		}

		offchainTxStore, err = offchainTxStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to create offchain tx store: %w", err)
		}
		convictionStore, err = convictionStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to create conviction store: %w", err)
		}
	case "sqlite":
		if len(config.DataStoreConfig) != 1 {
			return nil, fmt.Errorf("invalid data store config")
		}

		baseDir, ok := config.DataStoreConfig[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid base directory")
		}

		dbFile := filepath.Join(baseDir, sqliteDbFile)
		db, err := sqlitedb.OpenDb(dbFile)
		if err != nil {
			return nil, fmt.Errorf("failed to open db: %s", err)
		}

		driver, err := sqlitemigrate.WithInstance(db, &sqlitemigrate.Config{})
		if err != nil {
			return nil, fmt.Errorf("failed to init driver: %s", err)
		}

		source, err := iofs.New(migrations, "sqlite/migration")
		if err != nil {
			return nil, fmt.Errorf("failed to embed migrations: %s", err)
		}

		m, err := migrate.NewWithInstance("iofs", source, "arkdb", driver)
		if err != nil {
			return nil, fmt.Errorf("failed to create migration instance: %s", err)
		}

		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return nil, fmt.Errorf("failed to run migrations: %s", err)
		}

		roundStore, err = roundStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to open round store: %s", err)
		}
		vtxoStore, err = vtxoStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to open vtxo store: %s", err)
		}
		scheduledSessionStore, err = scheduledSessionStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to create scheduled session store: %w", err)
		}
		offchainTxStore, err = offchainTxStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to create offchain tx store: %w", err)
		}
		convictionStore, err = convictionStoreFactory(db)
		if err != nil {
			return nil, fmt.Errorf("failed to create conviction store: %w", err)
		}
	}

	svc := &service{
		eventStore:            eventStore,
		roundStore:            roundStore,
		vtxoStore:             vtxoStore,
		scheduledSessionStore: scheduledSessionStore,
		offchainTxStore:       offchainTxStore,
		txDecoder:             txDecoder,
		convictionStore:       convictionStore,
	}

	// Register handlers that take care of keeping the projection store up-to-date.
	if txDecoder != nil {
		eventStore.RegisterEventsHandler(domain.RoundTopic, svc.updateProjectionsAfterRoundEvents)
		eventStore.RegisterEventsHandler(
			domain.OffchainTxTopic, svc.updateProjectionsAfterOffchainTxEvents,
		)
	}

	go func() {
		if err := svc.fixOffchainTxsWithSpentInputsAndNoOuts(context.Background()); err != nil {
			fmt.Printf("failed to fix offchain txs with spent inputs and no outs: %s\n", err)
		}

		if err := svc.fixOffchainTxsWithUnspentOrDoubleSpentInputsAndNoOuts(context.Background()); err != nil {
			fmt.Printf("failed to fix offchain txs with unspent or double spent inputs and no outs: %s\n", err)
		}
	}()

	return svc, nil
}

func (s *service) Events() domain.EventRepository {
	return s.eventStore
}

func (s *service) Rounds() domain.RoundRepository {
	return s.roundStore
}

func (s *service) Vtxos() domain.VtxoRepository {
	return s.vtxoStore
}

func (s *service) ScheduledSession() domain.ScheduledSessionRepo {
	return s.scheduledSessionStore
}

func (s *service) OffchainTxs() domain.OffchainTxRepository {
	return s.offchainTxStore
}

func (s *service) Convictions() domain.ConvictionRepository {
	return s.convictionStore
}

func (s *service) Close() {
	s.eventStore.Close()
	s.roundStore.Close()
	s.vtxoStore.Close()
	s.scheduledSessionStore.Close()
	s.offchainTxStore.Close()
	s.convictionStore.Close()
}

func (s *service) updateProjectionsAfterRoundEvents(events []domain.Event) {
	ctx := context.Background()
	round := domain.NewRoundFromEvents(events)

	if err := s.roundStore.AddOrUpdateRound(ctx, *round); err != nil {
		log.WithError(err).Errorf("failed to add or update round %s", round.Id)
		return
	}
	log.Debugf("added or updated round %s", round.Id)

	if !round.IsEnded() {
		return
	}

	repo := s.vtxoStore

	lastEvent := events[len(events)-1]
	if lastEvent.GetType() == domain.EventTypeBatchSwept {
		event := lastEvent.(domain.BatchSwept)
		allSweptVtxos := append(event.LeafVtxos, event.PreconfirmedVtxos...)
		sweptCount, err := repo.SweepVtxos(ctx, allSweptVtxos)
		if err != nil {
			log.WithError(err).Warn("failed to sweep vtxos")
		} else {
			log.Debugf("swept %d vtxos", sweptCount)
		}

		if event.FullySwept {
			log.WithField("commitment_txid", round.CommitmentTxid).Debugf(
				"round %s fully swept", round.Id,
			)
		}
		return
	}

	spentVtxos := getSpentVtxoKeysFromRound(*round, s.txDecoder)
	newVtxos := getNewVtxosFromRound(round)

	if len(spentVtxos) > 0 {
		for {
			if err := repo.SettleVtxos(ctx, spentVtxos, round.CommitmentTxid); err != nil {
				log.WithError(err).Warn("failed to spend vtxos, retrying...")
				time.Sleep(100 * time.Millisecond)
				continue
			}
			log.Debugf("spent %d vtxos", len(spentVtxos))
			break
		}
	}

	if len(newVtxos) > 0 {
		for {
			if err := repo.AddVtxos(ctx, newVtxos); err != nil {
				log.WithError(err).Warn("failed to add new vtxos, retrying soon")
				time.Sleep(100 * time.Millisecond)
				continue
			}
			log.Debugf("added %d new vtxos", len(newVtxos))
			break
		}
	}
}

func (s *service) updateProjectionsAfterOffchainTxEvents(events []domain.Event) {
	ctx := context.Background()
	offchainTx := domain.NewOffchainTxFromEvents(events)

	if err := s.offchainTxStore.AddOrUpdateOffchainTx(ctx, offchainTx); err != nil {
		log.WithError(err).Errorf("failed to add or update offchain tx %s", offchainTx.ArkTxid)
		return
	}
	log.Debugf("added or updated offchain tx %s", offchainTx.ArkTxid)

	switch {
	case offchainTx.IsAccepted():
		spentVtxos := make(map[domain.Outpoint]string)
		for _, tx := range offchainTx.CheckpointTxs {
			txid, ins, _, err := s.txDecoder.DecodeTx(tx)
			if err != nil {
				log.WithError(err).Warn("failed to decode checkpoint tx")
				continue
			}
			for _, in := range ins {
				spentVtxos[in] = txid
			}
		}

		// as soon as the checkpoint txs are signed by the signer,
		// we must mark the vtxos as spent to prevent double spending.
		if err := s.vtxoStore.SpendVtxos(ctx, spentVtxos, offchainTx.ArkTxid); err != nil {
			log.WithError(err).Warn("failed to spend vtxos")
			return
		}
		log.Debugf("spent %d vtxos", len(spentVtxos))
	case offchainTx.IsFinalized():
		txid, _, outs, err := s.txDecoder.DecodeTx(offchainTx.ArkTx)
		if err != nil {
			log.WithError(err).Warn("failed to decode ark tx")
			return
		}

		// once the offchain tx is finalized, the user signed the checkpoint txs
		// thus, we can create the new vtxos in the db.
		newVtxos := make([]domain.Vtxo, 0, len(outs))
		for outIndex, out := range outs {
			// ignore anchors
			if bytes.Equal(out.PkScript, txutils.ANCHOR_PKSCRIPT) {
				continue
			}

			isDust := script.IsSubDustScript(out.PkScript)

			newVtxos = append(newVtxos, domain.Vtxo{
				Outpoint: domain.Outpoint{
					Txid: txid,
					VOut: uint32(outIndex),
				},
				PubKey:             hex.EncodeToString(out.PkScript[2:]),
				Amount:             uint64(out.Amount),
				ExpiresAt:          offchainTx.ExpiryTimestamp,
				CommitmentTxids:    offchainTx.CommitmentTxidsList(),
				RootCommitmentTxid: offchainTx.RootCommitmentTxId,
				Preconfirmed:       true,
				CreatedAt:          offchainTx.StartingTimestamp,
				// mark the vtxo as "swept" if it is below dust limit to prevent it from being spent again in a future offchain tx
				// the only way to spend a swept vtxo is by collecting enough dust to cover the minSettlementVtxoAmount and then settle.
				// because sub-dust vtxos are using OP_RETURN output script, they can't be unilaterally exited.
				Swept: isDust,
			})
		}

		if err := s.vtxoStore.AddVtxos(ctx, newVtxos); err != nil {
			log.WithError(err).Warn("failed to add vtxos")
			return
		}
		log.Debugf("added %d vtxos", len(newVtxos))
	}
}

func (s *service) updateProjectionsAfterReplayingOffchainTxEvents(events []domain.Event) {
	ctx := context.Background()
	offchainTx := domain.NewOffchainTxFromEvents(events)

	if err := s.offchainTxStore.AddOrUpdateOffchainTx(ctx, offchainTx); err != nil {
		log.WithError(err).Errorf("failed to add or update offchain tx %s", offchainTx.ArkTxid)
		return
	}
	log.Debugf("added or updated offchain tx %s", offchainTx.ArkTxid)

	switch {
	case offchainTx.IsAccepted():
		spentVtxos := make(map[domain.Outpoint]string)
		for _, tx := range offchainTx.CheckpointTxs {
			txid, ins, _, err := s.txDecoder.DecodeTx(tx)
			if err != nil {
				log.WithError(err).Warn("failed to decode checkpoint tx")
				continue
			}
			for _, in := range ins {
				spentVtxos[in] = txid
			}
		}

		if err := s.vtxoStore.SpendVtxos(ctx, spentVtxos, offchainTx.ArkTxid); err != nil {
			log.WithError(err).Warn("failed to spend vtxos")
			return
		}
		log.Debugf("spent %d vtxos", len(spentVtxos))
	case offchainTx.IsFinalized():
		spentVtxos := make(map[domain.Outpoint]string)
		for _, tx := range offchainTx.CheckpointTxs {
			txid, ins, _, err := s.txDecoder.DecodeTx(tx)
			if err != nil {
				log.WithError(err).Warn("failed to decode checkpoint tx")
				continue
			}
			for _, in := range ins {
				spentVtxos[in] = txid
			}
		}

		txid, _, outs, err := s.txDecoder.DecodeTx(offchainTx.ArkTx)
		if err != nil {
			log.WithError(err).Warn("failed to decode ark tx")
			return
		}

		newVtxos := make([]domain.Vtxo, 0, len(outs))
		for outIndex, out := range outs {
			// ignore anchors
			if bytes.Equal(out.PkScript, txutils.ANCHOR_PKSCRIPT) {
				continue
			}

			isDust := script.IsSubDustScript(out.PkScript)

			newVtxos = append(newVtxos, domain.Vtxo{
				Outpoint: domain.Outpoint{
					Txid: txid,
					VOut: uint32(outIndex),
				},
				PubKey:             hex.EncodeToString(out.PkScript[2:]),
				Amount:             uint64(out.Amount),
				ExpiresAt:          offchainTx.ExpiryTimestamp,
				CommitmentTxids:    offchainTx.CommitmentTxidsList(),
				RootCommitmentTxid: offchainTx.RootCommitmentTxId,
				Preconfirmed:       true,
				CreatedAt:          offchainTx.StartingTimestamp,
				// mark the vtxo as "swept" if it is below dust limit to prevent it from being spent again in a future offchain tx
				// the only way to spend a swept vtxo is by collecting enough dust to cover the minSettlementVtxoAmount and then settle.
				// because sub-dust vtxos are using OP_RETURN output script, they can't be unilaterally exited.
				Swept: isDust,
			})
		}

		if err := s.vtxoStore.SpendVtxos(ctx, spentVtxos, offchainTx.ArkTxid); err != nil {
			log.WithError(err).Warn("failed to spend vtxos")
			return
		}
		log.Debugf("spent %d vtxos", len(spentVtxos))
		if err := s.vtxoStore.AddVtxos(ctx, newVtxos); err != nil {
			log.WithError(err).Warn("failed to add vtxos")
			return
		}
		log.Debugf("added %d vtxos", len(newVtxos))
	}
}

func (s *service) fixOffchainTxsWithSpentInputsAndNoOuts(ctx context.Context) error {
	ev, err := s.offchainTxStore.GetOffchainTxsWithSpentInputsAndNoOuts(ctx)
	if err != nil {
		return fmt.Errorf("failed to get offchain txs with spent inputs and no outs: %s", err)
	}

	if len(ev) == 0 {
		fmt.Printf("FOUND %d OFFCHAIN TXS WITH SPENT INPUT VTXOS AND NO OUT VTXOS\n", len(ev))
		fmt.Printf("NO RECORDS TO UPDATE\n")
	} else {
		outpoints := make([]domain.Outpoint, 0)
		for txid, events := range ev {
			s.updateProjectionsAfterOffchainTxEvents(events)
			outpoints = append(outpoints, []domain.Outpoint{
				{Txid: txid, VOut: 0}, {Txid: txid, VOut: 1},
			}...)
			time.Sleep(50 * time.Millisecond)
		}
		fmt.Printf("FOUND %d OFFCHAIN TXS WITH SPENT INPUT VTXOS AND NO OUT VTXOS\n", len(ev))
		fmt.Printf("UPDATED %d RECORDS\n", len(ev))

		time.Sleep(2 * time.Second)

		vtxos, err := s.vtxoStore.GetVtxos(ctx, outpoints)
		if err != nil {
			return nil
		}
		fmt.Printf("ADDED %d VTXOS\n", len(vtxos))

		indexedVtxos := map[string][]domain.Vtxo{}
		for _, vtxo := range vtxos {
			indexedVtxos[vtxo.RootCommitmentTxid] = append(indexedVtxos[vtxo.RootCommitmentTxid], vtxo)
		}

		vtxosToSweep := make([]domain.Outpoint, 0)
		for commitmentTxid, targets := range indexedVtxos {
			round, err := s.roundStore.GetRoundWithCommitmentTxid(ctx, commitmentTxid)
			if err != nil {
				fmt.Printf("failed to get round with commitment txid %s: %s\n", commitmentTxid, err)
				return nil
			}
			if round.Swept {
				for _, vtxo := range targets {
					vtxosToSweep = append(vtxosToSweep, vtxo.Outpoint)
				}
			}
		}

		if len(vtxosToSweep) == 0 {
			fmt.Printf("NO VTXOS TO SWEEP\n")
		} else {
			count, err := s.vtxoStore.SweepVtxos(ctx, vtxosToSweep)
			if err != nil {
				fmt.Printf("failed to sweep vtxos: %s\n", err)
				return nil
			}
			fmt.Printf("SWEPT %d/%d VTXOS\n", count, len(vtxos))
		}
		fmt.Println("DONE")
	}
	return nil
}

func (s *service) fixOffchainTxsWithUnspentOrDoubleSpentInputsAndNoOuts(ctx context.Context) error {
	ev, err := s.offchainTxStore.GetOffchainTxsWithUnspentOrDoubleSpentInputsAndNoOuts(ctx)
	if err != nil {
		return fmt.Errorf("failed to get offchain txs with spent inputs: %s", err)
	}
	fmt.Printf("FOUND %d OFFCHAIN TXS WITH UNSPENT OR DOUBLE SPENT INPUT VTXOS AND NO OUT VTXOS\n", len(ev))
	outpoints := make([]domain.Outpoint, 0)
	expectedArkTxid := make(map[string]string)
	for id, events := range ev {
		tx := domain.NewOffchainTxFromEvents(events)
		for _, t := range tx.CheckpointTxs {
			ptx, err := psbt.NewFromRawBytes(strings.NewReader(t), true)
			if err != nil {
				return fmt.Errorf("failed to decode checkpoint tx for offchain tx %s: %s", id, err)
			}
			for _, in := range ptx.UnsignedTx.TxIn {
				outpoint := domain.Outpoint{
					Txid: in.PreviousOutPoint.Hash.String(),
					VOut: in.PreviousOutPoint.Index,
				}
				outpoints = append(outpoints, outpoint)
				expectedArkTxid[outpoint.String()] = id
			}
		}
	}
	vtxos, err := s.vtxoStore.GetVtxos(ctx, outpoints)
	if err != nil {
		return fmt.Errorf("failed to get vtxos: %s", err)
	}

	unspentCount := 0
	txids := make([]string, 0)
	txidsOfUnspent := make([]string, 0)
	for _, v := range vtxos {
		if v.ArkTxid == "" && v.SpentBy == "" && v.SettledBy == "" {
			unspentCount++
			txidsOfUnspent = append(txidsOfUnspent, expectedArkTxid[v.Outpoint.String()])
			continue
		}
		if v.ArkTxid != expectedArkTxid[v.Outpoint.String()] {
			txids = append(txids, expectedArkTxid[v.Outpoint.String()])
		}
	}

	if len(txids) == 0 {
		fmt.Println("NO DOUBLE-SPEND TXS TO DELETE")
	} else {
		if err := s.offchainTxStore.DeleteTxs(ctx, txids); err != nil {
			return fmt.Errorf("failed to delete offchain txs: %s", err)
		}
		fmt.Printf("DELETED %d DOUBLE-SPEND TXS\n", len(txids))
	}

	if len(txidsOfUnspent) == 0 {
		fmt.Printf("NO TXS DETECTED THAT HAVE UNSPENT INPUTS AND NO OUTS IN VTXO TABLE\n")
	} else {
		outpoints := make([]domain.Outpoint, 0)
		for _, txid := range txidsOfUnspent {
			s.updateProjectionsAfterReplayingOffchainTxEvents(ev[txid])
			outpoints = append(outpoints, []domain.Outpoint{
				{Txid: txid, VOut: 0}, {Txid: txid, VOut: 1},
			}...)
			time.Sleep(50 * time.Millisecond)
		}
		fmt.Printf("UPDATED %d TXS THAT HAD UNSPENT INPUTS AND NO OUTS IN VTXO TABLE\n", len(txidsOfUnspent))

		vtxos, err := s.vtxoStore.GetVtxos(ctx, outpoints)
		if err != nil {
			return nil
		}
		fmt.Printf("SPENT %d VTXOS\n", len(txidsOfUnspent))
		fmt.Printf("ADDED %d VTXOS\n", len(vtxos))

		indexedVtxos := map[string][]domain.Vtxo{}
		for _, vtxo := range vtxos {
			indexedVtxos[vtxo.RootCommitmentTxid] = append(indexedVtxos[vtxo.RootCommitmentTxid], vtxo)
		}

		vtxosToSweep := make([]domain.Outpoint, 0)
		for commitmentTxid, targets := range indexedVtxos {
			round, err := s.roundStore.GetRoundWithCommitmentTxid(ctx, commitmentTxid)
			if err != nil {
				fmt.Printf("failed to get round with commitment txid %s: %s\n", commitmentTxid, err)
				return nil
			}
			if round.Swept {
				for _, vtxo := range targets {
					vtxosToSweep = append(vtxosToSweep, vtxo.Outpoint)
				}
			}
		}

		if len(vtxosToSweep) == 0 {
			fmt.Printf("NO VTXOS TO SWEEP\n")
		} else {
			count, err := s.vtxoStore.SweepVtxos(ctx, vtxosToSweep)
			if err != nil {
				fmt.Printf("failed to sweep vtxos: %s\n", err)
				return nil
			}
			fmt.Printf("SWEPT %d/%d VTXOS\n", count, len(vtxos))
		}
	}

	fmt.Println("DONE")
	return nil
}

func getSpentVtxoKeysFromRound(
	round domain.Round, txDecoder ports.TxDecoder,
) map[domain.Outpoint]string {
	spentVtxos := make(map[domain.Outpoint]string)

	// Build a map of forfeit tx inputs for O(1) lookup
	forfeitInputs := make(map[domain.Outpoint]string)
	for _, forfeitTx := range round.ForfeitTxs {
		_, ins, _, err := txDecoder.DecodeTx(forfeitTx.Tx)
		if err != nil {
			log.WithError(err).Warnf("failed to decode forfeit tx %s", forfeitTx.Txid)
			continue
		}
		for _, in := range ins {
			forfeitInputs[in] = forfeitTx.Txid
		}
	}

	// Match vtxos with forfeit transactions
	for _, intent := range round.Intents {
		for _, vtxo := range intent.Inputs {
			if !vtxo.RequiresForfeit() {
				spentVtxos[vtxo.Outpoint] = ""
			} else if txid, found := forfeitInputs[vtxo.Outpoint]; found {
				spentVtxos[vtxo.Outpoint] = txid
			}
		}
	}
	return spentVtxos
}

func getNewVtxosFromRound(round *domain.Round) []domain.Vtxo {
	if len(round.VtxoTree) <= 0 {
		return nil
	}

	vtxos := make([]domain.Vtxo, 0)
	for _, node := range tree.FlatTxTree(round.VtxoTree).Leaves() {
		tx, err := psbt.NewFromRawBytes(strings.NewReader(node.Tx), true)
		if err != nil {
			log.WithError(err).Warn("failed to parse tx")
			continue
		}
		for i, out := range tx.UnsignedTx.TxOut {
			// ignore anchors
			if bytes.Equal(out.PkScript, txutils.ANCHOR_PKSCRIPT) {
				continue
			}

			vtxoTapKey, err := schnorr.ParsePubKey(out.PkScript[2:])
			if err != nil {
				log.WithError(err).Warn("failed to parse vtxo tap key")
				continue
			}

			vtxoPubkey := hex.EncodeToString(schnorr.SerializePubKey(vtxoTapKey))
			vtxos = append(vtxos, domain.Vtxo{
				Outpoint:           domain.Outpoint{Txid: tx.UnsignedTx.TxID(), VOut: uint32(i)},
				PubKey:             vtxoPubkey,
				Amount:             uint64(out.Value),
				CommitmentTxids:    []string{round.CommitmentTxid},
				RootCommitmentTxid: round.CommitmentTxid,
				CreatedAt:          round.EndingTimestamp,
				ExpiresAt:          round.ExpiryTimestamp(),
			})
		}
	}
	return vtxos
}

func initBadgerArkRepository(args ...interface{}) (badgerdb.ArkRepository, error) {
	if arkRepo == nil {
		repo, err := badgerdb.NewArkRepository(args...)
		if err != nil {
			return nil, err
		}
		arkRepo = repo
	}
	return arkRepo, nil
}

func newBadgerRoundRepository(args ...interface{}) (domain.RoundRepository, error) {
	return initBadgerArkRepository(args...)
}

func newBadgerOffchainTxRepository(args ...interface{}) (domain.OffchainTxRepository, error) {
	return initBadgerArkRepository(args...)
}
