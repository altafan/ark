package domain

import "context"

type OffchainTxRepository interface {
	AddOrUpdateOffchainTx(ctx context.Context, offchainTx *OffchainTx) error
	GetOffchainTx(ctx context.Context, txid string) (*OffchainTx, error)
	GetOffchainTxsWithSpentInputsAndNoOuts(ctx context.Context) (map[string][]Event, error)
	GetOffchainTxsWithUnspentOrDoubleSpentInputsAndNoOuts(ctx context.Context) (map[string][]Event, error)
	DeleteTxs(ctx context.Context, txids []string) error
	Close()
}
