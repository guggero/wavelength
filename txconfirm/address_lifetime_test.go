package txconfirm

import (
	"context"
	"fmt"
	"testing"

	"github.com/lightninglabs/wavelength/walletcore"
	"github.com/stretchr/testify/require"
)

// allocatingWallet records address issuance while preserving the existing
// wallet double's funding, signing, and lease behavior. Every successful call
// returns a different P2TR script, exposing accidental per-attempt allocation.
type allocatingWallet struct {
	*fakeWallet
	scriptCalls int
	scriptErr   error
}

// NewWalletPkScript counts even failed allocation requests. Failed requests
// provide no script to cache; successful requests produce distinct scripts.
func (w *allocatingWallet) NewWalletPkScript(_ context.Context) ([]byte,
	error) {

	w.scriptCalls++
	if w.scriptErr != nil {
		return nil, w.scriptErr
	}

	script := append([]byte{0x51, 0x20}, make([]byte, 32)...)
	script[len(script)-1] = byte(w.scriptCalls)

	return script, nil
}

// TestCPFPAddressLifetime drives shortages, accepted replacements and fee-cap
// refusals through Submit. The destination belongs to the parent, not a fee
// input reservation or successful package, and is discarded only on eviction.
func TestCPFPAddressLifetime(t *testing.T) {
	t.Parallel()

	chain := newFakeChainSourceRef(100)
	wallet := &allocatingWallet{fakeWallet: &fakeWallet{}}
	b := NewCPFPBroadcaster(BroadcasterConfig{
		ChainSource:           chain,
		Wallet:                wallet,
		MaxFeeRateSatPerVByte: 6,
	})
	parent := makeTestTx(true)
	req := &BroadcastRequest{Tx: parent}
	for range 20 {
		_, err := b.Submit(t.Context(), 100, req)
		require.ErrorIs(t, err, ErrCPFPFeeInputUnavailable)
	}
	require.Equal(t, 1, wallet.scriptCalls)
	require.Empty(t, b.parentStates, "allocation is not fee history")

	wallet.utxos = []*walletcore.Utxo{makeWalletUTXO(t)}
	first, err := b.Submit(t.Context(), 102, req)
	require.NoError(t, err)
	require.EqualValues(t, 5, first.FeeRate)
	second, err := b.Submit(t.Context(), 104, req)
	require.NoError(t, err)
	require.EqualValues(t, 6, second.FeeRate)
	for range 20 {
		_, err := b.Submit(t.Context(), 106, req)
		require.ErrorIs(t, err, ErrFeeRateCapReached)
	}
	require.Equal(t, 1, wallet.scriptCalls)
	require.Equal(t, 2, chain.packageCallCount())
	require.Equal(
		t, chain.packageCalls[0].Child.TxOut[0].PkScript,
		chain.packageCalls[1].Child.TxOut[0].PkScript,
	)

	b.Evict(t.Context(), parent.TxHash())
	require.Empty(t, b.changeScripts)
	_, err = b.Submit(t.Context(), 108, req)
	require.NoError(t, err)
	require.Equal(t, 2, wallet.scriptCalls)
}

// TestCPFPRejectedPackageKeepsAddress proves fee-input cleanup on a rejected
// package does not remove its parent's cached destination or fabricate prior
// fee history. A later successful retry uses the original output script.
func TestCPFPRejectedPackageKeepsAddress(t *testing.T) {
	t.Parallel()

	chain := newFakeChainSourceRef(100)
	chain.packageErr = fmt.Errorf("mempool temporarily full")
	wallet := &allocatingWallet{fakeWallet: &fakeWallet{
		utxos: []*walletcore.Utxo{
			makeWalletUTXO(t),
		},
	}}
	b := NewCPFPBroadcaster(BroadcasterConfig{
		ChainSource: chain,
		Wallet:      wallet,
	})
	req := &BroadcastRequest{Tx: makeTestTx(true)}
	for range 3 {
		_, err := b.Submit(t.Context(), 100, req)
		require.Error(t, err)
		require.Empty(t, b.parentStates)
	}
	chain.packageErr = nil
	_, err := b.Submit(t.Context(), 102, req)
	require.NoError(t, err)
	require.Equal(t, 1, wallet.scriptCalls)
	for _, call := range chain.packageCalls {
		require.Equal(
			t, chain.packageCalls[0].Child.TxOut[0].PkScript,
			call.Child.TxOut[0].PkScript,
		)
	}
}

// TestCPFPAddressCacheBoundaries pins independent parents, failed derivation,
// defensive copies, eviction without fee state, and the deliberate restart
// boundary: a new broadcaster may allocate again for the same parent.
func TestCPFPAddressCacheBoundaries(t *testing.T) {
	t.Parallel()

	wallet := &allocatingWallet{fakeWallet: &fakeWallet{},
		scriptErr: fmt.Errorf("wallet unavailable")}
	cfg := BroadcasterConfig{Wallet: wallet}
	b := NewCPFPBroadcaster(cfg)
	parentA := makeTestTxWithPrevoutSeed(1, true).TxHash()
	parentB := makeTestTxWithPrevoutSeed(2, true).TxHash()
	_, err := b.parentChangePkScript(t.Context(), parentA)
	require.Error(t, err)
	require.Empty(t, b.changeScripts)
	wallet.scriptErr = nil
	a, err := b.parentChangePkScript(t.Context(), parentA)
	require.NoError(t, err)
	original := append([]byte(nil), a...)
	a[0]++
	a, err = b.parentChangePkScript(t.Context(), parentA)
	require.NoError(t, err)
	require.Equal(t, original, a)
	other, err := b.parentChangePkScript(t.Context(), parentB)
	require.NoError(t, err)
	require.NotEqual(t, a, other)
	require.Equal(t, 3, wallet.scriptCalls)
	b.Evict(t.Context(), parentA)
	require.NotContains(t, b.changeScripts, parentA)
	require.Contains(t, b.changeScripts, parentB)
	restarted := NewCPFPBroadcaster(cfg)
	afterRestart, err := restarted.parentChangePkScript(
		t.Context(), parentA,
	)
	require.NoError(t, err)
	require.NotEqual(t, original, afterRestart)
	require.Equal(t, 4, wallet.scriptCalls)
}
