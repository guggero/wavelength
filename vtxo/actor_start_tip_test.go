package vtxo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/lightninglabs/wavelength/baselib/actor"
	"github.com/lightninglabs/wavelength/chainsource"
	"github.com/lightninglabs/wavelength/lib/actormsg"
	fn "github.com/lightningnetwork/lnd/fn/v2"
	"github.com/stretchr/testify/require"
)

// tipChainSourceRef accepts block subscriptions without ever publishing an
// epoch and answers best-height asks with a fixed tip. It models a VTXO
// spawned between two blocks, whose subscription stays silent until the
// next one.
type tipChainSourceRef struct {
	noopChainSourceRef

	height int32
}

// Ask answers best-height asks and defers everything else to the no-op
// chain source.
func (c *tipChainSourceRef) Ask(ctx context.Context,
	msg chainsource.ChainSourceMsg,
) actor.Future[chainsource.ChainSourceResp] {

	if _, ok := msg.(*chainsource.BestHeightRequest); !ok {
		return c.noopChainSourceRef.Ask(ctx, msg)
	}

	promise := actor.NewPromise[chainsource.ChainSourceResp]()
	promise.Complete(
		fn.Ok[chainsource.ChainSourceResp](
			&chainsource.BestHeightResponse{
				Height: c.height,
			},
		),
	)

	return promise.Future()
}

// recordingSelfRef captures what an actor sends itself, so a test can
// replay it as the actor's next turn.
type recordingSelfRef struct {
	msgs chan actormsg.VTXOActorMsg
}

// ID returns a fixed identifier.
func (r *recordingSelfRef) ID() string {
	return "recording-self"
}

// Tell records the message.
func (r *recordingSelfRef) Tell(_ context.Context,
	msg actormsg.VTXOActorMsg) error {

	r.msgs <- msg

	return nil
}

// TryTell delegates to Tell.
func (r *recordingSelfRef) TryTell(ctx context.Context,
	msg actormsg.VTXOActorMsg) error {

	return r.Tell(ctx, msg)
}

// TestStartClassifiesExpiryAtCurrentTip is the regression for a wallet
// reporting expired VTXOs as spendable. An actor spawned after its batch
// expired, as every VTXO a recovery scan finds on a long-idle wallet is,
// used to wait for the next block before evaluating its own expiry. Start
// now asks for the current tip and classifies on the answer.
func TestStartClassifiesExpiryAtCurrentTip(t *testing.T) {
	t.Parallel()

	h := newVTXOTestHarness(t)
	desc := h.newTestDescriptor()

	h.store.On(
		"UpdateVTXOStatus", h.ctx, desc.Outpoint,
		VTXOStatusExpired,
	).Return(nil).Once()
	self := &recordingSelfRef{
		msgs: make(chan actormsg.VTXOActorMsg, 1),
	}
	actorUnderTest := &VTXOActor{
		cfg: &VTXOActorConfig{
			VTXO:        desc,
			Store:       h.store,
			Wallet:      h.wallet,
			ChainParams: &chaincfg.RegressionNetParams,
			ChainSource: &tipChainSourceRef{
				height: desc.BatchExpiry,
			},
			Manager: newMockManagerRef(t),
		},
		state: &LiveState{
			VTXO: desc,
		},
		env: h.env,
	}

	require.NoError(t, actorUnderTest.Start(h.ctx, self))

	// The answer arrives as the actor's own message, not on Start's
	// stack: starting an actor must not block whoever spawned it.
	var msg actormsg.VTXOActorMsg
	select {
	case msg = <-self.msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("start did not deliver the current tip")
	}
	require.IsType(t, &startTipEvent{}, msg)
	require.IsType(t, &LiveState{}, actorUnderTest.state)

	_, err := actorUnderTest.Receive(h.ctx, msg).Unpack()
	require.NoError(t, err)
	require.IsType(t, &ExpiredState{}, actorUnderTest.state)

	h.store.AssertExpectations(t)
}

// TestStartTipFailureKeepsState pins that a chain source which cannot
// answer the start ask costs the actor nothing: its block subscription
// still drives expiry, so it is classified one block later instead.
func TestStartTipFailureKeepsState(t *testing.T) {
	t.Parallel()

	h := newVTXOTestHarness(t)
	desc := h.newTestDescriptor()

	actorUnderTest := &VTXOActor{
		cfg: &VTXOActorConfig{
			VTXO:        desc,
			Store:       h.store,
			Wallet:      h.wallet,
			ChainParams: &chaincfg.RegressionNetParams,
			Manager:     newMockManagerRef(t),
		},
		state: &LiveState{
			VTXO: desc,
		},
		env: h.env,
	}

	msg := &startTipEvent{
		tip: fn.Err[chainsource.ChainSourceResp](
			errors.New("chain backend not ready"),
		),
	}
	response, err := actorUnderTest.Receive(h.ctx, msg).Unpack()
	require.NoError(t, err)
	require.Equal(t, VTXOActorResponse{
		PriorState: actorUnderTest.state,
		NewState:   actorUnderTest.state,
	}, response)
	require.IsType(t, &LiveState{}, actorUnderTest.state)

	// No store expectations were set, so any persisted transition fails.
	h.store.AssertExpectations(t)
}
