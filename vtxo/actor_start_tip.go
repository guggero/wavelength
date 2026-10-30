package vtxo

import (
	"context"
	"log/slog"
	"time"

	"github.com/lightninglabs/wavelength/baselib/actor"
	"github.com/lightninglabs/wavelength/chainsource"
	"github.com/lightninglabs/wavelength/lib/actormsg"
	fn "github.com/lightningnetwork/lnd/fn/v2"
)

// startTipAskTimeout bounds the best-height ask a VTXO actor sends when it
// starts. A chain source that does not answer in time only costs the actor
// its early classification; the block subscription still drives expiry.
const startTipAskTimeout = 30 * time.Second

// startTipEvent carries the chain source's answer to the best-height ask a
// VTXO actor sends itself on start. It never reaches the FSM: Receive turns
// a successful answer into an ordinary BlockEpochEvent and drops a failure.
type startTipEvent struct {
	actor.BaseMessage

	tip fn.Result[chainsource.ChainSourceResp]
}

// MessageType returns the message type identifier for logging and debugging.
func (*startTipEvent) MessageType() string {
	return "StartTipEvent"
}

// VTXOActorMsg marks startTipEvent as a VTXO actor message.
func (*startTipEvent) VTXOActorMsg() {}

// requestStartTip asks the chain source for the tip as it stands right now
// and has the answer delivered to the actor as its own message.
//
// Every expiry decision an actor makes is driven by a BlockEpochEvent, and
// a subscription that starts after the current block emits nothing until
// the next one. Until then the actor believes an expired VTXO is still live
// and the wallet counts its value as spendable balance. That is not an edge
// case: a recovery scan on a restored seed materializes VTXOs minutes after
// startup, and on a wallet whose batches expired while it was away every one
// of them is misreported until the next block, ten minutes on mainnet.
//
// The ask completes on a later turn of this actor, never on the spawning
// manager's turn, so starting an actor still cannot block the manager.
func (a *VTXOActor) requestStartTip(ctx context.Context) {
	var req chainsource.ChainSourceMsg = &chainsource.BestHeightRequest{}
	actor.AskThen(
		ctx, a.cfg.ChainSource, req,
		a.selfRef, startTipAskTimeout,
		func(
			r fn.Result[chainsource.ChainSourceResp],
		) actormsg.VTXOActorMsg {

			return &startTipEvent{tip: r}
		},
	)
}

// startTipEpoch maps the answer to the start ask onto the epoch the actor
// would otherwise have waited a block for. A failure is logged, not
// returned: the actor is already running and its block subscription still
// drives expiry, so the only cost is classifying it one block later.
func (a *VTXOActor) startTipEpoch(ctx context.Context, msg *startTipEvent) (
	*BlockEpochEvent, bool) {

	response, err := msg.tip.Unpack()
	if err != nil {
		a.logger(ctx).WarnS(ctx, "Could not apply current tip on start",
			err,
			slog.String("outpoint", a.cfg.VTXO.Outpoint.String()),
		)

		return nil, false
	}

	tip, ok := response.(*chainsource.BestHeightResponse)
	if !ok {
		a.logger(ctx).WarnS(ctx, "Unexpected best height response",
			nil,
			slog.String("outpoint", a.cfg.VTXO.Outpoint.String()),
			slog.Any("response", response),
		)

		return nil, false
	}

	return &BlockEpochEvent{
		Height: tip.Height,
		Hash:   tip.Hash,
	}, true
}
