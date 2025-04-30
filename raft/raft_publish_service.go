package raft

import (
	"context"
	"errors"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"math/rand"
	"time"
)

func (r *Raft) applicationServiceStart(ctx context.Context,
	checkLeaderTimeout time.Duration, appServiceAddresses []string, pubDoneCh chan error) {

	for {
		select {
		case <-r.closer.CloseCh():
			pubDoneCh <- ErrStopped
			return
		case <-ctx.Done():
			pubDoneCh <- ctx.Err()
			return
		default:
		}
		if err := func() error {
			replyID := r.idGenerator.Next()
			if r.config.DisableProposalForwarding {
				leaderID, err := r.findLeader(ctx, checkLeaderTimeout)
				if err != nil {
					return err
				}
				if leaderID == r.config.LocalPeerID {
					res := r.proposalPubAppService(ctx, replyID, appServiceAddresses)
					return func() error {
						ar := res.WaitForApplyResult()
						defer res.Release()
						return ar.Error
					}()
				} else {
					return r.sendPubAppServiceMsgToLeader(ctx, leaderID, replyID, appServiceAddresses)
				}
			} else {
				res := r.proposalPubAppService(ctx, replyID, appServiceAddresses)
				ar := res.WaitForApplyResult()
				res.Release()
				return ar.Error
			}
		}(); err != nil {
			if errors.Is(err, ErrStopped) || errors.Is(err, context.DeadlineExceeded) {
				pubDoneCh <- err
				break
			}
			r.logger.Warningf("Failed to publish application service addresses error: %v", err)
			time.Sleep(time.Millisecond * 200)
		} else {
			pubDoneCh <- nil
			break
		}
	}
}

func (r *Raft) findLeader(ctx context.Context, checkLeaderTimeout time.Duration) (uint64, error) {
	ticker := time.NewTicker(checkLeaderTimeout + time.Duration(rand.Int63n(int64(checkLeaderTimeout/10))))
	defer ticker.Stop()
	leaderID := r.getLeaderId()
	for leaderID == None {
		select {
		case <-r.closer.CloseCh():
			return 0, ErrStopped
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
			leaderID = r.getLeaderId()
		}
	}
	return leaderID, nil
}

func (r *Raft) proposalPubAppService(ctx context.Context, replyID uint64, appServiceAddresses []string) ProposedResult {
	proposalData, err := EncodePubAppServiceAddressesRequest(replyID, r.config.LocalPeerID, appServiceAddresses)
	if err != nil {
		return NewErrorResult(err)
	}
	ch, err := r.propose(ctx, replyID, proposalData)
	if err != nil {
		return NewErrorResult(err)
	}
	return NewProposalResult(ctx, r.closer, ch)
}

func (r *Raft) sendPubAppServiceMsgToLeader(ctx context.Context, leaderID, replyID uint64,
	appServiceAddresses []string) error {

	c, err := r.trans.CreateTransportClient()
	if err != nil {
		return err
	}
	defer c.Close()
	resultCh, err := r.resultReplier.AcquireResultChan(replyID)
	if err != nil {
		return err
	}
	defer r.resultReplier.CancelResult(replyID)
	res := c.PublishApplicationService(babuzapb.PublishApplicationServiceRequest{
		ClusterID:           r.config.ClusterID,
		From:                r.config.LocalPeerID,
		To:                  leaderID,
		ProposalReplyID:     replyID,
		AppServiceAddresses: appServiceAddresses,
	})
	if res.Status == babuzapb.SUCCESS {
		select {
		case <-r.closer.CloseCh():
			return ErrStopped
		case <-ctx.Done():
			return ctx.Err()
		case result := <-resultCh:
			return result.Error
		}
	}
	return errors.New(res.Message)
}
