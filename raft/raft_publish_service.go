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
			replyId := r.idGenerator.Next()
			if r.config.DisableProposalForwarding {
				leaderId, err := r.findLeader(ctx, checkLeaderTimeout)
				if err != nil {
					return err
				}
				if leaderId == r.config.LocalPeerId {
					res := r.proposalPubAppService(ctx, replyId, appServiceAddresses)
					return func() error {
						pErr := res.Wait()
						defer res.Release()
						if pErr != nil {
							return pErr
						}
						_ = res.Response()
						return pErr
					}()
				} else {
					return r.sendPubAppServiceMsgToLeader(ctx, leaderId, replyId, appServiceAddresses)
				}
			} else {
				res := r.proposalPubAppService(ctx, replyId, appServiceAddresses)
				err := res.Wait()
				res.Release()
				return err
			}
		}(); err != nil {
			if errors.Is(err, ErrStopped) || errors.Is(err, context.DeadlineExceeded) {
				pubDoneCh <- err
				break
			}
			r.logger.Warningf("Failed to publish application service addresses error: %v", err)
			// continue
		} else {
			r.status.MarkPublishServiceDone()
			pubDoneCh <- nil
			break
		}
	}
}

func (r *Raft) findLeader(ctx context.Context, checkLeaderTimeout time.Duration) (uint64, error) {
	ticker := time.NewTicker(checkLeaderTimeout + time.Duration(rand.Int63n(int64(checkLeaderTimeout/10))))
	defer ticker.Stop()
	leaderId := r.getLeaderId()
	for leaderId == None {
		select {
		case <-r.closer.CloseCh():
			return 0, ErrStopped
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
			leaderId = r.getLeaderId()
		}
	}
	return leaderId, nil
}

func (r *Raft) proposalPubAppService(ctx context.Context, replyId uint64, appServiceAddresses []string) ProposedResult {
	proposalData, err := encodePubAppServiceAddressesRequest(replyId, r.config.LocalPeerId, appServiceAddresses)
	if err != nil {
		return newErrorResult(err)
	}
	ch, err := r.propose(ctx, replyId, proposalData)
	if err != nil {
		return newErrorResult(err)
	}
	return newProposalResult(ctx, r.closer, ch)
}

func (r *Raft) sendPubAppServiceMsgToLeader(ctx context.Context, leaderId, replyId uint64,
	appServiceAddresses []string) error {

	c, err := r.trans.CreateTransportClient()
	if err != nil {
		return err
	}
	defer c.Close()
	resultCh, err := r.resultReplier.AcquireResultChan(replyId)
	if err != nil {
		return err
	}
	defer r.resultReplier.CancelResult(replyId)
	res := c.PublishApplicationService(babuzapb.PublishApplicationServiceRequest{
		ClusterId:           r.config.ClusterId,
		FromId:              r.config.LocalPeerId,
		ToId:                leaderId,
		ProposalReplyId:     replyId,
		AppServiceAddresses: appServiceAddresses,
	})
	if res.Status == babuzapb.SUCCESS {
		select {
		case <-r.closer.CloseCh():
			return ErrStopped
		case <-ctx.Done():
			return ctx.Err()
		case result := <-resultCh:
			if result.Error != nil {
				return result.Error
			}
			return nil
		}
	}
	return errors.New(res.Message)
}
