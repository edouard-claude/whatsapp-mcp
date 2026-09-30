package main

import (
	"errors"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/edouard-claude/whatsapp-mcp/bridge/pb"
)

// Délai laissé au téléphone pour renvoyer un média.
const mediaRetryTimeout = 60 * time.Second

func (b *bridge) messageSource(chatStr, senderStr string, fromMe bool) (types.MessageSource, error) {
	chat, err := types.ParseJID(chatStr)
	if err != nil {
		return types.MessageSource{}, err
	}
	src := types.MessageSource{Chat: chat, IsFromMe: fromMe, IsGroup: chat.Server == types.GroupServer}
	if senderStr != "" {
		if src.Sender, err = types.ParseJID(senderStr); err != nil {
			return types.MessageSource{}, err
		}
	}
	return src, nil
}

// mediaRetry envoie la demande au téléphone et attend sa réponse (événement
// MediaRetry portant le même identifiant de message).
func (b *bridge) mediaRetry(req *pb.MediaRetry) (*pb.Reply, error) {
	a, err := b.account(req.Account)
	if err != nil {
		return nil, err
	}
	src, err := b.messageSource(req.Chat, req.Sender, req.FromMe)
	if err != nil {
		return nil, err
	}
	ch := make(chan *events.MediaRetry, 1)
	b.retryMu.Lock()
	b.retries[req.Id] = ch
	b.retryMu.Unlock()
	defer func() {
		b.retryMu.Lock()
		delete(b.retries, req.Id)
		b.retryMu.Unlock()
	}()

	info := &types.MessageInfo{MessageSource: src, ID: req.Id}
	if err := a.client.SendMediaRetryReceipt(b.ctx, info, req.MediaKey); err != nil {
		return nil, fmt.Errorf("demande au téléphone : %w", err)
	}
	select {
	case evt := <-ch:
		notif, err := whatsmeow.DecryptMediaRetryNotification(evt, req.MediaKey)
		if errors.Is(err, whatsmeow.ErrMediaNotAvailableOnPhone) {
			return nil, errors.New("le téléphone n'a plus ce média")
		}
		if err != nil {
			return nil, err
		}
		if notif.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS || notif.GetDirectPath() == "" {
			return nil, fmt.Errorf("le téléphone n'a pas pu renvoyer le média (%s)", notif.GetResult())
		}
		return &pb.Reply{Payload: &pb.Reply_Text{Text: notif.GetDirectPath()}}, nil
	case <-time.After(mediaRetryTimeout):
		return nil, errors.New("pas de réponse du téléphone : il doit être allumé et connecté")
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	}
}

func (b *bridge) onMediaRetry(e *events.MediaRetry) {
	b.retryMu.Lock()
	ch, ok := b.retries[e.MessageID]
	b.retryMu.Unlock()
	if ok {
		select {
		case ch <- e:
		default:
		}
	}
}

func (b *bridge) requestHistory(req *pb.RequestHistory) error {
	a, err := b.account(req.Account)
	if err != nil {
		return err
	}
	src, err := b.messageSource(req.Chat, req.OldestSender, req.OldestFromMe)
	if err != nil {
		return err
	}
	info := &types.MessageInfo{MessageSource: src, ID: req.OldestId, Timestamp: time.UnixMilli(req.OldestTsMs)}
	count := int(req.Count)
	if count <= 0 || count > 500 {
		count = 50
	}
	_, err = a.client.SendPeerMessage(b.ctx, a.client.BuildHistorySyncRequest(info, count))
	return err
}
