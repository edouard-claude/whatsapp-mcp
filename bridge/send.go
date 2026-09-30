package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/edouard-claude/whatsapp-mcp/bridge/pb"
)

// Taille maximale d'un fichier téléversé : limite des documents WhatsApp.
const maxUpload = 2 << 30

func (b *bridge) sendMessage(req *pb.SendMessage) (*pb.Reply, error) {
	a, err := b.account(req.Account)
	if err != nil {
		return nil, err
	}
	to, err := parseRecipient(req.To)
	if err != nil {
		return nil, err
	}
	var msg waE2E.Message
	if err := proto.Unmarshal(req.Message, &msg); err != nil {
		return nil, fmt.Errorf("message illisible : %w", err)
	}
	var extra whatsmeow.SendRequestExtra
	if req.Id != "" {
		extra.ID = req.Id
	}
	resp, err := a.client.SendMessage(b.ctx, to, &msg, extra)
	if err != nil {
		return nil, err
	}
	return &pb.Reply{Payload: &pb.Reply_Send{Send: &pb.SendResult{
		MessageId:   resp.ID,
		TimestampMs: resp.Timestamp.UnixMilli(),
	}}}, nil
}

func (b *bridge) uploadMedia(req *pb.UploadMedia) (*pb.Reply, error) {
	a, err := b.account(req.Account)
	if err != nil {
		return nil, err
	}
	var kind whatsmeow.MediaType
	switch req.MediaType {
	case "image":
		kind = whatsmeow.MediaImage
	case "video":
		kind = whatsmeow.MediaVideo
	case "audio":
		kind = whatsmeow.MediaAudio
	case "document":
		kind = whatsmeow.MediaDocument
	default:
		return nil, fmt.Errorf("type de média inconnu : %q", req.MediaType)
	}
	info, err := os.Stat(req.Path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxUpload {
		return nil, fmt.Errorf("%s : fichier absent, spécial ou trop gros", req.Path)
	}
	data, err := os.ReadFile(req.Path)
	if err != nil {
		return nil, err
	}
	up, err := a.client.Upload(b.ctx, data, kind)
	if err != nil {
		return nil, err
	}
	return &pb.Reply{Payload: &pb.Reply_Upload{Upload: &pb.UploadResult{
		Url:           up.URL,
		DirectPath:    up.DirectPath,
		MediaKey:      up.MediaKey,
		FileEncSha256: up.FileEncSHA256,
		FileSha256:    up.FileSHA256,
		FileLength:    up.FileLength,
	}}}, nil
}

func (b *bridge) markRead(req *pb.MarkRead) error {
	a, err := b.account(req.Account)
	if err != nil {
		return err
	}
	chat, err := types.ParseJID(req.Chat)
	if err != nil {
		return err
	}
	var sender types.JID
	if req.Sender != "" {
		if sender, err = types.ParseJID(req.Sender); err != nil {
			return err
		}
	}
	return a.client.MarkRead(b.ctx, req.Ids, time.Now(), chat, sender)
}

func (b *bridge) chatPresence(req *pb.ChatPresence) error {
	a, err := b.account(req.Account)
	if err != nil {
		return err
	}
	chat, err := parseRecipient(req.Chat)
	if err != nil {
		return err
	}
	state, media := types.ChatPresenceComposing, types.ChatPresenceMediaText
	switch req.State {
	case "composing":
	case "recording":
		media = types.ChatPresenceMediaAudio
	case "paused":
		state = types.ChatPresencePaused
	default:
		return fmt.Errorf("état inconnu : %q (composing, recording, paused)", req.State)
	}
	return a.client.SendChatPresence(b.ctx, chat, state, media)
}

func (b *bridge) checkPhones(req *pb.CheckPhones) (*pb.Reply, error) {
	a, err := b.account(req.Account)
	if err != nil {
		return nil, err
	}
	queries := make([]string, 0, len(req.Phones))
	for _, p := range req.Phones {
		digits := strings.TrimPrefix(strings.TrimSpace(p), "+")
		queries = append(queries, "+"+digits)
	}
	resp, err := a.client.IsOnWhatsApp(b.ctx, queries)
	if err != nil {
		return nil, err
	}
	out := &pb.PhoneChecks{}
	for _, r := range resp {
		out.Results = append(out.Results, &pb.PhoneCheck{
			Phone: strings.TrimPrefix(r.Query, "+"), OnWhatsapp: r.IsIn, Jid: jid(r.JID),
		})
	}
	return &pb.Reply{Payload: &pb.Reply_Phones{Phones: out}}, nil
}
