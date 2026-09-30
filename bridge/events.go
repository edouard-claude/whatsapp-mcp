package main

import (
	"time"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/edouard-claude/whatsapp-mcp/bridge/pb"
)

// Taille visée d'un lot d'historique : bien en deçà de la trame maximale.
const (
	historyBatchMessages = 500
	historyBatchBytes    = 8 << 20
)

func (b *bridge) onEvent(a *account, evt any) bool {
	switch e := evt.(type) {
	case *events.Message:
		return b.forwardMessage(a, e)
	case *events.HistorySync:
		return b.forwardHistory(a, e)
	case *events.MediaRetry:
		b.onMediaRetry(e)
	case *events.Receipt:
		b.forwardReceipt(a, e)
	case *events.GroupInfo:
		b.groupChanged(a, e.JID)
	case *events.JoinedGroup:
		b.groupChanged(a, e.JID)
	case *events.Archive:
		b.settingsChanged(a, e.JID, func(c *pb.ChatSettingsChanged) { c.Archived = proto.Bool(e.Action.GetArchived()) })
	case *events.Pin:
		b.settingsChanged(a, e.JID, func(c *pb.ChatSettingsChanged) { c.Pinned = proto.Bool(e.Action.GetPinned()) })
	case *events.Mute:
		b.settingsChanged(a, e.JID, func(c *pb.ChatSettingsChanged) {
			end := int64(0)
			if e.Action.GetMuted() {
				end = e.Action.GetMuteEndTimestamp()
			}
			c.MuteEndMs = proto.Int64(end)
		})
	case *events.MarkChatAsRead:
		b.settingsChanged(a, e.JID, func(c *pb.ChatSettingsChanged) { c.Read = proto.Bool(e.Action.GetRead()) })
	case *events.PairSuccess:
		b.out.send(&pb.Event{Kind: &pb.Event_PairSuccess{PairSuccess: &pb.PairSuccess{Account: a.alias, Jid: jid(e.ID)}}})
	case *events.Connected:
		st := &pb.ConnectionState{Account: a.alias, State: pb.ConnectionState_CONNECTED}
		if id := a.client.Store.ID; id != nil {
			st.Jid = jid(*id)
		}
		st.Lid = jid(a.client.Store.LID)
		b.out.send(&pb.Event{Kind: &pb.Event_Connection{Connection: st}})
	case *events.Disconnected:
		b.state(a, pb.ConnectionState_DISCONNECTED, "")
	case *events.LoggedOut:
		a.stopped.Store(true)
		b.state(a, pb.ConnectionState_LOGGED_OUT, e.Reason.String())
	case *events.StreamReplaced:
		a.stopped.Store(true)
		b.state(a, pb.ConnectionState_STREAM_REPLACED, "session ouverte ailleurs")
	case *events.TemporaryBan:
		b.out.send(&pb.Event{Kind: &pb.Event_Connection{Connection: &pb.ConnectionState{
			Account: a.alias, State: pb.ConnectionState_TEMPORARY_BAN,
			Detail: e.Code.String(), UntilMs: time.Now().Add(e.Expire).UnixMilli(),
		}}})
	case *events.ConnectFailure:
		b.state(a, pb.ConnectionState_CONNECT_FAILED, e.Reason.String()+" "+e.Message)
	case *events.KeepAliveTimeout:
		b.state(a, pb.ConnectionState_KEEPALIVE_TIMEOUT, "")
	case *events.KeepAliveRestored:
		b.state(a, pb.ConnectionState_KEEPALIVE_RESTORED, "")
	case *events.ClientOutdated:
		b.state(a, pb.ConnectionState_CLIENT_OUTDATED, "mettre whatsmeow à jour")
	}
	return true
}

// messageInfo convertit l'enveloppe whatsmeow. Les formes alternatives
// (LID <-> numéro) manquantes sont complétées depuis la table de correspondance
// de whatsmeow.
func (b *bridge) messageInfo(a *account, info *types.MessageInfo, isEdit bool) *pb.MessageInfo {
	senderAlt := info.SenderAlt
	if senderAlt.IsEmpty() {
		senderAlt = b.altJID(a, info.Sender)
	}
	var chatAlt types.JID
	if !info.IsGroup {
		if info.IsFromMe {
			chatAlt = info.RecipientAlt
		} else if info.Sender.ToNonAD() == info.Chat.ToNonAD() {
			chatAlt = senderAlt
		}
		if chatAlt.IsEmpty() {
			chatAlt = b.altJID(a, info.Chat)
		}
	}
	return &pb.MessageInfo{
		Id:          info.ID,
		Chat:        jid(info.Chat),
		ChatAlt:     jid(chatAlt),
		Sender:      jid(info.Sender),
		SenderAlt:   jid(senderAlt),
		FromMe:      info.IsFromMe,
		IsGroup:     info.IsGroup,
		TimestampMs: info.Timestamp.UnixMilli(),
		PushName:    info.PushName,
		Type:        info.Type,
		MediaType:   info.MediaType,
		IsEdit:      isEdit,
	}
}

// altJID cherche l'autre forme d'un JID individuel. Vide si inconnue.
func (b *bridge) altJID(a *account, j types.JID) types.JID {
	var alt types.JID
	switch j.Server {
	case types.DefaultUserServer:
		alt, _ = a.client.Store.LIDs.GetLIDForPN(b.ctx, j.ToNonAD())
	case types.HiddenUserServer:
		alt, _ = a.client.Store.LIDs.GetPNForLID(b.ctx, j.ToNonAD())
	}
	return alt
}

// forwardMessage transmet le message et attend son Ack.
func (b *bridge) forwardMessage(a *account, e *events.Message) bool {
	raw, err := proto.Marshal(e.RawMessage)
	if err != nil {
		a.log.Error().Err(err).Str("id", e.Info.ID).Msg("sérialisation du message")
		return false
	}
	info := b.messageInfo(a, &e.Info, e.IsEdit)
	return b.emitAndWait(a, func(seq uint64) *pb.Event {
		return &pb.Event{Kind: &pb.Event_Message{Message: &pb.IncomingMessage{
			Account: a.alias, Seq: seq, Info: info, Raw: raw,
		}}}
	})
}

// forwardHistory convertit un morceau d'historique avec le parseur de whatsmeow
// (même format que le direct) et l'envoie en lots, chacun acquitté.
func (b *bridge) forwardHistory(a *account, e *events.HistorySync) bool {
	data := e.Data
	syncType := data.GetSyncType().String()
	newBatch := func() *pb.HistoryBatch {
		return &pb.HistoryBatch{Account: a.alias, SyncType: syncType, Progress: data.GetProgress()}
	}
	batch := newBatch()
	size := 0
	flush := func() bool {
		if len(batch.Chats)+len(batch.Messages)+len(batch.Reactions)+len(batch.Mappings) == 0 {
			return true
		}
		cur := batch
		ok := b.emitAndWait(a, func(seq uint64) *pb.Event {
			cur.Seq = seq
			return &pb.Event{Kind: &pb.Event_History{History: cur}}
		})
		batch = newBatch()
		size = 0
		return ok
	}

	for _, m := range data.GetPhoneNumberToLidMappings() {
		if m.GetLidJID() != "" && m.GetPnJID() != "" {
			batch.Mappings = append(batch.Mappings, &pb.LidMapping{Lid: m.GetLidJID(), Pn: m.GetPnJID()})
		}
	}

	var own types.JID
	if id := a.client.Store.ID; id != nil {
		own = id.ToNonAD()
	}
	for _, conv := range data.GetConversations() {
		chatJID, err := types.ParseJID(conv.GetID())
		if err != nil {
			continue
		}
		batch.Chats = append(batch.Chats, chatMeta(conv))
		if conv.GetPnJID() != "" && conv.GetLidJID() != "" {
			batch.Mappings = append(batch.Mappings, &pb.LidMapping{Lid: conv.GetLidJID(), Pn: conv.GetPnJID()})
		}
		for _, hm := range conv.GetMessages() {
			web := hm.GetMessage()
			if web == nil {
				continue
			}
			evt, err := a.client.ParseWebMessage(chatJID, web)
			if err != nil || evt.RawMessage == nil {
				continue
			}
			raw, err := proto.Marshal(evt.RawMessage)
			if err != nil {
				continue
			}
			batch.Messages = append(batch.Messages, &pb.HistoryMessage{Info: b.messageInfo(a, &evt.Info, evt.IsEdit), Raw: raw})
			size += len(raw)
			for _, r := range web.GetReactions() {
				batch.Reactions = append(batch.Reactions, historyReaction(chatJID, evt.Info.ID, own, r.GetKey().GetFromMe(),
					r.GetKey().GetParticipant(), r.GetText(), r.GetSenderTimestampMS()))
			}
			if len(batch.Messages) >= historyBatchMessages || size >= historyBatchBytes {
				if !flush() {
					return false
				}
			}
		}
	}
	return flush()
}

func chatMeta(conv *waHistorySync.Conversation) *pb.ChatMeta {
	name := conv.GetName()
	if name == "" {
		name = conv.GetDisplayName()
	}
	ts := conv.GetLastMsgTimestamp()
	if ts == 0 {
		ts = conv.GetConversationTimestamp()
	}
	return &pb.ChatMeta{
		Jid:           conv.GetID(),
		Name:          name,
		UnreadCount:   conv.GetUnreadCount(),
		Archived:      conv.GetArchived(),
		Pinned:        conv.GetPinned() > 0,
		MuteEndMs:     int64(conv.GetMuteEndTime()) * 1000,
		LastMessageMs: int64(ts) * 1000,
		Pn:            conv.GetPnJID(),
		Lid:           conv.GetLidJID(),
	}
}

func historyReaction(chat types.JID, target string, own types.JID, fromMe bool, participant, emoji string, tsMs int64) *pb.Reaction {
	sender := participant
	switch {
	case fromMe:
		sender = jid(own)
	case sender == "":
		// Discussion individuelle : l'auteur est l'interlocuteur.
		sender = jid(chat)
	default:
		if p, err := types.ParseJID(participant); err == nil {
			sender = jid(p)
		}
	}
	return &pb.Reaction{Chat: jid(chat), TargetId: target, Sender: sender, Emoji: emoji, TimestampMs: tsMs}
}

func (b *bridge) forwardReceipt(a *account, e *events.Receipt) {
	kind := string(e.Type)
	if e.Type == types.ReceiptTypeDelivered {
		kind = "delivered"
	}
	b.out.send(&pb.Event{Kind: &pb.Event_Receipt{Receipt: &pb.Receipt{
		Account:     a.alias,
		Chat:        jid(e.Chat),
		Sender:      jid(e.Sender),
		FromMe:      e.IsFromMe,
		MessageIds:  e.MessageIDs,
		Type:        kind,
		TimestampMs: e.Timestamp.UnixMilli(),
	}}})
}

func (b *bridge) groupChanged(a *account, j types.JID) {
	b.out.send(&pb.Event{Kind: &pb.Event_GroupChanged{GroupChanged: &pb.GroupChanged{Account: a.alias, Jid: jid(j)}}})
}

func (b *bridge) settingsChanged(a *account, chat types.JID, set func(*pb.ChatSettingsChanged)) {
	c := &pb.ChatSettingsChanged{Account: a.alias, Chat: jid(chat)}
	set(c)
	b.out.send(&pb.Event{Kind: &pb.Event_ChatSettings{ChatSettings: c}})
}
