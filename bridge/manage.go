package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/edouard-claude/whatsapp-mcp/bridge/pb"
)

func parseJIDs(in []string) ([]types.JID, error) {
	out := make([]types.JID, 0, len(in))
	for _, s := range in {
		j, err := parseRecipient(s)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

func participantResults(ps []types.GroupParticipant) *pb.Reply {
	out := &pb.ParticipantResults{}
	for _, p := range ps {
		j := p.PhoneNumber
		if j.IsEmpty() {
			j = p.JID
		}
		out.Results = append(out.Results, &pb.ParticipantResult{Jid: jid(j), Error: int32(p.Error)})
	}
	return &pb.Reply{Payload: &pb.Reply_Participants{Participants: out}}
}

// inviteCode accepte un lien complet ou le code seul.
func inviteCode(link string) string {
	link = strings.TrimSpace(link)
	if i := strings.LastIndex(link, "/"); i >= 0 {
		link = link[i+1:]
	}
	return link
}

func (b *bridge) group(req *pb.GroupCommand) (*pb.Reply, error) {
	a, err := b.account(req.Account)
	if err != nil {
		return nil, err
	}
	cli, ctx := a.client, b.ctx
	var g types.JID
	if req.Jid != "" {
		if g, err = types.ParseJID(req.Jid); err != nil {
			return nil, err
		}
	}
	ok := &pb.Reply{}
	switch op := req.Op.(type) {
	case *pb.GroupCommand_Create:
		ps, err := parseJIDs(op.Create.Participants)
		if err != nil {
			return nil, err
		}
		cr := whatsmeow.ReqCreateGroup{Name: op.Create.Name, Participants: ps}
		cr.IsParent = op.Create.Community
		if op.Create.Parent != "" {
			if cr.LinkedParentJID, err = types.ParseJID(op.Create.Parent); err != nil {
				return nil, err
			}
		}
		info, err := cli.CreateGroup(ctx, cr)
		if err != nil {
			return nil, err
		}
		return &pb.Reply{Payload: &pb.Reply_Groups{Groups: &pb.Groups{Groups: []*pb.Group{group(info)}}}}, nil
	case *pb.GroupCommand_SetName:
		return ok, cli.SetGroupName(ctx, g, op.SetName)
	case *pb.GroupCommand_SetTopic:
		// Le sujet précédent sert d'ancre contre les modifications concurrentes.
		info, err := cli.GetGroupInfo(ctx, g)
		if err != nil {
			return nil, err
		}
		return ok, cli.SetGroupTopic(ctx, g, info.TopicID, "", op.SetTopic)
	case *pb.GroupCommand_SetAnnounce:
		return ok, cli.SetGroupAnnounce(ctx, g, op.SetAnnounce)
	case *pb.GroupCommand_SetLocked:
		return ok, cli.SetGroupLocked(ctx, g, op.SetLocked)
	case *pb.GroupCommand_SetJoinApproval:
		return ok, cli.SetGroupJoinApprovalMode(ctx, g, op.SetJoinApproval)
	case *pb.GroupCommand_SetEphemeralSeconds:
		return ok, cli.SetDisappearingTimer(ctx, g, time.Duration(op.SetEphemeralSeconds)*time.Second, time.Now())
	case *pb.GroupCommand_SetPhotoPath:
		data, err := os.ReadFile(op.SetPhotoPath)
		if err != nil {
			return nil, err
		}
		_, err = cli.SetGroupPhoto(ctx, g, data)
		return ok, err
	case *pb.GroupCommand_Participants:
		ps, err := parseJIDs(op.Participants.Participants)
		if err != nil {
			return nil, err
		}
		var action whatsmeow.ParticipantChange
		switch op.Participants.Action {
		case "add":
			action = whatsmeow.ParticipantChangeAdd
		case "remove":
			action = whatsmeow.ParticipantChangeRemove
		case "promote":
			action = whatsmeow.ParticipantChangePromote
		case "demote":
			action = whatsmeow.ParticipantChangeDemote
		default:
			return nil, fmt.Errorf("action inconnue : %q", op.Participants.Action)
		}
		res, err := cli.UpdateGroupParticipants(ctx, g, ps, action)
		if err != nil {
			return nil, err
		}
		return participantResults(res), nil
	case *pb.GroupCommand_InviteLink:
		link, err := cli.GetGroupInviteLink(ctx, g, op.InviteLink)
		return &pb.Reply{Payload: &pb.Reply_Text{Text: link}}, err
	case *pb.GroupCommand_JoinLink:
		j, err := cli.JoinGroupWithLink(ctx, inviteCode(op.JoinLink))
		return &pb.Reply{Payload: &pb.Reply_Text{Text: jid(j)}}, err
	case *pb.GroupCommand_PreviewLink:
		info, err := cli.GetGroupInfoFromLink(ctx, inviteCode(op.PreviewLink))
		if err != nil {
			return nil, err
		}
		return &pb.Reply{Payload: &pb.Reply_Groups{Groups: &pb.Groups{Groups: []*pb.Group{group(info)}}}}, nil
	case *pb.GroupCommand_ListRequests:
		reqs, err := cli.GetGroupRequestParticipants(ctx, g)
		if err != nil {
			return nil, err
		}
		out := &pb.ParticipantResults{}
		for _, r := range reqs {
			out.Results = append(out.Results, &pb.ParticipantResult{Jid: jid(r.JID), RequestedAtMs: r.RequestedAt.UnixMilli()})
		}
		return &pb.Reply{Payload: &pb.Reply_Participants{Participants: out}}, nil
	case *pb.GroupCommand_Requests:
		ps, err := parseJIDs(op.Requests.Participants)
		if err != nil {
			return nil, err
		}
		action := whatsmeow.ParticipantChangeReject
		if op.Requests.Approve {
			action = whatsmeow.ParticipantChangeApprove
		}
		res, err := cli.UpdateGroupRequestParticipants(ctx, g, ps, action)
		if err != nil {
			return nil, err
		}
		return participantResults(res), nil
	case *pb.GroupCommand_Leave:
		return ok, cli.LeaveGroup(ctx, g)
	case *pb.GroupCommand_LinkChild:
		child, err := types.ParseJID(op.LinkChild)
		if err != nil {
			return nil, err
		}
		return ok, cli.LinkGroup(ctx, g, child)
	case *pb.GroupCommand_UnlinkChild:
		child, err := types.ParseJID(op.UnlinkChild)
		if err != nil {
			return nil, err
		}
		return ok, cli.UnlinkGroup(ctx, g, child)
	}
	return nil, errors.New("opération de groupe inconnue")
}

// sendAppState protège contre la panique de whatsmeow quand les clés d'app
// state ne sont pas encore synchronisées (juste après un appairage).
func sendAppState(ctx context.Context, cli *whatsmeow.Client, patch appstate.PatchInfo) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("synchronisation des réglages pas encore prête, réessayer dans une minute (%v)", r)
		}
	}()
	return cli.SendAppState(ctx, patch)
}

func (b *bridge) chatSettings(req *pb.ChatSettings) error {
	a, err := b.account(req.Account)
	if err != nil {
		return err
	}
	chat, err := types.ParseJID(req.Chat)
	if err != nil {
		return err
	}
	var key *waCommon.MessageKey
	if req.LastId != "" {
		key = &waCommon.MessageKey{
			RemoteJID: proto.String(chat.String()),
			FromMe:    proto.Bool(req.LastFromMe),
			ID:        proto.String(req.LastId),
		}
		if !req.LastFromMe && chat.Server == types.GroupServer && req.LastSender != "" {
			key.Participant = proto.String(req.LastSender)
		}
	}
	lastTS := time.UnixMilli(req.LastTsMs)
	var patch appstate.PatchInfo
	switch op := req.Op.(type) {
	case *pb.ChatSettings_Archive:
		patch = appstate.BuildArchive(chat, op.Archive, lastTS, key)
	case *pb.ChatSettings_Pin:
		patch = appstate.BuildPin(chat, op.Pin)
	case *pb.ChatSettings_MuteUntilMs:
		switch {
		case op.MuteUntilMs == 0:
			patch = appstate.BuildMuteAbs(chat, false, nil)
		case op.MuteUntilMs < 0:
			patch = appstate.BuildMuteAbs(chat, true, nil)
		default:
			patch = appstate.BuildMuteAbs(chat, true, proto.Int64(op.MuteUntilMs))
		}
	case *pb.ChatSettings_MarkRead:
		patch = appstate.BuildMarkChatAsRead(chat, op.MarkRead, lastTS, key)
	default:
		return errors.New("réglage inconnu")
	}
	return sendAppState(b.ctx, a.client, patch)
}

func (b *bridge) accountCommand(req *pb.AccountCommand) (*pb.Reply, error) {
	a, err := b.account(req.Account)
	if err != nil {
		return nil, err
	}
	cli, ctx := a.client, b.ctx
	one := func(s string) (types.JID, error) { return parseRecipient(s) }
	blocklist := func(bl *types.Blocklist, err error) (*pb.Reply, error) {
		if err != nil {
			return nil, err
		}
		out := &pb.StringList{}
		for _, j := range bl.JIDs {
			out.Values = append(out.Values, jid(j))
		}
		return &pb.Reply{Payload: &pb.Reply_Strings{Strings: out}}, nil
	}
	privacy := func(p types.PrivacySettings) *pb.Reply {
		return &pb.Reply{Payload: &pb.Reply_KeyValues{KeyValues: &pb.KeyValues{Values: map[string]string{
			"groupadd": string(p.GroupAdd), "last": string(p.LastSeen), "status": string(p.Status),
			"profile": string(p.Profile), "readreceipts": string(p.ReadReceipts), "calladd": string(p.CallAdd),
			"online": string(p.Online), "messages": string(p.Messages), "defense": string(p.Defense),
			"stickers": string(p.Stickers),
		}}}}
	}
	switch op := req.Op.(type) {
	case *pb.AccountCommand_UserInfo:
		jids, err := parseJIDs(op.UserInfo.Values)
		if err != nil {
			return nil, err
		}
		infos, err := cli.GetUserInfo(ctx, jids)
		if err != nil {
			return nil, err
		}
		out := &pb.Users{}
		for j, u := range infos {
			user := &pb.User{Jid: jid(j), About: u.Status, PictureId: u.PictureID, Devices: uint32(len(u.Devices)), Lid: jid(u.LID)}
			if u.VerifiedName != nil && u.VerifiedName.Details != nil {
				user.VerifiedName = u.VerifiedName.Details.GetVerifiedName()
			}
			out.Users = append(out.Users, user)
		}
		return &pb.Reply{Payload: &pb.Reply_Users{Users: out}}, nil
	case *pb.AccountCommand_Picture:
		j, err := one(op.Picture)
		if err != nil {
			return nil, err
		}
		pic, err := cli.GetProfilePictureInfo(ctx, j, &whatsmeow.GetProfilePictureParams{})
		if errors.Is(err, whatsmeow.ErrProfilePictureNotSet) || errors.Is(err, whatsmeow.ErrProfilePictureUnauthorized) {
			return &pb.Reply{Payload: &pb.Reply_Text{Text: ""}}, nil
		}
		if err != nil {
			return nil, err
		}
		url := ""
		if pic != nil {
			url = pic.URL
		}
		return &pb.Reply{Payload: &pb.Reply_Text{Text: url}}, nil
	case *pb.AccountCommand_Business:
		j, err := one(op.Business)
		if err != nil {
			return nil, err
		}
		bp, err := cli.GetBusinessProfile(ctx, j)
		if err != nil {
			return nil, err
		}
		vals := map[string]string{"address": bp.Address, "email": bp.Email}
		var cats []string
		for _, c := range bp.Categories {
			cats = append(cats, c.Name)
		}
		vals["categories"] = strings.Join(cats, ", ")
		for k, v := range bp.ProfileOptions {
			vals[k] = v
		}
		return &pb.Reply{Payload: &pb.Reply_KeyValues{KeyValues: &pb.KeyValues{Values: vals}}}, nil
	case *pb.AccountCommand_SetAbout:
		return &pb.Reply{}, cli.SetStatusMessage(ctx, types.SetStatusInput{Text: proto.String(op.SetAbout)})
	case *pb.AccountCommand_GetBlocklist:
		return blocklist(cli.GetBlocklist(ctx))
	case *pb.AccountCommand_Block:
		j, err := one(op.Block)
		if err != nil {
			return nil, err
		}
		return blocklist(cli.UpdateBlocklist(ctx, j, events.BlocklistChangeActionBlock))
	case *pb.AccountCommand_Unblock:
		j, err := one(op.Unblock)
		if err != nil {
			return nil, err
		}
		return blocklist(cli.UpdateBlocklist(ctx, j, events.BlocklistChangeActionUnblock))
	case *pb.AccountCommand_GetPrivacy:
		p, err := cli.TryFetchPrivacySettings(ctx, true)
		if err != nil {
			return nil, err
		}
		return privacy(*p), nil
	case *pb.AccountCommand_SetPrivacy:
		p, err := cli.SetPrivacySetting(ctx, types.PrivacySettingType(op.SetPrivacy.Key), types.PrivacySetting(op.SetPrivacy.Value))
		if err != nil {
			return nil, err
		}
		return privacy(p), nil
	}
	return nil, errors.New("opération de compte inconnue")
}
