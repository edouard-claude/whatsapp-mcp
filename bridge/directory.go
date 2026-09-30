package main

import (
	"fmt"

	"go.mau.fi/whatsmeow/types"

	"github.com/edouard-claude/whatsapp-mcp/bridge/pb"
)

// getContacts rend le carnet de whatsmeow et toutes les correspondances
// LID <-> numéro qu'il connaît pour ces contacts.
func (b *bridge) getContacts(alias string) (*pb.Reply, error) {
	a, err := b.account(alias)
	if err != nil {
		return nil, err
	}
	all, err := a.client.Store.Contacts.GetAllContacts(b.ctx)
	if err != nil {
		return nil, err
	}
	out := &pb.Contacts{}
	var pns []types.JID
	for j, c := range all {
		out.Contacts = append(out.Contacts, &pb.Contact{
			Jid:           jid(j),
			FirstName:     c.FirstName,
			FullName:      c.FullName,
			PushName:      c.PushName,
			BusinessName:  c.BusinessName,
			RedactedPhone: c.RedactedPhone,
		})
		switch j.Server {
		case types.DefaultUserServer:
			pns = append(pns, j.ToNonAD())
		case types.HiddenUserServer:
			if pn, err := a.client.Store.LIDs.GetPNForLID(b.ctx, j.ToNonAD()); err == nil && !pn.IsEmpty() {
				out.Mappings = append(out.Mappings, &pb.LidMapping{Lid: jid(j), Pn: jid(pn)})
			}
		}
	}
	if len(pns) > 0 {
		lids, err := a.client.Store.LIDs.GetManyLIDsForPNs(b.ctx, pns)
		if err != nil {
			return nil, fmt.Errorf("correspondances LID : %w", err)
		}
		for pn, lid := range lids {
			if !lid.IsEmpty() {
				out.Mappings = append(out.Mappings, &pb.LidMapping{Lid: jid(lid), Pn: jid(pn)})
			}
		}
	}
	return &pb.Reply{Payload: &pb.Reply_Contacts{Contacts: out}}, nil
}

func (b *bridge) getGroups(req *pb.GetGroups) (*pb.Reply, error) {
	a, err := b.account(req.Account)
	if err != nil {
		return nil, err
	}
	var infos []*types.GroupInfo
	if len(req.Jids) == 0 {
		infos, err = a.client.GetJoinedGroups(b.ctx)
		if err != nil {
			return nil, err
		}
	} else {
		for _, s := range req.Jids {
			j, err := types.ParseJID(s)
			if err != nil {
				return nil, err
			}
			info, err := a.client.GetGroupInfo(b.ctx, j)
			if err != nil {
				return nil, fmt.Errorf("groupe %s : %w", s, err)
			}
			infos = append(infos, info)
		}
	}
	out := &pb.Groups{}
	for _, g := range infos {
		out.Groups = append(out.Groups, group(g))
	}
	return &pb.Reply{Payload: &pb.Reply_Groups{Groups: out}}, nil
}

func group(g *types.GroupInfo) *pb.Group {
	owner := g.OwnerPN
	if owner.IsEmpty() {
		owner = g.OwnerJID
	}
	out := &pb.Group{
		Jid:              jid(g.JID),
		Name:             g.Name,
		Topic:            g.Topic,
		Owner:            jid(owner),
		CreatedMs:        g.GroupCreated.UnixMilli(),
		Announce:         g.IsAnnounce,
		Locked:           g.IsLocked,
		IsCommunity:      g.IsParent,
		Parent:           jid(g.LinkedParentJID),
		EphemeralSeconds: g.DisappearingTimer,
	}
	for _, p := range g.Participants {
		role := "member"
		switch {
		case p.IsSuperAdmin:
			role = "superadmin"
		case p.IsAdmin:
			role = "admin"
		}
		out.Participants = append(out.Participants, &pb.Participant{
			Jid: jid(p.JID), Pn: jid(p.PhoneNumber), Lid: jid(p.LID), Role: role, DisplayName: p.DisplayName,
		})
	}
	return out
}
