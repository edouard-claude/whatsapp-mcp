package main

import (
	"time"

	"github.com/edouard-claude/whatsapp-mcp/bridge/pb"
)

// Maintien de la session.
//
// Un appareil lié reste valide tant qu'il se connecte régulièrement et que le
// téléphone principal lui-même se connecte (WhatsApp délie les appareils quand
// le téléphone reste hors ligne une quinzaine de jours : ça, on ne peut que le
// signaler). Côté bridge, trois étages :
//
//  1. whatsmeow maintient la websocket (keepalive) et se reconnecte seul après
//     une coupure (EnableAutoReconnect, InitialAutoReconnect) ;
//  2. keepConnected réessaie avec backoff quand une connexion échoue pour une
//     raison que whatsmeow ne juge pas « réessayable » ;
//  3. un chien de garde vérifie périodiquement la connexion et relance l'étage 2
//     si tout le reste a abandonné.
//
// Seules une session révoquée (LoggedOut) ou reprise par une autre instance
// (StreamReplaced) arrêtent les tentatives.

const (
	backoffMin     = 2 * time.Second
	backoffMax     = 5 * time.Minute
	watchdogPeriod = 2 * time.Minute
	// Déconnecté depuis plus longtemps que ça : le chien de garde intervient.
	watchdogGrace = 3 * time.Minute
)

// keepConnected connecte le compte et le surveille jusqu'à l'arrêt du bridge.
func (b *bridge) keepConnected(a *account) {
	b.connectWithBackoff(a)
	ticker := time.NewTicker(watchdogPeriod)
	defer ticker.Stop()
	var downSince time.Time
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
		}
		if a.stopped.Load() {
			return
		}
		if a.client.IsConnected() {
			downSince = time.Time{}
			continue
		}
		if downSince.IsZero() {
			downSince = time.Now()
			continue
		}
		if time.Since(downSince) >= watchdogGrace {
			a.log.Warn().Dur("down", time.Since(downSince)).Msg("chien de garde : reconnexion forcée")
			b.connectWithBackoff(a)
			downSince = time.Time{}
		}
	}
}

func (b *bridge) connectWithBackoff(a *account) {
	if !a.connecting.CompareAndSwap(false, true) {
		return
	}
	defer a.connecting.Store(false)
	delay := backoffMin
	for !a.stopped.Load() && b.ctx.Err() == nil {
		if a.client.IsConnected() {
			return
		}
		err := a.client.ConnectContext(b.ctx)
		if err == nil {
			return
		}
		a.log.Warn().Err(err).Dur("retry_in", delay).Msg("connexion impossible")
		b.state(a, pb.ConnectionState_CONNECT_FAILED, err.Error())
		select {
		case <-b.ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, backoffMax)
	}
}

func (b *bridge) state(a *account, s pb.ConnectionState_State, detail string) {
	b.out.send(&pb.Event{Kind: &pb.Event_Connection{Connection: &pb.ConnectionState{
		Account: a.alias, State: s, Detail: detail,
	}}})
}
