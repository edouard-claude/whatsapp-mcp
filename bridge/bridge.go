package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"

	"github.com/edouard-claude/whatsapp-mcp/bridge/pb"
)

// Délai d'attente d'un Ack. Au-delà, les données ne sont pas acquittées auprès
// de WhatsApp, qui les redélivrera.
const ackTimeout = 60 * time.Second

var aliasRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)

type bridge struct {
	ctx     context.Context
	dataDir string
	out     *writer
	log     zerolog.Logger

	mu       sync.Mutex
	accounts map[string]*account

	seq     atomic.Uint64
	ackMu   sync.Mutex
	pending map[uint64]chan bool

	// Demandes de renvoi de média en attente, par identifiant de message.
	retryMu sync.Mutex
	retries map[string]chan *events.MediaRetry
}

type account struct {
	alias  string
	client *whatsmeow.Client
	db     *sql.DB
	log    zerolog.Logger

	// Session révoquée ou reprise par une autre instance : plus de reconnexion.
	stopped    atomic.Bool
	connecting atomic.Bool
}

func newBridge(ctx context.Context, dataDir string, out *writer, log zerolog.Logger) *bridge {
	return &bridge{
		ctx:      ctx,
		dataDir:  dataDir,
		out:      out,
		log:      log,
		accounts: make(map[string]*account),
		pending:  make(map[uint64]chan bool),
		retries:  make(map[string]chan *events.MediaRetry),
	}
}

// handle est appelé par la boucle de lecture de stdin. Les commandes lentes
// partent dans une goroutine : un Ack ne doit jamais attendre derrière un envoi,
// sinon le handler whatsmeow qui l'attend bloquerait la réception.
func (b *bridge) handle(cmd *pb.Command) {
	switch k := cmd.Kind.(type) {
	case *pb.Command_Ack:
		b.resolveAck(k.Ack.Seq, k.Ack.Ok)
	case *pb.Command_StartAccount:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.startAccount(k.StartAccount) })
	case *pb.Command_SendMessage:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.sendMessage(k.SendMessage) })
	case *pb.Command_UploadMedia:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.uploadMedia(k.UploadMedia) })
	case *pb.Command_MarkRead:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return &pb.Reply{}, b.markRead(k.MarkRead) })
	case *pb.Command_ChatPresence:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return &pb.Reply{}, b.chatPresence(k.ChatPresence) })
	case *pb.Command_Group:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.group(k.Group) })
	case *pb.Command_ChatSettings:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return &pb.Reply{}, b.chatSettings(k.ChatSettings) })
	case *pb.Command_AccountCommand:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.accountCommand(k.AccountCommand) })
	case *pb.Command_MediaRetry:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.mediaRetry(k.MediaRetry) })
	case *pb.Command_RequestHistory:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return &pb.Reply{}, b.requestHistory(k.RequestHistory) })
	case *pb.Command_CheckPhones:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.checkPhones(k.CheckPhones) })
	case *pb.Command_Logout:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return &pb.Reply{}, b.logout(k.Logout.Account) })
	case *pb.Command_GetContacts:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.getContacts(k.GetContacts.Account) })
	case *pb.Command_GetGroups:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.getGroups(k.GetGroups) })
	case *pb.Command_DownloadMedia:
		go b.reply(cmd.Id, func() (*pb.Reply, error) { return b.downloadMedia(k.DownloadMedia) })
	default:
		b.reply(cmd.Id, func() (*pb.Reply, error) { return nil, errors.New("commande inconnue") })
	}
}

func (b *bridge) reply(id uint64, f func() (*pb.Reply, error)) {
	r, err := f()
	if r == nil {
		r = &pb.Reply{}
	}
	r.Id = id
	if err != nil {
		r.Error = err.Error()
	}
	b.out.send(&pb.Event{Kind: &pb.Event_Reply{Reply: r}})
}

func (b *bridge) account(alias string) (*account, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.accounts[alias]
	if a == nil {
		return nil, fmt.Errorf("compte %q non démarré", alias)
	}
	return a, nil
}

func (b *bridge) startAccount(req *pb.StartAccount) (*pb.Reply, error) {
	alias := req.Account
	if !aliasRe.MatchString(alias) {
		return nil, fmt.Errorf("alias de compte invalide : %q", alias)
	}
	b.mu.Lock()
	if _, ok := b.accounts[alias]; ok {
		b.mu.Unlock()
		return nil, fmt.Errorf("compte %q déjà démarré", alias)
	}
	// Réservation immédiate : deux StartAccount concurrents ne créent pas deux clients.
	b.accounts[alias] = nil
	b.mu.Unlock()

	a, err := b.openAccount(alias)
	if err != nil {
		b.mu.Lock()
		delete(b.accounts, alias)
		b.mu.Unlock()
		return nil, err
	}
	b.mu.Lock()
	b.accounts[alias] = a
	b.mu.Unlock()

	if a.client.Store.ID == nil {
		code, err := b.pair(a, req.PairPhone)
		if err != nil {
			return nil, err
		}
		reply := &pb.Reply{}
		if code != "" {
			reply.Payload = &pb.Reply_PairCode{PairCode: &pb.PairCode{Account: alias, Code: code}}
		}
		return reply, nil
	}
	go b.keepConnected(a)
	return &pb.Reply{}, nil
}

func (b *bridge) openAccount(alias string) (*account, error) {
	dir := filepath.Join(b.dataDir, "accounts", alias)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	dsn := "file:" + filepath.Join(dir, "session.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	logger := b.log.With().Str("account", alias).Logger()
	container := sqlstore.NewWithDB(db, "sqlite3", waLog.Zerolog(logger.With().Str("module", "store").Logger()))
	if err := container.Upgrade(b.ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("migration de la session : %w", err)
	}
	device, err := container.GetFirstDevice(b.ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	client := whatsmeow.NewClient(device, waLog.Zerolog(logger.With().Str("module", "client").Logger()))
	// Les données ne sont acquittées auprès de WhatsApp qu'une fois tous les
	// handlers revenus avec succès, c'est-à-dire après l'Ack de wa-mcp.
	client.SynchronousAck = true
	// Le clair est gardé jusqu'à l'ack : une redélivrance ne bute pas sur un
	// déchiffrement déjà consommé.
	client.EnableDecryptedEventBuffer = true
	// Reconnexion automatique, y compris quand la toute première connexion échoue.
	client.EnableAutoReconnect = true
	client.InitialAutoReconnect = true
	a := &account{alias: alias, client: client, db: db, log: logger}
	client.AddEventHandlerWithSuccessStatus(func(evt any) bool { return b.onEvent(a, evt) })
	return a, nil
}

// pair connecte un compte non appairé. Sans numéro : QR émis en événements, rend
// "". Avec numéro : attend le code d'appairage (il faut d'abord le premier QR,
// signe que la connexion est établie) et le rend.
func (b *bridge) pair(a *account, phone string) (string, error) {
	qrChan, err := a.client.GetQRChannel(b.ctx)
	if err != nil {
		return "", err
	}
	if err := a.client.ConnectContext(b.ctx); err != nil {
		return "", err
	}
	type result struct {
		code string
		err  error
	}
	first := make(chan result, 1)
	go func() {
		sent := false
		for item := range qrChan {
			switch item.Event {
			case whatsmeow.QRChannelEventCode:
				if phone == "" {
					b.out.send(&pb.Event{Kind: &pb.Event_Qr{Qr: &pb.Qr{Account: a.alias, Code: item.Code}}})
					continue
				}
				if sent {
					continue
				}
				sent = true
				code, err := a.client.PairPhone(b.ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (macOS)")
				if err == nil {
					b.out.send(&pb.Event{Kind: &pb.Event_PairCode{PairCode: &pb.PairCode{Account: a.alias, Code: code}}})
				} else {
					a.log.Error().Err(err).Msg("PairPhone")
				}
				first <- result{code, err}
			case "success":
				// Appairé : la session est désormais surveillée comme les autres.
				go b.keepConnected(a)
			default:
				a.log.Warn().Str("event", item.Event).Err(item.Error).Msg("canal QR")
				if !sent && phone != "" {
					sent = true
					first <- result{"", fmt.Errorf("appairage : %s %v", item.Event, item.Error)}
				}
			}
		}
	}()
	if phone == "" {
		return "", nil
	}
	select {
	case r := <-first:
		return r.code, r.err
	case <-time.After(60 * time.Second):
		return "", errors.New("pas de code d'appairage au bout de 60 s")
	case <-b.ctx.Done():
		return "", b.ctx.Err()
	}
}

// resolveAck débloque le handler qui attend l'Ack `seq`.
func (b *bridge) resolveAck(seq uint64, ok bool) {
	b.ackMu.Lock()
	ch, found := b.pending[seq]
	b.ackMu.Unlock()
	if found {
		ch <- ok
	}
}

// emitAndWait envoie un événement porteur d'un numéro de séquence et attend son
// Ack. Faux sur refus, délai dépassé ou arrêt : WhatsApp redélivrera.
func (b *bridge) emitAndWait(a *account, build func(seq uint64) *pb.Event) bool {
	seq := b.seq.Add(1)
	ch := make(chan bool, 1)
	b.ackMu.Lock()
	b.pending[seq] = ch
	b.ackMu.Unlock()
	defer func() {
		b.ackMu.Lock()
		delete(b.pending, seq)
		b.ackMu.Unlock()
	}()
	b.out.send(build(seq))
	select {
	case ok := <-ch:
		return ok
	case <-time.After(ackTimeout):
		a.log.Warn().Uint64("seq", seq).Msg("pas d'ack, données laissées à redélivrer")
		return false
	case <-b.ctx.Done():
		return false
	}
}

func (b *bridge) logout(alias string) error {
	a, err := b.account(alias)
	if err != nil {
		return err
	}
	a.stopped.Store(true)
	return a.client.Logout(b.ctx)
}

// shutdown déconnecte tous les comptes. Les Acks en attente tombent sur le
// contexte annulé et renvoient false : rien n'est perdu, tout sera redélivré.
func (b *bridge) shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, a := range b.accounts {
		if a == nil {
			continue
		}
		a.stopped.Store(true)
		a.client.Disconnect()
		a.db.Close()
	}
}

func parseRecipient(to string) (types.JID, error) {
	if strings.Contains(to, "@") {
		return types.ParseJID(to)
	}
	digits := strings.TrimPrefix(to, "+")
	if digits == "" || strings.Trim(digits, "0123456789") != "" {
		return types.JID{}, fmt.Errorf("destinataire invalide : %q", to)
	}
	return types.NewJID(digits, types.DefaultUserServer), nil
}

// jid rend un JID sans partie appareil, vide si le JID l'est.
func jid(j types.JID) string {
	if j.IsEmpty() {
		return ""
	}
	return j.ToNonAD().String()
}
