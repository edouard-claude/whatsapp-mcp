// wa-bridge expose whatsmeow à wa-mcp sur stdin/stdout, et rien d'autre.
//
// Pas de SQL métier, pas de HTTP : le bridge relaie des commandes vers
// whatsmeow et des événements vers Rust. Les logs vont sur stderr, stdout ne
// porte que des trames (longueur big-endian sur 4 octets + protobuf).
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"github.com/edouard-claude/whatsapp-mcp/bridge/pb"
)

// Taille maximale d'une trame, dans les deux sens.
const maxFrame = 64 << 20

// Posée à la compilation : -ldflags "-X main.version=v0.1.0".
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "affiche la version et sort")
	dataDir := flag.String("data-dir", "", "racine des données (accounts/<alias>/session.db)")
	logLevel := flag.String("log-level", "info", "niveau de log sur stderr")
	flag.Parse()
	if *showVersion {
		fmt.Println("wa-bridge", version)
		return
	}

	level, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true}).
		Level(level).With().Timestamp().Str("component", "wa-bridge").Logger()

	if *dataDir == "" {
		log.Fatal().Msg("--data-dir est obligatoire")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	out := newWriter(os.Stdout)
	b := newBridge(ctx, *dataDir, out, log)

	// La fin de stdin signifie que wa-mcp est parti : on s'arrête avec lui.
	readErr := readCommands(os.Stdin, b.handle)
	switch {
	case readErr == nil, errors.Is(readErr, io.EOF):
		log.Info().Msg("stdin fermé, arrêt")
	default:
		log.Error().Err(readErr).Msg("lecture des commandes")
	}
	cancel()
	b.shutdown()
	out.close()
}

// readCommands lit les trames de stdin jusqu'à EOF et les passe à `handle`.
func readCommands(r io.Reader, handle func(*pb.Command)) error {
	br := bufio.NewReader(r)
	var header [4]byte
	for {
		if _, err := io.ReadFull(br, header[:]); err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(header[:])
		if n > maxFrame {
			return fmt.Errorf("trame de %d octets, maximum %d", n, maxFrame)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(br, buf); err != nil {
			return err
		}
		var cmd pb.Command
		if err := proto.Unmarshal(buf, &cmd); err != nil {
			return fmt.Errorf("commande illisible : %w", err)
		}
		handle(&cmd)
	}
}

// writer sérialise les événements sur stdout depuis une seule goroutine.
// `ch` n'est jamais fermé : un émetteur tardif (goroutine QR, handler en fin de
// déconnexion) ne peut donc pas paniquer, il est simplement ignoré après `close`.
type writer struct {
	ch   chan *pb.Event
	stop chan struct{}
	done chan struct{}
}

func newWriter(w io.Writer) *writer {
	wr := &writer{ch: make(chan *pb.Event, 256), stop: make(chan struct{}), done: make(chan struct{})}
	go wr.run(w)
	return wr
}

func (wr *writer) run(w io.Writer) {
	defer close(wr.done)
	bw := bufio.NewWriter(w)
	defer func() { _ = bw.Flush() }()
	var header [4]byte
	write := func(evt *pb.Event) error {
		buf, err := proto.Marshal(evt)
		if err != nil {
			fmt.Fprintf(os.Stderr, "événement non sérialisable : %v\n", err)
			return nil
		}
		binary.BigEndian.PutUint32(header[:], uint32(len(buf)))
		if _, err := bw.Write(header[:]); err != nil {
			return err
		}
		_, err = bw.Write(buf)
		return err
	}
	for {
		select {
		case evt := <-wr.ch:
			if write(evt) != nil {
				return
			}
			// Vidage dès que la file est vide : latence minimale sans écrire trame par trame.
			if len(wr.ch) == 0 && bw.Flush() != nil {
				return
			}
		case <-wr.stop:
			for {
				select {
				case evt := <-wr.ch:
					if write(evt) != nil {
						return
					}
				default:
					return
				}
			}
		}
	}
}

func (wr *writer) send(evt *pb.Event) {
	select {
	case wr.ch <- evt:
	case <-wr.stop:
	}
}

func (wr *writer) close() {
	close(wr.stop)
	<-wr.done
}
