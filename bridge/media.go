package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"

	"github.com/edouard-claude/whatsapp-mcp/bridge/pb"
)

// downloadMedia déchiffre le média d'un message et l'écrit à l'endroit demandé.
// Le chemin doit rester sous le répertoire de données : wa-mcp le choisit, le
// bridge le vérifie quand même.
func (b *bridge) downloadMedia(req *pb.DownloadMedia) (*pb.Reply, error) {
	a, err := b.account(req.Account)
	if err != nil {
		return nil, err
	}
	path, err := b.insideDataDir(req.Path)
	if err != nil {
		return nil, err
	}
	var msg waE2E.Message
	if err := proto.Unmarshal(req.Message, &msg); err != nil {
		return nil, fmt.Errorf("message illisible : %w", err)
	}
	data, err := a.client.DownloadAny(b.ctx, &msg)
	switch {
	case errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403),
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404),
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410):
		// Les serveurs ne gardent les médias que quelques semaines ; le téléphone
		// peut les renvoyer (media retry, P5).
		return nil, fmt.Errorf("média expiré sur les serveurs WhatsApp (%w)", err)
	case err != nil:
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	// Écriture atomique : un fichier présent est toujours complet.
	tmp := path + ".part"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	return &pb.Reply{Payload: &pb.Reply_Media{Media: &pb.MediaResult{Path: path, Size: uint64(len(data))}}}, nil
}

func (b *bridge) insideDataDir(p string) (string, error) {
	root, err := filepath.Abs(b.dataDir)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("chemin hors du répertoire de données : %q", p)
	}
	return abs, nil
}
