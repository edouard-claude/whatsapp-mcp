#!/bin/sh
# Installe wa-mcp et wa-bridge (dernière release) dans ~/.local/bin, ou dans
# $WA_MCP_INSTALL_DIR. Les deux binaires doivent rester côte à côte.
set -eu

repo="edouard-claude/whatsapp-mcp"
dest="${WA_MCP_INSTALL_DIR:-$HOME/.local/bin}"

case "$(uname -s)" in
  Darwin) os="apple-darwin" ;;
  Linux) os="unknown-linux-gnu" ;;
  *) echo "système non pris en charge : $(uname -s) (compiler depuis les sources)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64 | aarch64) arch="aarch64" ;;
  x86_64 | amd64) arch="x86_64" ;;
  *) echo "architecture non prise en charge : $(uname -m)" >&2; exit 1 ;;
esac
target="$arch-$os"

tag="${WA_MCP_VERSION:-$(curl -fsSL "https://api.github.com/repos/$repo/releases/latest" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -n 1)}"
[ -n "$tag" ] || { echo "impossible de trouver la dernière release" >&2; exit 1; }
name="wa-mcp-$tag-$target"
url="https://github.com/$repo/releases/download/$tag/$name.tar.gz"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
echo "téléchargement de $name"
curl -fsSL "$url" -o "$tmp/$name.tar.gz"
curl -fsSL "$url.sha256" -o "$tmp/$name.tar.gz.sha256"
(cd "$tmp" && if command -v shasum >/dev/null 2>&1; then shasum -a 256 -c "$name.tar.gz.sha256"; else sha256sum -c "$name.tar.gz.sha256"; fi) >/dev/null
tar -xzf "$tmp/$name.tar.gz" -C "$tmp"

mkdir -p "$dest"
install -m 0755 "$tmp/$name/wa-mcp" "$dest/wa-mcp"
install -m 0755 "$tmp/$name/wa-bridge" "$dest/wa-bridge"
echo "installé dans $dest : wa-mcp, wa-bridge ($tag)"

case ":$PATH:" in
  *":$dest:"*) ;;
  *) echo "attention : $dest n'est pas dans le PATH" ;;
esac
cat <<NEXT

Suite :
  wa-mcp doctor
  wa-mcp pair perso --phone 33612345678     # numéro international, sans + ni 0
  claude mcp add whatsapp -- $dest/wa-mcp serve
NEXT
