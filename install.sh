#!/bin/sh
set -e

# Orbi installer
#
# Local install (from repo, after cargo build --release):
#   ./install.sh --local
#
# Remote install (once release binaries are published to GitHub):
#   curl -fsSL https://raw.githubusercontent.com/epyphite/orbi/main/install.sh | sh
#
# Custom install dir:
#   ORBI_INSTALL_DIR=~/.local/bin ./install.sh --local

VERSION="0.3.1"
INSTALL_DIR="${ORBI_INSTALL_DIR:-/usr/local/bin}"
REPO_URL="https://github.com/epyphite/orbi/releases/download"
LOCAL_MODE=false

for arg in "$@"; do
    case "$arg" in
        --local) LOCAL_MODE=true ;;
        --help|-h)
            echo "Usage: ./install.sh [--local]"
            echo ""
            echo "Options:"
            echo "  --local    Install from local build (requires cargo build --release first)"
            echo ""
            echo "Environment:"
            echo "  ORBI_INSTALL_DIR    Install directory (default: /usr/local/bin)"
            exit 0
            ;;
    esac
done

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$ARCH" in
    x86_64|amd64) ARCH="x86_64" ;;
    aarch64|arm64) ARCH="aarch64" ;;
    *) echo "Error: unsupported architecture: $ARCH"; exit 1 ;;
esac

case "$OS" in
    linux) PLATFORM="linux" ;;
    darwin) PLATFORM="darwin" ;;
    *) echo "Error: unsupported OS: $OS"; exit 1 ;;
esac

install_binary() {
    local src="$1" dst="$2"
    if [ -w "$(dirname "$dst")" ]; then
        cp "$src" "$dst"
        chmod +x "$dst"
    else
        echo "  (requires sudo)"
        sudo cp "$src" "$dst"
        sudo chmod +x "$dst"
    fi
}

if [ "$LOCAL_MODE" = true ]; then
    # ── Local install from cargo build ──
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    ORBI_BIN="$SCRIPT_DIR/target/release/orbi"
    AGENT_BIN="$SCRIPT_DIR/target/release/kvmql-agent"

    if [ ! -f "$ORBI_BIN" ]; then
        echo "Binary not found at $ORBI_BIN"
        echo "Run 'cargo build --release' first."
        exit 1
    fi

    echo "Installing Orbi v${VERSION} from local build..."
    echo ""

    mkdir -p "$INSTALL_DIR" 2>/dev/null || true

    echo "  orbi → $INSTALL_DIR/orbi"
    install_binary "$ORBI_BIN" "$INSTALL_DIR/orbi"

    if [ -f "$AGENT_BIN" ]; then
        echo "  kvmql-agent → $INSTALL_DIR/kvmql-agent"
        install_binary "$AGENT_BIN" "$INSTALL_DIR/kvmql-agent"
    fi
else
    # ── Remote install from GitHub releases ──
    ARCHIVE="orbi-${VERSION}-${PLATFORM}-${ARCH}"
    URL="${REPO_URL}/v${VERSION}/${ARCHIVE}.tar.gz"

    echo "Installing Orbi v${VERSION} for ${PLATFORM}/${ARCH}..."
    echo ""

    TMPDIR=$(mktemp -d)
    trap 'rm -rf "$TMPDIR"' EXIT

    echo "Downloading ${URL}..."
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$URL" -o "$TMPDIR/orbi.tar.gz"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$URL" -O "$TMPDIR/orbi.tar.gz"
    else
        echo "Error: curl or wget required"
        exit 1
    fi

    tar -xzf "$TMPDIR/orbi.tar.gz" -C "$TMPDIR"

    mkdir -p "$INSTALL_DIR" 2>/dev/null || true

    echo "  orbi → $INSTALL_DIR/orbi"
    install_binary "$TMPDIR/$ARCHIVE/orbi" "$INSTALL_DIR/orbi"

    if [ -f "$TMPDIR/$ARCHIVE/kvmql-agent" ]; then
        echo "  kvmql-agent → $INSTALL_DIR/kvmql-agent"
        install_binary "$TMPDIR/$ARCHIVE/kvmql-agent" "$INSTALL_DIR/kvmql-agent"
    fi
fi

echo ""
echo "Orbi v${VERSION} installed successfully."
echo ""
echo "  orbi version              # verify install"
echo "  orbi init                 # initialize registry"
echo "  orbi --simulate 'SHOW VERSION;'   # test with no credentials"
echo "  orbi shell                # interactive REPL"
echo ""
