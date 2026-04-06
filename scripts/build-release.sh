#!/bin/bash
set -e

VERSION="0.3.1"
OUT_DIR="dist"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$SCRIPT_DIR"

echo "=== Building Orbi v${VERSION} release ==="
echo ""

# Build release binaries
echo "[1/4] Compiling..."
cargo build --release

# Detect platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$OS" in
    linux) PLATFORM="linux" ;;
    darwin) PLATFORM="darwin" ;;
    *) PLATFORM="$OS" ;;
esac

ARCHIVE="orbi-${VERSION}-${PLATFORM}-${ARCH}"

# Create dist directory
echo "[2/4] Packaging ${ARCHIVE}..."
rm -rf "$OUT_DIR/$ARCHIVE"
mkdir -p "$OUT_DIR/$ARCHIVE"

cp target/release/orbi        "$OUT_DIR/$ARCHIVE/"
cp target/release/kvmql-agent "$OUT_DIR/$ARCHIVE/"
cp MANUAL.md                  "$OUT_DIR/$ARCHIVE/"
cp README.md                  "$OUT_DIR/$ARCHIVE/"
cp LICENSE                    "$OUT_DIR/$ARCHIVE/"
cp install.sh                 "$OUT_DIR/$ARCHIVE/"
cp -r examples                "$OUT_DIR/$ARCHIVE/"

# Create tarball
echo "[3/4] Creating tarball..."
cd "$OUT_DIR"
tar -czf "${ARCHIVE}.tar.gz" "$ARCHIVE"
rm -rf "$ARCHIVE"

# Generate checksum
echo "[4/4] Generating checksum..."
sha256sum "${ARCHIVE}.tar.gz" > "${ARCHIVE}.tar.gz.sha256"

cd "$SCRIPT_DIR"

TARBALL_SIZE=$(du -h "$OUT_DIR/${ARCHIVE}.tar.gz" | cut -f1)
CHECKSUM=$(cat "$OUT_DIR/${ARCHIVE}.tar.gz.sha256" | cut -d' ' -f1)

echo ""
echo "=== Release built ==="
echo ""
echo "  Archive:  $OUT_DIR/${ARCHIVE}.tar.gz ($TARBALL_SIZE)"
echo "  Checksum: ${CHECKSUM:0:16}..."
echo ""
echo "Install locally:"
echo "  ./install.sh --local"
echo ""
echo "Or from the tarball:"
echo "  tar -xzf $OUT_DIR/${ARCHIVE}.tar.gz"
echo "  cd $ARCHIVE && ./install.sh --local"
echo ""
