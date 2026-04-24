#!/bin/bash
# Install script for golars Zed extension
# Usage: curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/install-zed-extension.sh | bash

set -e

REPO_URL="https://github.com/Gaurav-Gosain/golars.git"
EXTENSION_DIR="editors/zed-golars"

# Detect platform and set extensions directory
detect_extensions_dir() {
    case "$(uname -s)" in
        Darwin)
            # macOS
            echo "$HOME/Library/Application Support/Zed/extensions"
            ;;
        Linux)
            # Try XDG_CONFIG_HOME first, then default
            if [ -n "$XDG_CONFIG_HOME" ]; then
                echo "$XDG_CONFIG_HOME/zed/extensions"
            else
                echo "$HOME/.config/zed/extensions"
            fi
            ;;
        CYGWIN*|MINGW32*|MSYS*|MINGW*)
            # Windows
            if [ -n "$LOCALAPPDATA" ]; then
                echo "$LOCALAPPDATA/Zed/extensions"
            else
                echo "$HOME/AppData/Local/Zed/extensions"
            fi
            ;;
        *)
            echo "Unsupported platform: $(uname -s)" >&2
            exit 1
            ;;
    esac
}

EXTENSIONS_DIR=$(detect_extensions_dir)
INSTALL_DIR="$EXTENSIONS_DIR/golars"

echo "Installing golars Zed extension..."
echo "Platform: $(uname -s)"
echo "Extensions directory: $EXTENSIONS_DIR"

# Create extensions directory if it doesn't exist
mkdir -p "$EXTENSIONS_DIR"

# Remove existing installation if present
if [ -d "$INSTALL_DIR" ]; then
    echo "Removing existing installation..."
    rm -rf "$INSTALL_DIR"
fi

# Clone the extension
echo "Cloning extension from $REPO_URL..."
git clone --depth 1 --filter=blob:none --sparse "$REPO_URL" "$INSTALL_DIR"

# Sparse checkout only the extension directory
cd "$INSTALL_DIR"
git sparse-checkout set "$EXTENSION_DIR"

# Move extension files to root of install directory
mv "$EXTENSION_DIR"/* .
rm -rf "$EXTENSION_DIR"

echo ""
echo "golars Zed extension installed successfully!"
echo ""
echo "Next steps:"
echo "  1. Install the LSP binary: go install github.com/Gaurav-Gosain/golars/cmd/golars-lsp@latest"
echo "  2. Restart Zed or reload extensions"
echo "  3. Open any .glr file to use the extension"
echo ""
echo "Installation location: $INSTALL_DIR"
