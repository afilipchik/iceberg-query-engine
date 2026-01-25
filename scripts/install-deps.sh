#!/usr/bin/env bash
#
# Install system dependencies for the Query Engine project
# Supports: macOS (via Homebrew) and Ubuntu/Debian (via apt)
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Detect OS
detect_os() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "macos"
    elif [[ -f /etc/os-release ]]; then
        . /etc/os-release
        case "$ID" in
            ubuntu|debian)
                echo "debian"
                ;;
            fedora|rhel|centos|rocky|almalinux)
                echo "fedora"
                ;;
            arch|manjaro)
                echo "arch"
                ;;
            *)
                echo "unknown"
                ;;
        esac
    else
        echo "unknown"
    fi
}

# Install Rust if not present
install_rust() {
    if command -v rustc &> /dev/null; then
        info "Rust is already installed: $(rustc --version)"
    else
        info "Installing Rust via rustup..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source "$HOME/.cargo/env"
        info "Rust installed: $(rustc --version)"
    fi
}

# macOS installation via Homebrew
install_macos() {
    info "Detected macOS"

    # Check for Homebrew
    if ! command -v brew &> /dev/null; then
        info "Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi

    info "Installing dependencies via Homebrew..."

    # Core build dependencies
    brew install openssl@3 pkg-config

    # Optional but useful
    brew install cmake

    # Set environment variables for OpenSSL
    OPENSSL_PREFIX=$(brew --prefix openssl@3)

    info "Setting up environment variables..."

    # Add to shell profile if not already there
    SHELL_PROFILE=""
    if [[ -f "$HOME/.zshrc" ]]; then
        SHELL_PROFILE="$HOME/.zshrc"
    elif [[ -f "$HOME/.bashrc" ]]; then
        SHELL_PROFILE="$HOME/.bashrc"
    elif [[ -f "$HOME/.bash_profile" ]]; then
        SHELL_PROFILE="$HOME/.bash_profile"
    fi

    if [[ -n "$SHELL_PROFILE" ]]; then
        if ! grep -q "OPENSSL_DIR" "$SHELL_PROFILE"; then
            echo "" >> "$SHELL_PROFILE"
            echo "# OpenSSL for Rust builds" >> "$SHELL_PROFILE"
            echo "export OPENSSL_DIR=$OPENSSL_PREFIX" >> "$SHELL_PROFILE"
            echo "export OPENSSL_LIB_DIR=$OPENSSL_PREFIX/lib" >> "$SHELL_PROFILE"
            echo "export OPENSSL_INCLUDE_DIR=$OPENSSL_PREFIX/include" >> "$SHELL_PROFILE"
            echo "export PKG_CONFIG_PATH=\"$OPENSSL_PREFIX/lib/pkgconfig:\$PKG_CONFIG_PATH\"" >> "$SHELL_PROFILE"
            info "Added OpenSSL environment variables to $SHELL_PROFILE"
            info "Run 'source $SHELL_PROFILE' or restart your terminal"
        else
            info "OpenSSL environment variables already configured in $SHELL_PROFILE"
        fi
    fi

    # Export for current session
    export OPENSSL_DIR=$OPENSSL_PREFIX
    export OPENSSL_LIB_DIR=$OPENSSL_PREFIX/lib
    export OPENSSL_INCLUDE_DIR=$OPENSSL_PREFIX/include
    export PKG_CONFIG_PATH="$OPENSSL_PREFIX/lib/pkgconfig:$PKG_CONFIG_PATH"

    info "macOS dependencies installed successfully!"
}

# Ubuntu/Debian installation via apt
install_debian() {
    info "Detected Debian/Ubuntu"

    info "Updating package lists..."
    sudo apt-get update

    info "Installing dependencies via apt..."
    sudo apt-get install -y \
        build-essential \
        pkg-config \
        libssl-dev \
        cmake \
        curl \
        git

    info "Debian/Ubuntu dependencies installed successfully!"
}

# Fedora/RHEL installation via dnf
install_fedora() {
    info "Detected Fedora/RHEL"

    info "Installing dependencies via dnf..."
    sudo dnf install -y \
        gcc \
        gcc-c++ \
        make \
        pkg-config \
        openssl-devel \
        cmake \
        curl \
        git

    info "Fedora/RHEL dependencies installed successfully!"
}

# Arch Linux installation via pacman
install_arch() {
    info "Detected Arch Linux"

    info "Installing dependencies via pacman..."
    sudo pacman -S --needed --noconfirm \
        base-devel \
        pkg-config \
        openssl \
        cmake \
        curl \
        git

    info "Arch Linux dependencies installed successfully!"
}

# Verify installation
verify_installation() {
    info "Verifying installation..."

    local errors=0

    # Check Rust
    if command -v rustc &> /dev/null; then
        info "  Rust: $(rustc --version)"
    else
        warn "  Rust: NOT FOUND"
        errors=$((errors + 1))
    fi

    # Check Cargo
    if command -v cargo &> /dev/null; then
        info "  Cargo: $(cargo --version)"
    else
        warn "  Cargo: NOT FOUND"
        errors=$((errors + 1))
    fi

    # Check pkg-config
    if command -v pkg-config &> /dev/null; then
        info "  pkg-config: $(pkg-config --version)"
    else
        warn "  pkg-config: NOT FOUND"
        errors=$((errors + 1))
    fi

    # Check OpenSSL
    if pkg-config --exists openssl 2>/dev/null; then
        info "  OpenSSL: $(pkg-config --modversion openssl)"
    elif [[ -n "$OPENSSL_DIR" ]] && [[ -d "$OPENSSL_DIR" ]]; then
        info "  OpenSSL: Found at $OPENSSL_DIR"
    else
        warn "  OpenSSL: NOT FOUND (may need to set OPENSSL_DIR)"
        errors=$((errors + 1))
    fi

    if [[ $errors -eq 0 ]]; then
        info "All dependencies verified successfully!"
    else
        warn "Some dependencies may be missing. Check warnings above."
    fi

    return $errors
}

# Test build
test_build() {
    info "Testing build..."

    # Navigate to project root (script is in scripts/)
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

    cd "$PROJECT_DIR"

    if cargo check 2>&1; then
        info "Build check passed!"
    else
        error "Build check failed. Please check the error messages above."
    fi
}

# Main
main() {
    echo "========================================"
    echo "Query Engine - Dependency Installer"
    echo "========================================"
    echo ""

    OS=$(detect_os)

    case "$OS" in
        macos)
            install_macos
            ;;
        debian)
            install_debian
            ;;
        fedora)
            install_fedora
            ;;
        arch)
            install_arch
            ;;
        *)
            error "Unsupported operating system. Please install dependencies manually:

Required packages:
  - Rust toolchain (https://rustup.rs)
  - OpenSSL development libraries
  - pkg-config
  - C compiler (gcc/clang)
  - cmake (optional)

For other Linux distributions, install the equivalent packages."
            ;;
    esac

    # Install Rust if needed
    install_rust

    echo ""
    verify_installation

    echo ""
    info "Setup complete!"
    echo ""
    echo "Next steps:"
    echo "  1. If environment variables were added, run: source ~/.zshrc (or ~/.bashrc)"
    echo "  2. Build the project: cargo build --release"
    echo "  3. Run tests: cargo test"
    echo "  4. Generate benchmark data: cargo run --release -- generate-parquet --sf 10 --output ./data/tpch-10gb"
}

# Run with optional --test flag to also run build test
if [[ "$1" == "--test" ]]; then
    main
    test_build
else
    main
fi
