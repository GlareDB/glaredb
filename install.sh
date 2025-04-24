#!/bin/sh

set -e

# You can override this:
#
# export GLAREDB_INSTALL_DIR=/some/other/path
install_dir="${GLAREDB_INSTALL_DIR:-$HOME/.glaredb/bin}"
dest="$install_dir/glaredb"

echo "***"
echo "Running GlareDB install script"
echo "Installing to '${install_dir}'"
echo "***"

# Detect OS
case "$(uname -s)" in
  Linux)   os=linux ;;
  Darwin)  os=macos  ;;
  *)
    echo "Error: unsupported OS '$(uname -s)'" >&2
    exit 1
    ;;
esac

# Detect architecture
case "$(uname -m)" in
  x86_64)        arch=x86_64 ;;
  aarch64|arm64) arch=arm64  ;;
  *)
    echo "Error: unsupported arch '$(uname -m)'" >&2
    exit 1
    ;;
esac

# Asset name
asset="glaredb-${os}-${arch}"

# Direct download url
download_url="https://github.com/GlareDB/glaredb/releases/latest/download/${asset}"

# Download
tmpf=$(mktemp)
trap 'rm -f "${tmpf}"' EXIT
curl --fail --silent --show-error --location "${download_url}" -o "${tmpf}"

# Make executable
chmod +x "${tmpf}"

# Move to dest
mkdir -p "${install_dir}"
mv "${tmpf}" "${dest}"

echo "GlareDB installed!"
echo "You can run it by typing:"
echo "    ${dest}"
