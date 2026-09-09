#!/bin/sh
# gen-jwt-keys.sh — generate the Ed25519 JWT key pair the auth system
# signs/verifies tokens with (design doc §9.3, D20).
#
#   bin/gen-jwt-keys.sh [key-dir]      default ./keys
#
# Writes <key-dir>/ultima-jwt.pem  (PKCS#8 private key, mode 600)
# and   <key-dir>/ultima-jwt.pub   (PKIX/SPKI public key).
# These are the paths config keys auth.jwt_private_key_file /
# auth.jwt_public_key_file point at. The keys/ directory is gitignored —
# never commit real keys.
#
# Existing files are never overwritten unless FORCE=1 is set (rotating
# keys invalidates every outstanding token — do it deliberately).
set -eu

dir="${1:-./keys}"
priv="$dir/ultima-jwt.pem"
pub="$dir/ultima-jwt.pub"

if [ -e "$priv" ] || [ -e "$pub" ]; then
	if [ "${FORCE:-0}" != "1" ]; then
		echo "refusing to overwrite existing keys in $dir (set FORCE=1 to rotate)" >&2
		exit 1
	fi
fi

mkdir -p "$dir"
openssl genpkey -algorithm Ed25519 -out "$priv"
openssl pkey -in "$priv" -pubout -out "$pub"
chmod 600 "$priv"
chmod 644 "$pub"

echo "wrote $priv (private, 600) and $pub (public)"
