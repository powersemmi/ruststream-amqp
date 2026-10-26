#!/usr/bin/env bash
# The certificate chain the test stand's mutual-TLS listener uses: an authority, a server
# certificate for `localhost`, and a client certificate for the identity SASL EXTERNAL logs in as,
# both signed by that authority. A second, unrelated authority signs a client certificate the
# stand does not trust, for the refusal case. Nothing here is a secret and nothing here is
# committed: a certificate has a lifetime, and a repository is the wrong place to keep one.
#
# Usage: scripts/stand_tls_certs.sh [directory]   (default: .stand-tls next to this repository)
set -euo pipefail

dir="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.stand-tls}"
mkdir -p "$dir"

# The stand is started often and a chain lasts three months, so this is a no-op until it is a day
# from expiring.
if openssl x509 -in "$dir/client.crt" -checkend 86400 -noout >/dev/null 2>&1; then
    exit 0
fi

for ca in ca stranger-ca; do
    openssl req -x509 -newkey rsa:2048 -nodes -days 90 -subj "/CN=ruststream-amqp-$ca" \
        -keyout "$dir/$ca.key" -out "$dir/$ca.crt" 2>/dev/null
done

# name, the authority that signs it, the extensions it carries.
issue() {
    openssl req -newkey rsa:2048 -nodes -subj "/CN=$1" \
        -keyout "$dir/$1.key" -out "$dir/$1.csr" 2>/dev/null
    openssl x509 -req -in "$dir/$1.csr" -CA "$dir/$2.crt" -CAkey "$dir/$2.key" \
        -CAcreateserial -days 90 -out "$dir/$1.crt" -extfile <(printf '%s\n' "$3") 2>/dev/null
}

# The client verifies the server by the name in the URL, so the stand is reached as `localhost`.
issue server ca $'basicConstraints=CA:FALSE\nsubjectAltName=DNS:localhost'
issue client ca $'basicConstraints=CA:FALSE\nextendedKeyUsage=clientAuth'
issue stranger stranger-ca $'basicConstraints=CA:FALSE\nextendedKeyUsage=clientAuth'

# The broker reads the server's key and certificate from one file.
cat "$dir/server.key" "$dir/server.crt" > "$dir/server.pem"

# The broker reads its key after dropping privileges, so the mode has to let it.
chmod 644 "$dir"/*.key "$dir/server.pem"
