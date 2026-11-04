#!/bin/sh
set -eu

file=$1
shift
part="${file}.part"

for url in "$@"; do
    attempt=1
    while [ "$attempt" -le 2 ]; do
        if curl --fail --show-error --location --connect-timeout 30 \
            --max-time 900 --speed-limit 32768 --speed-time 120 \
            --continue-at - --output "$part" "$url"; then
            mv "$part" "$file"
            exit 0
        fi
        echo "Download failed from $url (attempt $attempt/2)" >&2
        attempt=$((attempt + 1))
    done
    rm -f "$part"
done

exit 1
