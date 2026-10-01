#!/bin/sh
set -eu

echo "Upgrading the Superset metadata database..."
superset db upgrade

admin_username="${SUPERSET_ADMIN_USERNAME:-admin}"
if ! superset fab list-users 2>/dev/null | grep -Fq "username:${admin_username}"; then
    echo "Creating the local Superset administrator..."
    superset fab create-admin \
        --username "$admin_username" \
        --firstname DATA \
        --lastname 226 \
        --email "${SUPERSET_ADMIN_EMAIL:-admin@localhost}" \
        --password "$SUPERSET_ADMIN_PASSWORD"
else
    echo "Local Superset administrator already exists; leaving it unchanged."
fi

echo "Initializing Superset roles and permissions..."
superset init
