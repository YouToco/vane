#!/usr/bin/env bash
set -euo pipefail
umask 077

[[ $(id -u) -eq 0 ]] || { echo "broker install requires root" >&2; exit 1; }
[[ $# -eq 1 && $1 == /* ]] || {
  echo "usage: $0 ABSOLUTE_TRANSPORT_PUBLIC_KEY_FILE" >&2
  exit 2
}
public_key_file=$1
[[ -f $public_key_file && ! -L $public_key_file ]] || {
  echo "broker transport public key is unavailable" >&2
  exit 1
}
read -r key_type key_value key_extra <"$public_key_file"
[[ $key_type == ssh-ed25519 && $key_value =~ ^[A-Za-z0-9+/]+={0,2}$ && -z ${key_extra:-} ]] || {
  echo "broker transport public key is not one exact Ed25519 key" >&2
  exit 1
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
broker_dir=$(cd -- "$script_dir/../broker" && pwd -P)

if ! getent group vane-broker >/dev/null; then
  groupadd --system vane-broker
fi
if ! id vane-broker >/dev/null 2>&1; then
  useradd --system --gid vane-broker --home-dir /var/lib/vane-broker \
    --shell /bin/bash --no-create-home vane-broker
fi
[[ $(id -gn vane-broker) == vane-broker ]] || {
  echo "existing vane-broker user has the wrong primary group" >&2
  exit 1
}
usermod --home /var/lib/vane-broker --shell /bin/bash vane-broker
passwd -l vane-broker >/dev/null

install -d -o root -g root -m 0755 /usr/local/libexec
install -o root -g root -m 0755 "$broker_dir/broker-shim.sh" /usr/local/libexec/vane-broker
install -o root -g root -m 0755 \
  "$broker_dir/promote_finalized_controller.py" \
  /usr/local/libexec/vane-broker-promote

install -d -o root -g root -m 0755 /etc/sudoers.d
sudoers=$(mktemp /etc/sudoers.d/.vane-broker.XXXXXX)
printf '%s\n' \
  'vane-broker ALL=(root) NOPASSWD: /usr/local/libexec/vane-broker-promote' \
  'vane-broker ALL=(root) NOPASSWD: /opt/vane-control/current/ops/broker/run-production-handler.sh *' \
  >"$sudoers"
chown root:root "$sudoers"
chmod 0440 "$sudoers"
/usr/sbin/visudo -cf "$sudoers" >/dev/null
mv -T "$sudoers" /etc/sudoers.d/90-vane-broker

install -d -o root -g vane-broker -m 0750 /var/lib/vane-broker
install -d -o root -g vane-broker -m 0750 /var/lib/vane-broker/state
install -d -o vane-broker -g vane-broker -m 0700 \
  /var/lib/vane-broker/.ssh \
  /var/lib/vane-broker/requests \
  /var/lib/vane-broker/state/broker-work
release_lock=/var/lib/vane-broker/state/broker-work/release.lock
if [[ ! -e $release_lock ]]; then
  install -o vane-broker -g vane-broker -m 0600 /dev/null "$release_lock"
fi
[[ -f $release_lock && ! -L $release_lock ]] || {
  echo "broker release lock is unsafe" >&2
  exit 1
}
chown vane-broker:vane-broker "$release_lock"
chmod 0600 "$release_lock"
install -d -o root -g root -m 0700 /var/lib/vane-broker/evidence
authorized_keys=$(mktemp /var/lib/vane-broker/.ssh/.authorized-keys.XXXXXX)
printf 'restrict,command="/usr/local/libexec/vane-broker" %s %s vane-release-controller\n' \
  "$key_type" "$key_value" >"$authorized_keys"
chown vane-broker:vane-broker "$authorized_keys"
chmod 0600 "$authorized_keys"
mv -T "$authorized_keys" /var/lib/vane-broker/.ssh/authorized_keys

install -d -o root -g root -m 0755 /etc/ssh/sshd_config.d
sshd_config=$(mktemp /etc/ssh/sshd_config.d/.vane-broker.XXXXXX)
cat >"$sshd_config" <<'EOF'
Match User vane-broker
    AuthenticationMethods publickey
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    PermitTTY no
    X11Forwarding no
    AllowTcpForwarding no
    PermitTunnel no
    GatewayPorts no
Match all
EOF
chown root:root "$sshd_config"
chmod 0644 "$sshd_config"
mv -T "$sshd_config" /etc/ssh/sshd_config.d/90-vane-broker.conf
/usr/sbin/sshd -t
systemctl reload ssh.service

echo "root-owned forced-command broker installed"
