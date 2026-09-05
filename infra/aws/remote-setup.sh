#!/usr/bin/env bash
# Runs ON each EC2 host (piped over ssh by provision.sh). Installs the
# toolchain and the brokers the runner manages. Everything user-space under
# ~/opt and ~/bin, matching the primes layout the runner already searches.
#   $1 = builder|echo   (only the builder gets rustup)
set -euo pipefail
ROLE=${1:-echo}
export DEBIAN_FRONTEND=noninteractive
sudo systemctl stop unattended-upgrades 2>/dev/null || true
sudo apt-get -qq update
sudo apt-get -qq install -y build-essential pkg-config libssl-dev uuid-dev libsasl2-dev zlib1g-dev \
  python3 openjdk-21-jre-headless redis-server curl unzip irqbalance >/dev/null
sudo systemctl disable --now redis-server >/dev/null 2>&1 || true
sudo systemctl disable --now irqbalance >/dev/null 2>&1 || true
mkdir -p ~/opt ~/bin
cd ~/opt
[ -x cmake/bin/cmake ] || { curl -fsSL https://github.com/Kitware/CMake/releases/download/v4.4.3/cmake-4.4.3-linux-x86_64.tar.gz | tar xz && mv cmake-4.4.3-linux-x86_64 cmake; }
[ -x ~/bin/nats-server ] || { curl -fsSL https://github.com/nats-io/nats-server/releases/download/v2.14.6/nats-server-v2.14.6-linux-amd64.tar.gz | tar xz && mv nats-server-v2.14.6-linux-amd64/nats-server ~/bin/ && rm -rf nats-server-v2.14.6-linux-amd64; }
[ -x kafka/bin/kafka-server-start.sh ] || { curl -fsSL https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz | tar xz && mv kafka_2.13-4.3.1 kafka; }
if [ "$ROLE" = builder ]; then
  # rusteron runs bindgen over Aeron's headers; bindgen needs libclang and
  # clang's own stddef.h, or it fails with "'stddef.h' file not found".
  sudo apt-get -qq install -y clang libclang-dev libbsd-dev >/dev/null   # libbsd: Aeron static link wants -lbsd
  [ -x ~/.cargo/bin/cargo ] || curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal -q >/dev/null
  # rusteron's build script formats its generated bindings and panics if
  # rustfmt is missing, and the minimal profile omits it.
  ~/.cargo/bin/rustup component add rustfmt >/dev/null 2>&1
fi
# Kernel side: nothing exotic. Busy-poll receive and taskset do the work.
echo "setup done on $(hostname): $(nproc) vcpus, kernel $(uname -r), cmake $(~/opt/cmake/bin/cmake --version | head -1 | awk '{print $3}'), java $(java -version 2>&1 | head -1 | awk -F'"' '{print $2}')"
