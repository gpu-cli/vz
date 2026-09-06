SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
REPO_ROOT := $(abspath $(ROOT_DIR)/..)
CRATES_DIR := $(REPO_ROOT)/crates

KERNEL_PROFILE ?= developer

ifeq ($(KERNEL_PROFILE),developer)
DEFAULT_OUT_DIR := $(ROOT_DIR)/out
KERNEL_BASE_CONFIG ?= defconfig
KERNEL_CONFIG_FRAGMENT ?= $(ROOT_DIR)/vz-linux.config
KERNEL_SECURITY_PROFILE ?= developer-nested-virt
KERNEL_CAPABILITIES ?= vsock virtiofs hvc0_serial ext4_root overlayfs netns seccomp io_uring cgroup_bpf user_ns nested_virt tun btrfs_snapshots device_mapper dm_crypt
else ifeq ($(KERNEL_PROFILE),container)
DEFAULT_OUT_DIR := $(ROOT_DIR)/out/container
KERNEL_BASE_CONFIG ?= allnoconfig
KERNEL_CONFIG_FRAGMENT ?= $(ROOT_DIR)/vz-linux-container.config
KERNEL_SECURITY_PROFILE ?= container-hardened
KERNEL_CAPABILITIES ?= vsock virtiofs hvc0_serial ext4_root overlayfs netns seccomp io_uring cgroup_bpf btrfs_snapshots nfsd container_sandbox device_mapper dm_crypt
else
$(error unsupported KERNEL_PROFILE '$(KERNEL_PROFILE)'; expected developer or container)
endif

OUT_DIR ?= $(DEFAULT_OUT_DIR)
ALL_OUT_DIR ?= $(ROOT_DIR)/out
SRC_DIR ?= $(ROOT_DIR)/src
BUILD_DIR ?= $(SRC_DIR)/build
CACHE_DIR ?= $(ROOT_DIR)/.cache
TRUST_EXISTING_KERNEL_IMAGE ?= 0

INITRAMFS_TEMPLATE_DIR := $(ROOT_DIR)/initramfs
INITRAMFS_ROOT := $(OUT_DIR)/initramfs-root
CA_TRUST_SOURCE := $(ROOT_DIR)/ca-trust
CA_TRUST_HELPER := $(ROOT_DIR)/ca-trust.py

ARCH ?= arm64
CROSS_COMPILE ?= aarch64-linux-gnu-
JOBS ?= $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 8)

include $(ROOT_DIR)/kernel-version.mk
KERNEL_URL ?= https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-$(KERNEL_VERSION).tar.xz
KERNEL_ARCHIVE := $(CACHE_DIR)/linux-$(KERNEL_VERSION).tar.xz
KERNEL_SRC := $(SRC_DIR)/linux-$(KERNEL_VERSION)-$(KERNEL_ARCHIVE_SHA256)
KERNEL_BUILD_DIR := $(BUILD_DIR)/$(KERNEL_PROFILE)/linux-$(KERNEL_VERSION)
KERNEL_IMAGE := $(OUT_DIR)/vmlinux

BUSYBOX_VERSION ?= 1.37.0
BUSYBOX_URL ?= https://busybox.net/downloads/busybox-$(BUSYBOX_VERSION).tar.bz2
BUSYBOX_ARCHIVE := $(CACHE_DIR)/busybox-$(BUSYBOX_VERSION).tar.bz2
# https://busybox.net/downloads/busybox-1.37.0.tar.bz2.sha256
BUSYBOX_ARCHIVE_SHA256 ?= 3311dff32e746499f4df0d5df04d7eb396382d7e108bb9250e7b519b837043a4
BUSYBOX_SRC := $(SRC_DIR)/busybox-$(BUSYBOX_VERSION)-$(BUSYBOX_ARCHIVE_SHA256)
BUSYBOX_BUILD_DIR := $(BUILD_DIR)/$(KERNEL_PROFILE)/busybox-$(BUSYBOX_VERSION)
BUSYBOX_BINARY := $(OUT_DIR)/busybox
SOURCE_BUILD_HELPER := $(ROOT_DIR)/source-build.py
SOURCE_BUILD_RECIPES := --recipe "$(ROOT_DIR)/Makefile" --recipe "$(ROOT_DIR)/Dockerfile" \
	--recipe "$(ROOT_DIR)/kernel-version.mk" --recipe "$(SOURCE_BUILD_HELPER)" \
	--recipe "$(ROOT_DIR)/docker-build.sh"

YOUKI_VERSION := $(shell sed -n 's/^YOUKI_VERSION=//p' $(ROOT_DIR)/youki/inputs.env)
YOUKI_BINARY := $(OUT_DIR)/youki
YOUKI_RECIPE := $(ROOT_DIR)/youki/build.sh

IPTABLES_VERSION ?= 1.8.13
IPTABLES_RELEASE_ARCHIVE := iptables-$(IPTABLES_VERSION).tar.xz
IPTABLES_URL ?= https://www.netfilter.org/projects/iptables/files/$(IPTABLES_RELEASE_ARCHIVE)
IPTABLES_ARCHIVE := $(CACHE_DIR)/$(IPTABLES_RELEASE_ARCHIVE)
IPTABLES_ARCHIVE_SHA256 ?= 1afcd33da9e8f913ace6a2126788162e207e26f5d5e29c6573c0e581ffc58b99
IPTABLES_SRC_DIR ?= $(SRC_DIR)
IPTABLES_SRC := $(IPTABLES_SRC_DIR)/iptables-$(IPTABLES_VERSION)
IPTABLES_BINARY := $(OUT_DIR)/xtables-legacy-multi
IPTABLES_HOST ?= aarch64-linux-musl
ifeq ($(shell uname -s),Darwin)
IPTABLES_CC ?= aarch64-linux-musl-gcc
IPTABLES_STRIP ?= aarch64-linux-musl-strip
IPTABLES_READELF ?= aarch64-linux-musl-readelf
IPTABLES_CPPFLAGS ?=
else
IPTABLES_CC ?= musl-gcc
IPTABLES_STRIP ?= strip
IPTABLES_READELF ?= readelf
# Debian keeps Linux UAPI asm headers in its target multiarch directory, which
# the musl wrapper intentionally omits from its default libc include search.
IPTABLES_CPPFLAGS ?= -idirafter /usr/include/$(shell $(IPTABLES_CC) -dumpmachine) -idirafter /usr/include
endif

E2FSPROGS_VERSION := 1.47.3
E2FSPROGS_ARCHIVE := $(CACHE_DIR)/e2fsprogs-$(E2FSPROGS_VERSION).tar.xz
E2FSPROGS_URL := https://www.kernel.org/pub/linux/kernel/people/tytso/e2fsprogs/v$(E2FSPROGS_VERSION)/e2fsprogs-$(E2FSPROGS_VERSION).tar.xz
E2FSPROGS_HELPER := $(ROOT_DIR)/e2fsprogs-build.py

ifeq ($(KERNEL_PROFILE),developer)
INITRAMFS_PROFILE_INPUTS := $(IPTABLES_BINARY) e2fsprogs $(CA_TRUST_HELPER) \
	$(CA_TRUST_SOURCE)/inputs.json $(CA_TRUST_SOURCE)/cacert.pem \
	$(CA_TRUST_SOURCE)/LICENSE $(CA_TRUST_SOURCE)/install.sh
IPTABLES_VERSION_JSON := "$(IPTABLES_VERSION)"
else
INITRAMFS_PROFILE_INPUTS :=
IPTABLES_VERSION_JSON := null
endif

GUEST_AGENT_TARGET ?= aarch64-unknown-linux-musl
GUEST_AGENT_BUILD_TOOL ?= cargo
GUEST_AGENT_BINARY := $(CRATES_DIR)/target/$(GUEST_AGENT_TARGET)/release/vz-guest-agent

INITRAMFS_IMAGE := $(OUT_DIR)/initramfs.img
VERSION_JSON := $(OUT_DIR)/version.json
DEVELOPER_PROBE_HELPER := $(ROOT_DIR)/developer-probe.py
DEVELOPER_PROBE_ARCHIVE := $(OUT_DIR)/developer-probe-rootfs.tar
DEVELOPER_PROBE_METADATA := $(OUT_DIR)/developer-probe.json
ifeq ($(KERNEL_PROFILE),developer)
VERSION_PROFILE_INPUTS := developer-probe
else
VERSION_PROFILE_INPUTS :=
endif

AGENT_VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' $(CRATES_DIR)/vz-guest-agent/Cargo.toml | head -1)
AGENT_PROTOCOL_REVISION := $(shell sed -n 's/^pub const AGENT_PROTOCOL_REVISION: u32 = \([0-9][0-9]*\);/\1/p' $(CRATES_DIR)/vz-agent-proto/src/lib.rs | head -1)
BUILD_TIMESTAMP := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

.PHONY: help all all-profiles dirs clean distclean kernel kernel-config-summary initramfs guest-agent youki iptables developer-probe version docker-build docker-build-all

help:
	@echo "Linux artifact build targets:"
	@echo "  make all            Build vmlinux, initramfs.img, youki, version.json for KERNEL_PROFILE=$(KERNEL_PROFILE)"
	@echo "  make all-profiles   Build developer and container profile artifact bundles"
	@echo "  make kernel         Build only Linux kernel image"
	@echo "  make kernel-config-summary  Generate .config and print enabled option counts"
	@echo "  make initramfs      Build only initramfs image"
	@echo "  make youki          Build/verify pinned static youki with v2, devices, seccomp"
	@echo "  make iptables        Build pinned static iptables for the developer guest"
	@echo "  make version        Generate version.json"
	@echo "  make docker-build   Build selected profile artifacts in Docker"
	@echo "  make docker-build-all Build developer and container profiles in Docker"
	@echo "  make clean          Remove linux/out"
	@echo "  make distclean      Remove out/src/cache"
	@echo ""
	@echo "Profiles:"
	@echo "  KERNEL_PROFILE=developer  Broad dev/host VM kernel with nested virt"
	@echo "  KERNEL_PROFILE=container  allnoconfig sandbox kernel with btrfs snapshots and nfsd"

all: $(KERNEL_IMAGE) $(INITRAMFS_IMAGE) $(YOUKI_BINARY) $(VERSION_JSON)

all-profiles:
	$(MAKE) KERNEL_PROFILE=developer OUT_DIR="$(ALL_OUT_DIR)" all
	$(MAKE) KERNEL_PROFILE=container OUT_DIR="$(ALL_OUT_DIR)/container" all

dirs:
	mkdir -p "$(OUT_DIR)" "$(SRC_DIR)" "$(CACHE_DIR)"

$(KERNEL_ARCHIVE): | dirs
	@echo "Downloading Linux kernel $(KERNEL_VERSION)"
	curl -fL "$(KERNEL_URL)" -o "$@"

.PHONY: kernel-source-verify busybox-source-verify kernel-cache-verify source-check
source-check: kernel-source-verify busybox-source-verify

kernel-source-verify: $(KERNEL_ARCHIVE) | dirs
	python3 "$(SOURCE_BUILD_HELPER)" prepare --archive "$(KERNEL_ARCHIVE)" \
		--sha256 "$(KERNEL_ARCHIVE_SHA256)" --root-name "linux-$(KERNEL_VERSION)" \
		--destination "$(KERNEL_SRC)" --build-dir "$(KERNEL_BUILD_DIR)"

ifeq ($(TRUST_EXISTING_KERNEL_IMAGE),1)
kernel-cache-verify:
	python3 "$(SOURCE_BUILD_HELPER)" verify --kind kernel --artifact "$(KERNEL_IMAGE)" \
		--profile "$(KERNEL_PROFILE)" --sha256 "$(KERNEL_ARCHIVE_SHA256)" \
		--arch "$(ARCH)" --cross-compile="$(CROSS_COMPILE)" --base-config "$(KERNEL_BASE_CONFIG)" \
		--config "$(KERNEL_CONFIG_FRAGMENT)" $(SOURCE_BUILD_RECIPES)
$(KERNEL_IMAGE): kernel-cache-verify
else
$(KERNEL_IMAGE): kernel-source-verify $(KERNEL_CONFIG_FRAGMENT)
	@echo "Building Linux kernel image ($(KERNEL_PROFILE) profile)"
	python3 "$(SOURCE_BUILD_HELPER)" build --kind kernel --artifact "$(KERNEL_IMAGE)" \
		--profile "$(KERNEL_PROFILE)" --archive "$(KERNEL_ARCHIVE)" --sha256 "$(KERNEL_ARCHIVE_SHA256)" \
		--source "$(KERNEL_SRC)" --build-dir "$(KERNEL_BUILD_DIR)" --arch "$(ARCH)" \
		--cross-compile="$(CROSS_COMPILE)" --jobs "$(JOBS)" --base-config "$(KERNEL_BASE_CONFIG)" \
		--config "$(KERNEL_CONFIG_FRAGMENT)" $(SOURCE_BUILD_RECIPES)
endif

kernel: $(KERNEL_IMAGE)

kernel-config-summary: kernel-source-verify $(KERNEL_CONFIG_FRAGMENT)
	@echo "Generating Linux kernel config summary ($(KERNEL_PROFILE) profile)"
	$(MAKE) -C "$(KERNEL_SRC)" O="$(KERNEL_BUILD_DIR)" ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) $(KERNEL_BASE_CONFIG)
	cd "$(KERNEL_BUILD_DIR)" && "$(KERNEL_SRC)/scripts/kconfig/merge_config.sh" -m -O "$(KERNEL_BUILD_DIR)" .config "$(KERNEL_CONFIG_FRAGMENT)"
	$(MAKE) -C "$(KERNEL_SRC)" O="$(KERNEL_BUILD_DIR)" ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) olddefconfig
	@awk '\
		/^CONFIG_/ && /=y$$/ { y++ } \
		/^CONFIG_/ && /=m$$/ { m++ } \
		/^# CONFIG_/ && / is not set$$/ { n++ } \
		END { printf "profile=$(KERNEL_PROFILE) enabled_y=%d enabled_m=%d disabled=%d\n", y+0, m+0, n+0 } \
	' "$(KERNEL_BUILD_DIR)/.config"

$(BUSYBOX_ARCHIVE): | dirs
	@echo "Downloading BusyBox $(BUSYBOX_VERSION)"
	curl -fL "$(BUSYBOX_URL)" -o "$@"

busybox-source-verify: $(BUSYBOX_ARCHIVE) | dirs
	python3 "$(SOURCE_BUILD_HELPER)" prepare --archive "$(BUSYBOX_ARCHIVE)" \
		--sha256 "$(BUSYBOX_ARCHIVE_SHA256)" --root-name "busybox-$(BUSYBOX_VERSION)" \
		--destination "$(BUSYBOX_SRC)" --build-dir "$(BUSYBOX_BUILD_DIR)"

$(BUSYBOX_BINARY): busybox-source-verify
	@echo "Building BusyBox (static)"
	python3 "$(SOURCE_BUILD_HELPER)" build --kind busybox --artifact "$(BUSYBOX_BINARY)" \
		--profile "$(KERNEL_PROFILE)" --archive "$(BUSYBOX_ARCHIVE)" --sha256 "$(BUSYBOX_ARCHIVE_SHA256)" \
		--source "$(BUSYBOX_SRC)" --build-dir "$(BUSYBOX_BUILD_DIR)" --arch "$(ARCH)" \
		--cross-compile="$(CROSS_COMPILE)" --jobs "$(JOBS)" $(SOURCE_BUILD_RECIPES)

# Always revalidate a cached candidate and existing output, including executable
# mode. Timestamp-only reuse can retain a featureless or non-executable runtime.
.PHONY: youki-verify
youki-verify:

$(YOUKI_BINARY): youki-verify | dirs
	bash "$(YOUKI_RECIPE)" --install "$(YOUKI_BINARY)"

youki: $(YOUKI_BINARY)

$(E2FSPROGS_ARCHIVE): | dirs
	curl -fL "$(E2FSPROGS_URL)" -o "$@"

.PHONY: e2fsprogs
e2fsprogs: $(E2FSPROGS_ARCHIVE) $(E2FSPROGS_HELPER) | dirs
	@test "$(KERNEL_PROFILE)" = developer || { echo 'ext4 Docker tooling belongs only to Developer bundles' >&2; exit 1; }
	python3 "$(E2FSPROGS_HELPER)" --archive "$(E2FSPROGS_ARCHIVE)" --out-dir "$(OUT_DIR)/e2fsprogs" --jobs "$(JOBS)"

$(IPTABLES_ARCHIVE): | dirs
	@echo "Downloading pinned iptables $(IPTABLES_VERSION) source"
	curl -fL "$(IPTABLES_URL)" -o "$@"

ifeq ($(shell uname -s),Darwin)
# Upstream contains xt_TCPMSS.h/xt_tcpmss.h and case-distinct extension sources.
# A default macOS filesystem aliases those names and silently corrupts extraction.
$(IPTABLES_BINARY): $(IPTABLES_ARCHIVE) $(ROOT_DIR)/Makefile $(ROOT_DIR)/iptables-build-darwin.sh | dirs
	bash "$(ROOT_DIR)/iptables-build-darwin.sh" "$(REPO_ROOT)" "$(OUT_DIR)" \
		"$(IPTABLES_ARCHIVE)" "$(IPTABLES_VERSION)" "$(IPTABLES_ARCHIVE_SHA256)" "$(DOCKER_BUILD_JOBS)"
else
$(IPTABLES_SRC)/.extracted: $(IPTABLES_ARCHIVE) | dirs
	@echo "Verifying and extracting iptables $(IPTABLES_VERSION) source"
	@ARCHIVE_SHA="$$(shasum -a 256 "$(IPTABLES_ARCHIVE)" | cut -d' ' -f1)"; \
	if [[ "$$ARCHIVE_SHA" != "$(IPTABLES_ARCHIVE_SHA256)" ]]; then \
		echo "iptables archive checksum mismatch: expected $(IPTABLES_ARCHIVE_SHA256), found $$ARCHIVE_SHA" >&2; \
		exit 1; \
	fi
	mkdir -p "$(IPTABLES_SRC_DIR)"
	rm -rf "$(IPTABLES_SRC)"
	tar -xf "$(IPTABLES_ARCHIVE)" -C "$(IPTABLES_SRC_DIR)"
	touch "$@"

$(IPTABLES_BINARY): $(IPTABLES_SRC)/.extracted $(ROOT_DIR)/Makefile | dirs
	@echo "Building static iptables $(IPTABLES_VERSION) legacy backend (linux/arm64 musl)"
	@if [[ -f "$(IPTABLES_SRC)/Makefile" ]]; then \
		$(MAKE) -C "$(IPTABLES_SRC)" distclean; \
	fi
	cd "$(IPTABLES_SRC)" && \
		CC="$(IPTABLES_CC)" CPPFLAGS="$(IPTABLES_CPPFLAGS)" LDFLAGS="-static" ./configure \
			--host="$(IPTABLES_HOST)" \
			--prefix=/usr \
			--disable-nftables \
			--disable-shared \
			--enable-static \
			--disable-devel
	$(MAKE) -C "$(IPTABLES_SRC)" -j"$(JOBS)" LDFLAGS="-all-static"
	cp "$(IPTABLES_SRC)/iptables/xtables-legacy-multi" "$@.tmp"
	"$(IPTABLES_STRIP)" "$@.tmp"
	@file "$@.tmp" | grep -Eq 'ELF 64-bit LSB.*ARM aarch64.*statically linked' || { \
		echo "iptables binary must be a static linux/arm64 ELF: $$(file "$@.tmp")" >&2; \
		exit 1; \
	}
	@command -v "$(IPTABLES_READELF)" >/dev/null || { \
		echo "required ELF verifier not found: $(IPTABLES_READELF)" >&2; \
		exit 1; \
	}
	@ELF_PROGRAM_HEADERS="$$("$(IPTABLES_READELF)" -l "$@.tmp")" || { \
		echo "failed to read iptables ELF program headers" >&2; \
		exit 1; \
	}; \
	if grep -q 'INTERP' <<<"$$ELF_PROGRAM_HEADERS"; then \
		echo "iptables binary must not contain a dynamic program interpreter" >&2; \
		exit 1; \
	fi
	mv "$@.tmp" "$@"
endif

ifeq ($(KERNEL_PROFILE),developer)
iptables: $(IPTABLES_BINARY)
else
iptables:
	@echo "iptables is intentionally available only in the developer guest profile" >&2
	@exit 1
endif

guest-agent:
	@echo "Building vz-guest-agent for $(GUEST_AGENT_TARGET)"
	@if [[ "$(GUEST_AGENT_BUILD_TOOL)" == "zigbuild" ]]; then \
		cd "$(CRATES_DIR)" && cargo zigbuild --release -p vz-guest-agent --target "$(GUEST_AGENT_TARGET)"; \
	else \
		if ! rustup target list --installed | grep -q "^$(GUEST_AGENT_TARGET)$$"; then \
			rustup target add "$(GUEST_AGENT_TARGET)"; \
		fi; \
		cd "$(CRATES_DIR)" && cargo build --release -p vz-guest-agent --target "$(GUEST_AGENT_TARGET)"; \
	fi

$(INITRAMFS_IMAGE): $(BUSYBOX_BINARY) $(INITRAMFS_PROFILE_INPUTS) guest-agent $(ROOT_DIR)/Makefile \
	$(INITRAMFS_TEMPLATE_DIR)/init \
	$(INITRAMFS_TEMPLATE_DIR)/etc/resolv.conf \
	$(INITRAMFS_TEMPLATE_DIR)/etc/udhcpc.script
	@echo "Assembling initramfs"
	rm -rf "$(INITRAMFS_ROOT)"
	mkdir -p "$(INITRAMFS_ROOT)/bin" "$(INITRAMFS_ROOT)/sbin" "$(INITRAMFS_ROOT)/etc" \
		"$(INITRAMFS_ROOT)/dev" "$(INITRAMFS_ROOT)/proc" "$(INITRAMFS_ROOT)/sys" \
		"$(INITRAMFS_ROOT)/tmp" "$(INITRAMFS_ROOT)/run" "$(INITRAMFS_ROOT)/mnt/rootfs" \
		"$(INITRAMFS_ROOT)/usr/bin"

	cp "$(BUSYBOX_BINARY)" "$(INITRAMFS_ROOT)/bin/busybox"
	chmod +x "$(INITRAMFS_ROOT)/bin/busybox"
	for cmd in sh mount umount mkdir cp cat ls ip hostname chroot switch_root udhcpc echo sleep dmesg tail head wget nsenter unshare mke2fs blkid; do \
		ln -sf /bin/busybox "$(INITRAMFS_ROOT)/bin/$$cmd"; \
	done
	ln -sf /bin/busybox "$(INITRAMFS_ROOT)/sbin/udhcpc"

ifeq ($(KERNEL_PROFILE),developer)
	python3 "$(CA_TRUST_HELPER)" --source "$(CA_TRUST_SOURCE)" --install-root "$(INITRAMFS_ROOT)"
	cp "$(OUT_DIR)/e2fsprogs/mke2fs" "$(INITRAMFS_ROOT)/sbin/mke2fs"
	cp "$(OUT_DIR)/e2fsprogs/dumpe2fs" "$(INITRAMFS_ROOT)/sbin/dumpe2fs"
	cp "$(OUT_DIR)/e2fsprogs/e2fsprogs.build.json" "$(INITRAMFS_ROOT)/etc/vz-e2fsprogs.json"
	cp "$(IPTABLES_BINARY)" "$(INITRAMFS_ROOT)/sbin/xtables-legacy-multi"
	chmod +x "$(INITRAMFS_ROOT)/sbin/xtables-legacy-multi"
	for cmd in iptables iptables-save iptables-restore ip6tables ip6tables-save ip6tables-restore; do \
		ln -sf /sbin/xtables-legacy-multi "$(INITRAMFS_ROOT)/sbin/$$cmd"; \
	done
endif

	cp "$(INITRAMFS_TEMPLATE_DIR)/init" "$(INITRAMFS_ROOT)/init"
	cp "$(INITRAMFS_TEMPLATE_DIR)/etc/resolv.conf" "$(INITRAMFS_ROOT)/etc/resolv.conf"
	cp "$(INITRAMFS_TEMPLATE_DIR)/etc/udhcpc.script" "$(INITRAMFS_ROOT)/etc/udhcpc.script"
	chmod +x "$(INITRAMFS_ROOT)/init" "$(INITRAMFS_ROOT)/etc/udhcpc.script"

	cp "$(GUEST_AGENT_BINARY)" "$(INITRAMFS_ROOT)/usr/bin/vz-guest-agent"
	chmod +x "$(INITRAMFS_ROOT)/usr/bin/vz-guest-agent"

	cd "$(INITRAMFS_ROOT)" && find . | cpio -o -H newc | gzip -9 > "$(INITRAMFS_IMAGE)"

initramfs: $(INITRAMFS_IMAGE)

developer-probe: $(BUSYBOX_BINARY) $(DEVELOPER_PROBE_HELPER)
	@test "$(KERNEL_PROFILE)" = developer || { echo 'startup Docker probe belongs only to Developer bundles' >&2; exit 1; }
	python3 "$(DEVELOPER_PROBE_HELPER)" --busybox "$(BUSYBOX_BINARY)" \
		--provenance "$(BUSYBOX_BINARY).build.json" --source-sha256 "$(BUSYBOX_ARCHIVE_SHA256)" \
		--busybox-version "$(BUSYBOX_VERSION)" --out-dir "$(OUT_DIR)"

$(VERSION_JSON): $(KERNEL_IMAGE) $(INITRAMFS_IMAGE) $(YOUKI_BINARY) $(ROOT_DIR)/Makefile $(VERSION_PROFILE_INPUTS)
	@echo "Generating version metadata"
	@KERNEL_SHA="$$(shasum -a 256 "$(KERNEL_IMAGE)" | cut -d' ' -f1)"; \
	INITRAMFS_SHA="$$(shasum -a 256 "$(INITRAMFS_IMAGE)" | cut -d' ' -f1)"; \
	YOUKI_SHA="$$(shasum -a 256 "$(YOUKI_BINARY)" | cut -d' ' -f1)"; \
	PROBE_JSON=null; \
	if [[ "$(KERNEL_PROFILE)" == developer ]]; then PROBE_JSON="$$(cat "$(DEVELOPER_PROBE_METADATA)")"; fi; \
	CAPABILITIES_JSON=""; \
	for capability in $(KERNEL_CAPABILITIES); do \
		if [[ -n "$$CAPABILITIES_JSON" ]]; then CAPABILITIES_JSON="$$CAPABILITIES_JSON,\n"; fi; \
		CAPABILITIES_JSON="$$CAPABILITIES_JSON    \"$$capability\""; \
	done; \
	printf '{\n  "kernel": "$(KERNEL_VERSION)",\n  "profile": "$(KERNEL_PROFILE)",\n  "security_profile": "$(KERNEL_SECURITY_PROFILE)",\n  "busybox": "$(BUSYBOX_VERSION)",\n  "agent": "$(AGENT_VERSION)",\n  "agent_protocol_revision": $(AGENT_PROTOCOL_REVISION),\n  "youki": "$(YOUKI_VERSION)",\n  "iptables": $(IPTABLES_VERSION_JSON),\n  "built": "$(BUILD_TIMESTAMP)",\n  "sha256_vmlinux": "%s",\n  "sha256_initramfs": "%s",\n  "sha256_youki": "%s",\n  "developer_probe": %s,\n  "capabilities": [\n%b\n  ]\n}\n' "$$KERNEL_SHA" "$$INITRAMFS_SHA" "$$YOUKI_SHA" "$$PROBE_JSON" "$$CAPABILITIES_JSON" > "$(VERSION_JSON)"

version: $(VERSION_JSON)

DOCKER_BUILD_JOBS ?= 4
LINUX_DOCKER_TARGET ?= all
ifeq ($(LINUX_DOCKER_TARGET),all)
DOCKER_YOUKI_PREREQUISITE := youki
else
DOCKER_YOUKI_PREREQUISITE :=
endif

docker-build: $(KERNEL_ARCHIVE) $(BUSYBOX_ARCHIVE) $(DOCKER_YOUKI_PREREQUISITE)
	@echo "Building linux artifacts in Docker ($(KERNEL_PROFILE) profile, JOBS=$(DOCKER_BUILD_JOBS))"
	bash "$(ROOT_DIR)/docker-build.sh" "$(REPO_ROOT)" "$(OUT_DIR)" "$(CACHE_DIR)" "$(KERNEL_PROFILE)" "$(DOCKER_BUILD_JOBS)"

docker-build-all:
	@echo "Building all linux artifact profiles in Docker (JOBS=$(DOCKER_BUILD_JOBS))"
	$(MAKE) KERNEL_PROFILE=developer OUT_DIR="$(ALL_OUT_DIR)" docker-build
	$(MAKE) KERNEL_PROFILE=container OUT_DIR="$(ALL_OUT_DIR)/container" docker-build

clean:
	rm -rf "$(OUT_DIR)"

distclean: clean
	rm -rf "$(SRC_DIR)" "$(CACHE_DIR)"
