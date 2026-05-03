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
KERNEL_CAPABILITIES ?= vsock virtiofs hvc0_serial ext4_root overlayfs netns seccomp io_uring nested_virt tun btrfs_snapshots
else ifeq ($(KERNEL_PROFILE),container)
DEFAULT_OUT_DIR := $(ROOT_DIR)/out/container
KERNEL_BASE_CONFIG ?= allnoconfig
KERNEL_CONFIG_FRAGMENT ?= $(ROOT_DIR)/vz-linux-container.config
KERNEL_SECURITY_PROFILE ?= container-hardened
KERNEL_CAPABILITIES ?= vsock virtiofs hvc0_serial ext4_root overlayfs netns seccomp io_uring btrfs_snapshots nfsd container_sandbox
else
$(error unsupported KERNEL_PROFILE '$(KERNEL_PROFILE)'; expected developer or container)
endif

OUT_DIR ?= $(DEFAULT_OUT_DIR)
SRC_DIR ?= $(ROOT_DIR)/src
CACHE_DIR ?= $(ROOT_DIR)/.cache
TRUST_EXISTING_KERNEL_IMAGE ?= 0

INITRAMFS_TEMPLATE_DIR := $(ROOT_DIR)/initramfs
INITRAMFS_ROOT := $(OUT_DIR)/initramfs-root

ARCH ?= arm64
CROSS_COMPILE ?= aarch64-linux-gnu-
JOBS ?= $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 8)

include $(ROOT_DIR)/kernel-version.mk
KERNEL_URL ?= https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-$(KERNEL_VERSION).tar.xz
KERNEL_ARCHIVE := $(CACHE_DIR)/linux-$(KERNEL_VERSION).tar.xz
KERNEL_SRC := $(SRC_DIR)/linux-$(KERNEL_VERSION)
KERNEL_IMAGE := $(OUT_DIR)/vmlinux

BUSYBOX_VERSION ?= 1.37.0
BUSYBOX_URL ?= https://busybox.net/downloads/busybox-$(BUSYBOX_VERSION).tar.bz2
BUSYBOX_ARCHIVE := $(CACHE_DIR)/busybox-$(BUSYBOX_VERSION).tar.bz2
BUSYBOX_SRC := $(SRC_DIR)/busybox-$(BUSYBOX_VERSION)
BUSYBOX_BINARY := $(OUT_DIR)/busybox

YOUKI_VERSION ?= 0.5.7
YOUKI_RELEASE_ARCHIVE := youki-$(YOUKI_VERSION)-aarch64-musl.tar.gz
YOUKI_URL ?= https://github.com/youki-dev/youki/releases/download/v$(YOUKI_VERSION)/$(YOUKI_RELEASE_ARCHIVE)
YOUKI_ARCHIVE := $(CACHE_DIR)/$(YOUKI_RELEASE_ARCHIVE)
YOUKI_ARCHIVE_SHA256 ?= b3002d9d39b04f797e783745f92cffec9e0caa464254be98aa0b4dfc184f0233
YOUKI_BINARY := $(OUT_DIR)/youki

GUEST_AGENT_TARGET ?= aarch64-unknown-linux-musl
GUEST_AGENT_BUILD_TOOL ?= cargo
GUEST_AGENT_BINARY := $(CRATES_DIR)/target/$(GUEST_AGENT_TARGET)/release/vz-guest-agent

INITRAMFS_IMAGE := $(OUT_DIR)/initramfs.img
VERSION_JSON := $(OUT_DIR)/version.json

AGENT_VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' $(CRATES_DIR)/vz-guest-agent/Cargo.toml | head -1)
AGENT_PROTOCOL_REVISION := $(shell sed -n 's/^pub const AGENT_PROTOCOL_REVISION: u32 = \([0-9][0-9]*\);/\1/p' $(CRATES_DIR)/vz-agent-proto/src/lib.rs | head -1)
BUILD_TIMESTAMP := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

.PHONY: help all all-profiles dirs clean distclean kernel kernel-config-summary initramfs guest-agent youki version docker-build docker-build-all

help:
	@echo "Linux artifact build targets:"
	@echo "  make all            Build vmlinux, initramfs.img, youki, version.json for KERNEL_PROFILE=$(KERNEL_PROFILE)"
	@echo "  make all-profiles   Build developer and container profile artifact bundles"
	@echo "  make kernel         Build only Linux kernel image"
	@echo "  make kernel-config-summary  Generate .config and print enabled option counts"
	@echo "  make initramfs      Build only initramfs image"
	@echo "  make youki          Download pinned youki linux/arm64 runtime binary"
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
	$(MAKE) KERNEL_PROFILE=developer all
	$(MAKE) KERNEL_PROFILE=container all

dirs:
	mkdir -p "$(OUT_DIR)" "$(SRC_DIR)" "$(CACHE_DIR)"

$(KERNEL_ARCHIVE): | dirs
	@echo "Downloading Linux kernel $(KERNEL_VERSION)"
	curl -fL "$(KERNEL_URL)" -o "$@"

$(KERNEL_SRC)/.extracted: $(KERNEL_ARCHIVE) | dirs
	rm -rf "$(KERNEL_SRC)"
	tar -xf "$(KERNEL_ARCHIVE)" -C "$(SRC_DIR)"
	touch "$@"

ifeq ($(TRUST_EXISTING_KERNEL_IMAGE),1)
$(KERNEL_IMAGE):
	@test -s "$(KERNEL_IMAGE)" || { echo "missing cached kernel image for $(KERNEL_PROFILE): $(KERNEL_IMAGE)" >&2; exit 1; }
else
$(KERNEL_IMAGE): $(KERNEL_SRC)/.extracted $(KERNEL_CONFIG_FRAGMENT)
	@echo "Building Linux kernel image ($(KERNEL_PROFILE) profile)"
	cd "$(KERNEL_SRC)" && make ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) $(KERNEL_BASE_CONFIG)
	cd "$(KERNEL_SRC)" && scripts/kconfig/merge_config.sh -m .config "$(KERNEL_CONFIG_FRAGMENT)"
	cd "$(KERNEL_SRC)" && make ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) olddefconfig
	cd "$(KERNEL_SRC)" && make -j"$(JOBS)" ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) Image
	cp "$(KERNEL_SRC)/arch/arm64/boot/Image" "$(KERNEL_IMAGE)"
endif

kernel: $(KERNEL_IMAGE)

kernel-config-summary: $(KERNEL_SRC)/.extracted $(KERNEL_CONFIG_FRAGMENT)
	@echo "Generating Linux kernel config summary ($(KERNEL_PROFILE) profile)"
	cd "$(KERNEL_SRC)" && make ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) $(KERNEL_BASE_CONFIG)
	cd "$(KERNEL_SRC)" && scripts/kconfig/merge_config.sh -m .config "$(KERNEL_CONFIG_FRAGMENT)"
	cd "$(KERNEL_SRC)" && make ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) olddefconfig
	@awk '\
		/^CONFIG_/ && /=y$$/ { y++ } \
		/^CONFIG_/ && /=m$$/ { m++ } \
		/^# CONFIG_/ && / is not set$$/ { n++ } \
		END { printf "profile=$(KERNEL_PROFILE) enabled_y=%d enabled_m=%d disabled=%d\n", y+0, m+0, n+0 } \
	' "$(KERNEL_SRC)/.config"

$(BUSYBOX_ARCHIVE): | dirs
	@echo "Downloading BusyBox $(BUSYBOX_VERSION)"
	curl -fL "$(BUSYBOX_URL)" -o "$@"

$(BUSYBOX_SRC)/.extracted: $(BUSYBOX_ARCHIVE) | dirs
	rm -rf "$(BUSYBOX_SRC)"
	tar -xf "$(BUSYBOX_ARCHIVE)" -C "$(SRC_DIR)"
	touch "$@"

$(BUSYBOX_BINARY): $(BUSYBOX_SRC)/.extracted
	@echo "Building BusyBox (static)"
	cd "$(BUSYBOX_SRC)" && make ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) defconfig
	cd "$(BUSYBOX_SRC)" && sed -i.bak \
		-e 's/^# CONFIG_STATIC is not set/CONFIG_STATIC=y/' \
		-e 's/^CONFIG_SHA1_HWACCEL=y/# CONFIG_SHA1_HWACCEL is not set/' \
		-e 's/^CONFIG_SHA256_HWACCEL=y/# CONFIG_SHA256_HWACCEL is not set/' \
		.config && rm -f .config.bak
	cd "$(BUSYBOX_SRC)" && (set +o pipefail; yes "" | make ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) oldconfig)
	cd "$(BUSYBOX_SRC)" && make -j"$(JOBS)" ARCH=$(ARCH) CROSS_COMPILE=$(CROSS_COMPILE) busybox
	cp "$(BUSYBOX_SRC)/busybox" "$(BUSYBOX_BINARY)"

$(YOUKI_ARCHIVE): | dirs
	@echo "Downloading pinned youki $(YOUKI_VERSION) (linux/arm64 musl)"
	curl -fL "$(YOUKI_URL)" -o "$@"

$(YOUKI_BINARY): $(YOUKI_ARCHIVE) | dirs
	@echo "Verifying and extracting youki runtime"
	@ARCHIVE_SHA="$$(shasum -a 256 "$(YOUKI_ARCHIVE)" | cut -d' ' -f1)"; \
	if [[ "$$ARCHIVE_SHA" != "$(YOUKI_ARCHIVE_SHA256)" ]]; then \
		echo "youki archive checksum mismatch: expected $(YOUKI_ARCHIVE_SHA256), found $$ARCHIVE_SHA" >&2; \
		exit 1; \
	fi
	rm -f "$(YOUKI_BINARY)"
	tar -xzf "$(YOUKI_ARCHIVE)" -C "$(OUT_DIR)" youki
	chmod +x "$(YOUKI_BINARY)"

youki: $(YOUKI_BINARY)

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

$(INITRAMFS_IMAGE): $(BUSYBOX_BINARY) guest-agent \
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

	cp "$(INITRAMFS_TEMPLATE_DIR)/init" "$(INITRAMFS_ROOT)/init"
	cp "$(INITRAMFS_TEMPLATE_DIR)/etc/resolv.conf" "$(INITRAMFS_ROOT)/etc/resolv.conf"
	cp "$(INITRAMFS_TEMPLATE_DIR)/etc/udhcpc.script" "$(INITRAMFS_ROOT)/etc/udhcpc.script"
	chmod +x "$(INITRAMFS_ROOT)/init" "$(INITRAMFS_ROOT)/etc/udhcpc.script"

	cp "$(GUEST_AGENT_BINARY)" "$(INITRAMFS_ROOT)/usr/bin/vz-guest-agent"
	chmod +x "$(INITRAMFS_ROOT)/usr/bin/vz-guest-agent"

	cd "$(INITRAMFS_ROOT)" && find . | cpio -o -H newc | gzip -9 > "$(INITRAMFS_IMAGE)"

initramfs: $(INITRAMFS_IMAGE)

$(VERSION_JSON): $(KERNEL_IMAGE) $(INITRAMFS_IMAGE) $(YOUKI_BINARY)
	@echo "Generating version metadata"
	@KERNEL_SHA="$$(shasum -a 256 "$(KERNEL_IMAGE)" | cut -d' ' -f1)"; \
	INITRAMFS_SHA="$$(shasum -a 256 "$(INITRAMFS_IMAGE)" | cut -d' ' -f1)"; \
	YOUKI_SHA="$$(shasum -a 256 "$(YOUKI_BINARY)" | cut -d' ' -f1)"; \
	CAPABILITIES_JSON=""; \
	for capability in $(KERNEL_CAPABILITIES); do \
		if [[ -n "$$CAPABILITIES_JSON" ]]; then CAPABILITIES_JSON="$$CAPABILITIES_JSON,\n"; fi; \
		CAPABILITIES_JSON="$$CAPABILITIES_JSON    \"$$capability\""; \
	done; \
	printf '{\n  "kernel": "$(KERNEL_VERSION)",\n  "profile": "$(KERNEL_PROFILE)",\n  "security_profile": "$(KERNEL_SECURITY_PROFILE)",\n  "busybox": "$(BUSYBOX_VERSION)",\n  "agent": "$(AGENT_VERSION)",\n  "agent_protocol_revision": $(AGENT_PROTOCOL_REVISION),\n  "youki": "$(YOUKI_VERSION)",\n  "built": "$(BUILD_TIMESTAMP)",\n  "sha256_vmlinux": "%s",\n  "sha256_initramfs": "%s",\n  "sha256_youki": "%s",\n  "capabilities": [\n%b\n  ]\n}\n' "$$KERNEL_SHA" "$$INITRAMFS_SHA" "$$YOUKI_SHA" "$$CAPABILITIES_JSON" > "$(VERSION_JSON)"

version: $(VERSION_JSON)

docker-build:
	@echo "Building linux artifacts in Docker ($(KERNEL_PROFILE) profile)"
	docker build -t vz-linux-builder "$(ROOT_DIR)"
	docker run --rm \
		-v "$(REPO_ROOT):/workspace" \
		-w /workspace/linux \
		vz-linux-builder \
		make all KERNEL_PROFILE="$(KERNEL_PROFILE)" CROSS_COMPILE= GUEST_AGENT_BUILD_TOOL=cargo

docker-build-all:
	@echo "Building all linux artifact profiles in Docker"
	docker build -t vz-linux-builder "$(ROOT_DIR)"
	docker run --rm \
		-v "$(REPO_ROOT):/workspace" \
		-w /workspace/linux \
		vz-linux-builder \
		make all-profiles CROSS_COMPILE= GUEST_AGENT_BUILD_TOOL=cargo

clean:
	rm -rf "$(OUT_DIR)"

distclean: clean
	rm -rf "$(SRC_DIR)" "$(CACHE_DIR)"
