#!/usr/bin/env bash
set -euo pipefail

TARGET="aarch64-linux-android"

for tool in cargo rustup; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Required tool not found: $tool" >&2
    exit 1
  fi
done
if ! command -v cargo-apk >/dev/null 2>&1; then
  echo "cargo-apk is missing. Install it with: cargo install cargo-apk" >&2
  exit 1
fi
if [[ -z "${ANDROID_HOME:-}${ANDROID_SDK_ROOT:-}" ]]; then
  echo "Set ANDROID_HOME or ANDROID_SDK_ROOT to your Android SDK directory." >&2
  exit 1
fi
if [[ -z "${ANDROID_NDK_HOME:-}" ]]; then
  echo "Set ANDROID_NDK_HOME to your Android NDK directory." >&2
  exit 1
fi

rustup target add "$TARGET"
cargo apk build "$@"

echo "APK artifacts:"
find target -type f -name '*.apk' -print
