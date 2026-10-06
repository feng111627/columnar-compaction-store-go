#!/usr/bin/env bash
# 用"升级前那一版"（fc50cac，段格式 v1）的真实代码生成兼容性测试输入：
#   testdata/legacy-v1/   老库存储目录（manifest.json + seg-*.ccs）
# 重新生成：在本仓库根目录执行 bash scripts/gen_legacy_store.sh
set -euo pipefail

LEGACY_COMMIT=fc50cac
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/testdata/legacy-v1"
WT="$(mktemp -d)/legacy-src"

cleanup() { git -C "$ROOT" worktree remove --force "$WT" 2>/dev/null || true; }
trap cleanup EXIT

git -C "$ROOT" worktree add --detach "$WT" "$LEGACY_COMMIT" >/dev/null
mkdir -p "$WT/cmd/legacygen"
cp "$ROOT/scripts/legacygen/main.go" "$WT/cmd/legacygen/main.go"

rm -rf "$OUT"
mkdir -p "$OUT"
(cd "$WT" && go run ./cmd/legacygen -out "$OUT")
echo "regenerated: $OUT"
