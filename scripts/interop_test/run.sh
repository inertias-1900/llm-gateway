#!/bin/sh
# 协议互转矩阵回归：起一个假上游(8099) + 第二个网关实例(8098, 独立 config)，跑
# 3 客户端协议 × 3 上游协议 × 流式/非流式 + 多轮工具回传。不碰你自己的线上实例。
# 需要 Node >= 22.5（用了内置 node:sqlite）。
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
TMP="${TMPDIR:-/tmp}/gw-interop-test"
rm -rf "$TMP"; mkdir -p "$TMP"
cp "$ROOT/server.mjs" "$HERE/mock3.mjs" "$HERE/client.mjs" "$HERE/config.json" "$TMP/"
node "$TMP/mock3.mjs" 8099 & MOCK=$!
node "$TMP/server.mjs" & GW=$!
trap 'kill $MOCK $GW 2>/dev/null' EXIT INT TERM
sleep 2
node "$TMP/client.mjs"
