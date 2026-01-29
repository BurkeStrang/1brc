#!/usr/bin/env bash
set -euo pipefail

APP="./program/bin/Release/net10.0/1brc"
INPUT="measurements-1000000000.txt"
WORKERS="6"

echo "▶ Building (Release)..."
dotnet build -c Release >/dev/null

echo "▶ Starting app..."
$APP "$INPUT" "$WORKERS" &
APP_PID=$!

echo "   PID: $APP_PID"
echo "   Letting it warm up..."
sleep 3

dotnet-trace collect \
  -p $APP_PID \
  -o trace.nettrace

echo "▶ Waiting for app to finish..."
wait $APP_PID || true

echo "▶ Converting to Speedscope..."
dotnet-trace convert trace.nettrace --format Speedscope >/dev/null

JSON_FILE=$(ls -t *.speedscope.json | head -n1)

echo ""
echo "✅ Profile ready: $JSON_FILE"
echo ""
echo "Open https://www.speedscope.app and drag this file in."
echo ""
