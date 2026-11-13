#!/bin/bash
# Activate virtual environment
source ../.venv/bin/activate

echo "============================================================"
echo "🚀 STARTING STREAMING PIPELINE (KAFKA + ML)"
echo "============================================================"

# Trap Ctrl+C
trap 'echo -e "\n🛑 Stopping pipeline manually..."; kill $CONSUMER_PID 2>/dev/null; exit' INT

# Start the Consumer in the background
echo "🎧 Starting Consumer (waiting for events)..."
python consumer.py &
CONSUMER_PID=$!

# Give the consumer 3 seconds to connect
sleep 3

# Start the Producer (blocks until finished)
echo -e "📤 Starting Producer (sending events)...\n"
python producer.py

# Wait for the Consumer to process remaining messages
echo -e "\n⏳ Producer finished. Waiting 5 seconds for the Consumer..."
sleep 5

# Stop the Consumer
kill $CONSUMER_PID 2>/dev/null

echo -e "\n✅ Pipeline stopped successfully. All done!"