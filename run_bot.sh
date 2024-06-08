#!/bin/bash

# Run Bot.py with "NIFTY" in the background
python3 Bot.py "NIFTY" &

# Run Bot.py with "BANKNIFTY" in the background
python3 Bot.py "BANKNIFTY" &

# Wait for all background processes to finish (optional)
wait

echo "Both Python scripts are running in the background."
