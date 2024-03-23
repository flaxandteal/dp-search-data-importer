#!/bin/bash

# Function to simulate user input
simulate_input() {
    echo "$1"
}

# Counter for iterations
count=0

# Loop to answer questions 50 times or until the Go program exits
while [ $count -lt 1000 ]; do
    simulate_input "$((count + 1))"
    simulate_input "datasets"
    simulate_input "JobID"
    simulate_input "CDID"
    simulate_input "DatasetID"
    simulate_input "Summary"
    simulate_input "title"
    simulate_input "population type Name"
    simulate_input "population type Label"
    simulate_input ""
    ((count++))
done | go run main.go