#!/bin/bash

# Define the name of the single output file
OUTPUT_FILE="all.txt"

# Clear the output file to start fresh
> "$OUTPUT_FILE"

echo "Combining files into $OUTPUT_FILE..."

# Loop through all .go and .html files in the current directory
for file in *.go *.html; do
    
    # Check if the file exists and is a regular file
    # This prevents errors if no *.go or *.html files are found
    if [ -f "$file" ]; then
    
        # Append the START indicator to the output file
        echo "==============================================" >> "$OUTPUT_FILE"
        echo "--- START: $file ---" >> "$OUTPUT_FILE"
        echo "==============================================" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE" # Add a newline for spacing
        
        # Append the content of the file
        cat "$file" >> "$OUTPUT_FILE"
        
        # Append the END indicator
        echo "" >> "$OUTPUT_FILE" # Add a newline for spacing
        echo "==============================================" >> "$OUTPUT_FILE"
        echo "--- END: $file ---" >> "$OUTPUT_FILE"
        echo "==============================================" >> "$OUTPUT_FILE"
        echo -e "\n\n" >> "$OUTPUT_FILE" # Add two newlines to separate from the next file
        
        echo "Added $file"
    fi
done

echo "Done."

echo '
check for one impactful optimization in performance possible in this code. 
show the entire function or any other structure on go, not the entire source code file content. 
do not put citations in the resulted code e.g. [cite: xx]
if the files are too big, split the answer in various parts, send each part after I input "continue"
'
