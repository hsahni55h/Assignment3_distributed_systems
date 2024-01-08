#!/bin/bash

# Prompt for a commit message
echo "Enter your commit message:"
read commit_message

# Check if the commit message is not empty
if [ -z "$commit_message" ]; then
  echo "Commit message cannot be empty. Aborting."
  exit 1
fi

# Add all changes, commit with the provided message, and push to the default remote (usually 'origin')
git add .
git commit -m "$commit_message"
git push
