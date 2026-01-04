#!/bin/bash
set -euo pipefail

# spawn-issue.sh - Spawn a new iTerm2 tab running Claude Code for a GitHub issue
# Usage: ./spawn-issue.sh <issue-url-or-number> [description]
#
# This script opens a NEW iTerm2 tab and runs start-issue.sh in it,
# allowing the parent session to continue working while the new session runs.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_ISSUE_SCRIPT="${SCRIPT_DIR}/start-issue.sh"

usage() {
    cat <<EOF
Usage: $(basename "$0") <issue-url-or-number> [description]
       $(basename "$0") --batch <issue1> <issue2> ...

Spawns a new iTerm2 tab running Claude Code for the specified issue.

Examples:
  $(basename "$0") 97                           # Single issue
  $(basename "$0") https://github.com/owner/repo/issues/42
  $(basename "$0") --batch 97 98 99 100         # Multiple issues in parallel

Options:
  --batch    Spawn multiple issues, each in its own tab
  --help     Show this help message
EOF
    exit 1
}

# Spawn a single issue in a new iTerm2 tab
spawn_in_new_tab() {
    local issue="$1"
    local description="${2:-}"

    # Build the command to run in the new tab
    local cmd="${START_ISSUE_SCRIPT} ${issue}"
    if [[ -n "$description" ]]; then
        cmd="${cmd} \"${description}\""
    fi

    # Use AppleScript to create a new tab and run the command
    osascript <<EOF
tell application "iTerm2"
    tell current window
        create tab with default profile
        tell current session
            write text "cd \"$(pwd)\" && ${cmd}"
        end tell
    end tell
end tell
EOF

    echo "Spawned new tab for issue: $issue"
}

# Spawn multiple issues in separate tabs
spawn_batch() {
    local issues=("$@")

    for issue in "${issues[@]}"; do
        spawn_in_new_tab "$issue"
        # Small delay to avoid overwhelming iTerm2
        sleep 0.5
    done

    echo ""
    echo "Spawned ${#issues[@]} tabs for issues: ${issues[*]}"
}

main() {
    if [[ $# -lt 1 ]]; then
        usage
    fi

    case "$1" in
        --help|-h)
            usage
            ;;
        --batch)
            shift
            if [[ $# -lt 1 ]]; then
                echo "Error: --batch requires at least one issue"
                exit 1
            fi
            spawn_batch "$@"
            ;;
        *)
            spawn_in_new_tab "$@"
            ;;
    esac
}

main "$@"
