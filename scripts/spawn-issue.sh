#!/bin/bash
set -euo pipefail

# spawn-issue.sh - Spawn a new iTerm2 tab running Claude Code for a GitHub issue
# Usage: ./spawn-issue.sh <issue-url-or-number> [description]
#
# This script opens a NEW iTerm2 tab and runs start-issue.sh in it,
# allowing the parent session to continue working while the new session runs.
#
# Requirements: macOS with iTerm2 installed

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_ISSUE_SCRIPT="${SCRIPT_DIR}/start-issue.sh"
SPAWN_DELAY="${SPAWN_DELAY:-0.5}"  # Configurable delay between spawns

usage() {
    cat <<EOF
Usage: $(basename "$0") <issue-url-or-number> [description]
       $(basename "$0") --batch <issue1> <issue2> ...

Spawns a new iTerm2 tab running Claude Code for the specified issue.

Requirements:
  - macOS
  - iTerm2 installed and running

Examples:
  $(basename "$0") 97                           # Single issue
  $(basename "$0") https://github.com/owner/repo/issues/42
  $(basename "$0") --batch 97 98 99 100         # Multiple issues in parallel

Options:
  --batch    Spawn multiple issues, each in its own tab
  --help     Show this help message

Environment:
  SPAWN_DELAY  Delay between spawns in batch mode (default: 0.5)
EOF
    exit 1
}

# Check prerequisites
check_prerequisites() {
    # Check for macOS
    if [[ "$(uname)" != "Darwin" ]]; then
        echo "Error: This script requires macOS (detected: $(uname))"
        exit 1
    fi

    # Check for iTerm2
    if ! osascript -e 'tell application "System Events" to (name of processes) contains "iTerm2"' 2>/dev/null | grep -q "true"; then
        echo "Error: iTerm2 is not running. Please start iTerm2 first."
        exit 1
    fi

    # Check for start-issue.sh
    if [[ ! -x "$START_ISSUE_SCRIPT" ]]; then
        echo "Error: start-issue.sh not found or not executable at: $START_ISSUE_SCRIPT"
        exit 1
    fi
}

# Spawn a single issue in a new iTerm2 tab
spawn_in_new_tab() {
    local issue="$1"
    local description="${2:-}"

    # Capture current directory at function start
    local current_dir
    current_dir="$(pwd)"

    # Safely escape parameters for shell
    local escaped_issue escaped_description escaped_dir escaped_script
    escaped_issue=$(printf '%q' "$issue")
    escaped_dir=$(printf '%q' "$current_dir")
    escaped_script=$(printf '%q' "$START_ISSUE_SCRIPT")

    # Build the command to run in the new tab
    local cmd="${escaped_script} ${escaped_issue}"
    if [[ -n "$description" ]]; then
        escaped_description=$(printf '%q' "$description")
        cmd="${cmd} ${escaped_description}"
    fi

    # Use AppleScript to create a new tab and run the command
    osascript <<EOF
tell application "iTerm2"
    tell current window
        create tab with default profile
        tell current session
            write text "cd ${escaped_dir} && ${cmd}"
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
        # Configurable delay to avoid overwhelming iTerm2
        sleep "$SPAWN_DELAY"
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
            check_prerequisites
            spawn_batch "$@"
            ;;
        *)
            check_prerequisites
            spawn_in_new_tab "$@"
            ;;
    esac
}

main "$@"
