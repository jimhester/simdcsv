#!/bin/bash
set -euo pipefail

# start-issue.sh - Start a Claude Code session for a GitHub issue
# Usage: ./start-issue.sh <issue-url-or-number> [description]
#        ./start-issue.sh "description of new feature"

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
WORKTREE_BASE="${REPO_ROOT}/../worktrees"
MAIN_BRANCH="${MAIN_BRANCH:-main}"

usage() {
    cat <<EOF
Usage: $(basename "$0") <issue-url-or-number> [description]
       $(basename "$0") "description of new feature"

Examples:
  $(basename "$0") https://github.com/owner/repo/issues/42
  $(basename "$0") 42 "fix memory leak"
  $(basename "$0") "add dark mode support"

Environment:
  MAIN_BRANCH    Base branch for new branches (default: main)
  WORKTREE_BASE  Directory for worktrees (default: ../worktrees)
EOF
    exit 1
}

# Slugify a string for use in branch names
slugify() {
    echo "$1" | tr '[:upper:]' '[:lower:]' | \
        sed -E 's/[^a-z0-9]+/-/g' | \
        sed -E 's/^-+|-+$//g' | \
        cut -c1-50
}

# Extract issue number from URL or direct input
parse_issue_input() {
    local input="$1"

    # GitHub issue or PR URL pattern
    if [[ "$input" =~ github\.com/([^/]+)/([^/]+)/(issues|pull)/([0-9]+) ]]; then
        ISSUE_OWNER="${BASH_REMATCH[1]}"
        ISSUE_REPO="${BASH_REMATCH[2]}"
        ISSUE_TYPE="${BASH_REMATCH[3]}"  # "issues" or "pull"
        ISSUE_NUMBER="${BASH_REMATCH[4]}"
        return 0
    fi

    # Plain issue number
    if [[ "$input" =~ ^[0-9]+$ ]]; then
        ISSUE_NUMBER="$input"
        ISSUE_TYPE="issues"
        # Try to get owner/repo from git remote
        local remote_url
        remote_url=$(git remote get-url origin 2>/dev/null || echo "")
        if [[ "$remote_url" =~ github\.com[:/]([^/]+)/([^/.]+) ]]; then
            ISSUE_OWNER="${BASH_REMATCH[1]}"
            ISSUE_REPO="${BASH_REMATCH[2]}"
        fi
        return 0
    fi

    return 1
}

# Fetch issue title from GitHub API
fetch_issue_title() {
    if command -v gh &>/dev/null; then
        gh issue view "$ISSUE_NUMBER" --repo "${ISSUE_OWNER}/${ISSUE_REPO}" --json title -q .title 2>/dev/null || echo ""
    elif command -v curl &>/dev/null; then
        curl -s "https://api.github.com/repos/${ISSUE_OWNER}/${ISSUE_REPO}/issues/${ISSUE_NUMBER}" | \
            grep -o '"title"[[:space:]]*:[[:space:]]*"[^"]*"' | \
            sed 's/"title"[[:space:]]*:[[:space:]]*"\([^"]*\)"/\1/' || echo ""
    else
        echo ""
    fi
}

# Fetch PR branch name from GitHub API
fetch_pr_branch() {
    if command -v gh &>/dev/null; then
        gh pr view "$ISSUE_NUMBER" --repo "${ISSUE_OWNER}/${ISSUE_REPO}" --json headRefName -q .headRefName 2>/dev/null || echo ""
    else
        echo ""
    fi
}

# Find existing branch for an issue
find_existing_branch() {
    local issue_num="$1"
    # Look for branches matching issue-NUM-* pattern
    git branch -a --list "*issue-${issue_num}-*" 2>/dev/null | \
        sed 's/^[* ]*//' | \
        sed 's|remotes/origin/||' | \
        head -1
}

# Find existing worktree for a branch
find_existing_worktree() {
    local branch="$1"
    git worktree list --porcelain | \
        grep -A1 "^worktree" | \
        grep -B1 "branch refs/heads/${branch}$" | \
        grep "^worktree" | \
        sed 's/^worktree //' || true
}

# Main logic
main() {
    if [[ $# -lt 1 ]]; then
        usage
    fi

    local input="$1"
    local description="${2:-}"

    ISSUE_NUMBER=""
    ISSUE_OWNER=""
    ISSUE_REPO=""
    ISSUE_TYPE=""
    BRANCH_NAME=""
    WORKTREE_PATH=""

    # Try to parse as issue reference
    if parse_issue_input "$input"; then
        if [[ "$ISSUE_TYPE" == "pull" ]]; then
            echo "Found PR #${ISSUE_NUMBER}"
            # For PRs, fetch the actual branch name from GitHub
            if [[ -n "$ISSUE_OWNER" ]] && [[ -n "$ISSUE_REPO" ]]; then
                echo "Fetching PR branch from GitHub..."
                BRANCH_NAME=$(fetch_pr_branch)
            fi
            if [[ -n "$BRANCH_NAME" ]]; then
                echo "Found PR branch: $BRANCH_NAME"
            else
                echo "Could not fetch PR branch name"
                exit 1
            fi
        else
            echo "Found issue #${ISSUE_NUMBER}"

            # Check for existing branch
            BRANCH_NAME=$(find_existing_branch "$ISSUE_NUMBER")

            if [[ -n "$BRANCH_NAME" ]]; then
                echo "Found existing branch: $BRANCH_NAME"
            else
                # Create new branch name
                if [[ -z "$description" ]] && [[ -n "$ISSUE_OWNER" ]] && [[ -n "$ISSUE_REPO" ]]; then
                    echo "Fetching issue title from GitHub..."
                    description=$(fetch_issue_title)
                fi

                if [[ -n "$description" ]]; then
                    BRANCH_NAME="issue-${ISSUE_NUMBER}-$(slugify "$description")"
                else
                    BRANCH_NAME="issue-${ISSUE_NUMBER}"
                fi
                echo "Will create new branch: $BRANCH_NAME"
            fi
        fi
    else
        # Treat input as description for a new feature branch
        description="$input"
        BRANCH_NAME="feature-$(slugify "$description")"

        # Check if branch exists
        if git show-ref --verify --quiet "refs/heads/${BRANCH_NAME}" 2>/dev/null; then
            echo "Found existing branch: $BRANCH_NAME"
        else
            echo "Will create new branch: $BRANCH_NAME"
        fi
    fi

    # Check for existing worktree
    WORKTREE_PATH=$(find_existing_worktree "$BRANCH_NAME")

    if [[ -n "$WORKTREE_PATH" ]]; then
        echo "Using existing worktree: $WORKTREE_PATH"
    else
        # Create worktree directory
        mkdir -p "$WORKTREE_BASE"
        WORKTREE_PATH="${WORKTREE_BASE}/${BRANCH_NAME}"

        # Check if directory already exists (but not registered as worktree)
        if [[ -d "$WORKTREE_PATH" ]]; then
            # Check if it's a valid git directory on the correct branch
            if git -C "$WORKTREE_PATH" rev-parse --git-dir &>/dev/null; then
                local current_branch
                current_branch=$(git -C "$WORKTREE_PATH" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
                if [[ "$current_branch" == "$BRANCH_NAME" ]]; then
                    echo "Using existing directory (not registered as worktree): $WORKTREE_PATH"
                else
                    echo "Error: Directory exists but is on branch '$current_branch', expected '$BRANCH_NAME'"
                    echo "Path: $WORKTREE_PATH"
                    echo "Please remove or rename the directory and try again."
                    exit 1
                fi
            else
                echo "Error: Directory exists but is not a valid git directory: $WORKTREE_PATH"
                echo "Please remove or rename the directory and try again."
                exit 1
            fi
        # Check if branch exists (locally or remotely)
        elif git show-ref --verify --quiet "refs/heads/${BRANCH_NAME}" 2>/dev/null; then
            echo "Creating worktree for existing branch..."
            git worktree add "$WORKTREE_PATH" "$BRANCH_NAME"
        elif git show-ref --verify --quiet "refs/remotes/origin/${BRANCH_NAME}" 2>/dev/null; then
            echo "Creating worktree for remote branch..."
            git worktree add "$WORKTREE_PATH" "$BRANCH_NAME"
        else
            echo "Creating worktree with new branch from ${MAIN_BRANCH}..."
            git worktree add -b "$BRANCH_NAME" "$WORKTREE_PATH" "$MAIN_BRANCH"
        fi
    fi

    # Build the prompt for Claude
    local task_ref=""
    if [[ -n "$ISSUE_NUMBER" ]] && [[ -n "$ISSUE_OWNER" ]] && [[ -n "$ISSUE_REPO" ]]; then
        if [[ "$ISSUE_TYPE" == "pull" ]]; then
            task_ref="GitHub PR https://github.com/${ISSUE_OWNER}/${ISSUE_REPO}/pull/${ISSUE_NUMBER}"
        else
            task_ref="GitHub issue https://github.com/${ISSUE_OWNER}/${ISSUE_REPO}/issues/${ISSUE_NUMBER}"
        fi
    elif [[ -n "$ISSUE_NUMBER" ]]; then
        task_ref="issue #${ISSUE_NUMBER}"
    else
        task_ref="${description}"
    fi

    # Build comprehensive agent instructions
    local prompt
    prompt=$(cat <<EOF
Complete this task end-to-end: ${task_ref}

## Workflow

### Phase 1: Implementation
1. Read and understand the requirements
2. Plan the implementation approach
3. Implement the changes with appropriate tests
4. Ensure all tests pass locally (run: cd build && ctest --output-on-failure)
5. Commit your changes with descriptive commit messages

### Phase 2: Pull Request
1. Push your branch and open a PR using \`gh pr create\`
2. Link the PR to the issue if applicable

### Phase 3: CI & Review Loop
Repeat until the PR is ready to merge:

1. **Wait for CI**: Use \`gh pr checks --watch\` to monitor CI status
2. **Handle CI failures**: If CI fails, investigate logs with \`gh pr checks\` and \`gh run view\`, fix issues, push fixes
3. **Check for reviews**: Use \`gh pr view --comments\` and \`gh api repos/{owner}/{repo}/pulls/{pr}/reviews\` to check for review feedback
4. **Address critical feedback**: Fix any issues marked as "REQUEST_CHANGES" or critical comments. Respond to review comments as you address them.
5. **Re-request review if needed**: After addressing feedback, re-request review if appropriate

### Phase 4: Merge
Once CI passes and there are no blocking reviews:
1. Merge the PR using \`gh pr merge --squash --delete-branch\`
2. Confirm the merge succeeded

## Phase 5: Follow up issues
If there are any un-addressed issues identified from the PR review or implementation, please open new issues to track them.

## Guidelines
- Be thorough but avoid over-engineering
- Write clear commit messages explaining the "why"
- Keep PR description updated with significant changes
- If a feature is related to performance be sure to benchmark the new changes
  in comparison to the current code, and only accept changes that improve
  performance.
- If blocked by external factors (permissions, unclear requirements), explain and stop
EOF
)

    # Set iTerm2 tab/window title
    local title=""
    if [[ -n "$ISSUE_NUMBER" ]]; then
        title="Issue #${ISSUE_NUMBER}"
    else
        title="${description:0:30}"
    fi
    # \033]0; sets both window and tab title, \007 terminates
    printf '\033]0;%s\007' "$title"

    echo ""
    echo "Starting Claude Code session..."
    echo "  Worktree: $WORKTREE_PATH"
    echo "  Branch:   $BRANCH_NAME"
    echo "  Task:     $task_ref"
    echo ""

    # Change to worktree and start Claude Code
    cd "$WORKTREE_PATH"
    exec claude --dangerously-skip-permissions "$prompt"
}

main "$@"
