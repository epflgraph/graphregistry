#!/usr/bin/env bash
#
# scan.sh — secret scan for this repository, with human-readable triage.
#
# Runs three complementary scans:
#   1. TruffleHog over full git history (candidates verified LIVE against
#      the provider APIs — the only unambiguous "real secret" signal).
#   2. Gitleaks over git history (default + aggressive custom rules).
#   3. Gitleaks over the current worktree, including gitignored files.
#
# Findings are then triaged into risk buckets using file context and git
# status, so keyword matches in sample data, generated artifacts, docs and
# gitignored scratch dirs don't drown out real problems:
#   🔴 CRITICAL — trufflehog VERIFIED the credential is live → rotate now
#   🟠 HIGH     — secret-shaped match in tracked, active source/config paths
#   🟡 MEDIUM   — tests/examples, or gitignored files (skim; can't be committed)
#   ⚪ LOW      — sample data, generated artifacts, docs, gitignored scratch
#
# Usage:
#   ./scan.sh              # triaged human report
#   ./scan.sh --json PATH  # also save the classified findings (redacted) to PATH
#   NO_COLOR=1 ./scan.sh   # disable colors
#
# Exit codes:
#   0  clean — all scanners ran, no findings
#   1  findings detected (triaged above the exit)
#   2  scan failure — missing tool, not a git repo, or a scanner crashed
#      (a failed scan is NEVER reported as clean)

set -euo pipefail
umask 077

# --- Setup -----------------------------------------------------------------

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
    echo "ERROR: scan.sh must be run inside the git repository it scans." >&2
    exit 2
}
REPO_NAME="$(basename "$REPO_ROOT")"
HEAD_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo '?')"
N_COMMITS="$(git rev-list --count HEAD 2>/dev/null || echo '?')"
NOW="$(date '+%Y-%m-%d %H:%M %Z')"

JSON_OUT=""
case "${1:-}" in
    "") ;;
    --help|-h)
        sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
    --json)
        JSON_OUT="${2:?--json requires a file path}"
        shift 2
        [ "${1:-}" = "" ] || { echo "ERROR: unexpected argument: $1" >&2; exit 2; }
        ;;
    *)
        echo "ERROR: unknown option: $1 (try --help)" >&2
        exit 2
        ;;
esac

for tool in trufflehog gitleaks jq git; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "ERROR: required tool '$tool' not found in PATH." >&2
        exit 2
    }
done

# Colors and emoji only when stdout is a terminal (and NO_COLOR is unset).
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    BLD=$'\e[1m' DIM=$'\e[2m' RED=$'\e[1;31m' GRN=$'\e[1;32m' YLW=$'\e[1;33m'
    CYN=$'\e[1;36m' ORG=$'\e[38;5;208m' GRY=$'\e[90m' RST=$'\e[0m'
else
    BLD="" DIM="" RED="" GRN="" YLW="" CYN="" ORG="" GRY="" RST=""
fi

# Private workspace outside the repo; removed even on error/interrupt.
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/graphregistry-secret-scan.XXXXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

cd "$REPO_ROOT"

echo ""
echo "${BLD}🔒 SECRET SCAN — ${REPO_NAME}${RST}  ${DIM}@ ${HEAD_SHA} · ${NOW}${RST}"

# Shared scanner exit-code contract (trufflehog --fail uses 183 natively;
# gitleaks is pinned to the same code via --exit-code):
#   0 -> clean, 183 -> findings, else -> scanner failure
FINDINGS_EXIT=183

# Plain-English descriptions for gitleaks rule ids (fallback for unknown).
declare -A RULE_DESC=(
    ["generic-password-assignment"]="a password/passwd keyword followed by a literal value"
    ["generic-secret-assignment"]="a secret/… keyword followed by a literal value"
    ["generic-token-assignment"]="a token/… keyword followed by a literal value"
    ["generic-api-key-assignment"]="an api-key keyword followed by a literal value"
    ["generic-api-key"]="an api-key-like literal (default gitleaks rule)"
    ["high-entropy-secret-near-keyword"]="a random-looking string near a secret keyword"
    ["base64ish-secret-near-keyword"]="a base64-like blob near a secret keyword"
    ["env-secret-export"]="an env-var export of a secret-named variable"
    ["private-key-generic"]="a private key block"
    ["ssh-private-key-inline"]="an OpenSSH private key block"
    ["jwt-token"]="a JSON Web Token"
    ["bearer-token"]="a Bearer token literal"
    ["authorization-header"]="an Authorization header value"
    ["basic-auth-url"]="a URL with embedded user:password"
    ["database-url-with-password"]="a DB connection URL with inline password"
    ["jdbc-url-with-password"]="a JDBC URL with password parameter"
    ["db-password-parameter"]="a DB password config parameter"
    ["aws-access-key-id-custom"]="an AWS access key id"
    ["aws-secret-access-key-assignment"]="an AWS secret access key"
    ["github-token"]="a GitHub token (ghp_/gho_/…)"
    ["gitlab-token"]="a GitLab token (glpat-…)"
    ["slack-token"]="a Slack token (xox…-)"
    ["slack-webhook"]="a Slack webhook URL"
    ["discord-webhook"]="a Discord webhook URL"
    ["stripe-key"]="a Stripe secret/restricted key"
    ["sendgrid-api-key"]="a SendGrid API key"
    ["twilio-api-key"]="a Twilio API key SID"
    ["npm-token"]="an npm auth token"
    ["pypi-token"]="a PyPI token"
    ["docker-auth-config"]="a Docker registry auth blob"
    ["firebase-server-key"]="a Firebase server key"
    ["google-api-key"]="a Google API key"
    ["gcp-api-key"]="a Google Cloud API key"
    ["mailgun-key"]="a Mailgun API key"
    ["shopify-token"]="a Shopify access token"
    ["telegram-bot-token"]="a Telegram bot token"
    ["linear-client-secret"]="a Linear API client secret"
)
rule_desc() { printf '%s' "${RULE_DESC[$1]:-regex pattern match}"; }

bar() { # bar <count> <max> <color> — horizontal gauge scaled to max bucket
    local w=22 fill=0 i
    if [ "$2" -gt 0 ]; then fill=$(( $1 * w / $2 )); fi
    if [ "$fill" -eq 0 ] && [ "$1" -gt 0 ]; then fill=1; fi
    local out=""
    for ((i = 0; i < fill; i++)); do out+="█"; done
    for ((i = fill; i < w; i++)); do out+="░"; done
    printf '%s%s%s' "$3" "$out" "$RST"
}

# --- Scan 1: TruffleHog (git history, with live verification) ---------------

echo ""
echo "${BLD}🔍 [1/3] TruffleHog — full git history + live credential verification${RST}"
echo "    candidates are checked against the real provider API: \"verified\" ⇒ live credential"

set +e
trufflehog git "file://$REPO_ROOT" \
    --json --log-level=-1 \
    --filter-entropy=3.0 \
    --fail --fail-on-scan-errors \
    --no-update \
    >"$TMP_DIR/trufflehog.jsonl" 2>"$TMP_DIR/trufflehog.err"
trufflehog_rc=$?
set -e

trufflehog_status="clean"
case "$trufflehog_rc" in
    0) trufflehog_status="clean" ;;
    "$FINDINGS_EXIT") trufflehog_status="findings" ;;
    *) trufflehog_status="error" ;;
esac

# Normalize to JSONL: tool, scan, rule, verified, file, line, commit, redacted.
jq -cs --arg repo "$REPO_ROOT" 'map({
    tool: "trufflehog", scan: "history",
    rule: (.DetectorName // "unknown"),
    verified: (.Verified == true),
    file: ((.SourceMetadata.Data.Git.file // "?") | ltrimstr($repo + "/")),
    line: (.SourceMetadata.Data.Git.line // 0),
    commit: ((.SourceMetadata.Data.Git.commit // "?")[0:8]),
    redacted: (.Redacted // null),
    tracked: true, ignored: false
}) | .[]' "$TMP_DIR/trufflehog.jsonl" >"$TMP_DIR/th_norm.jsonl"

th_candidates="$(jq -s 'length' "$TMP_DIR/th_norm.jsonl")"
th_verified="$(jq -s '[.[] | select(.verified)] | length' "$TMP_DIR/th_norm.jsonl")"
if [ "$trufflehog_status" = "error" ]; then
    echo "    ${RED}❌ scan failed — see stderr below${RST}"
elif [ "$th_candidates" -eq 0 ]; then
    echo "    ${GRN}✅ 0 candidate secrets in ${N_COMMITS} commits${RST}"
elif [ "$th_verified" -gt 0 ]; then
    echo "    ${RED}🚨 ${th_candidates} candidates — ${th_verified} VERIFIED LIVE${RST}"
else
    echo "    ${YLW}⚠️  ${th_candidates} candidates — none verified live${RST}"
fi

# --- Scan 2: Gitleaks (git history) -----------------------------------------

echo ""
echo "${BLD}🔍 [2/3] Gitleaks — git history (.gitleaks.toml rules)${RST}"
echo "    regex rules over every commit — high recall, many intentional placeholders"

set +e
gitleaks detect \
    --source . \
    --config "$REPO_ROOT/.gitleaks.toml" \
    --redact --no-banner \
    --report-format json \
    --report-path "$TMP_DIR/gitleaks_history.json" \
    --exit-code "$FINDINGS_EXIT" \
    >"$TMP_DIR/gitleaks_history.out" 2>"$TMP_DIR/gitleaks_history.err"
gitleaks_history_rc=$?
set -e

gitleaks_history_status="clean"
case "$gitleaks_history_rc" in
    0) gitleaks_history_status="clean" ;;
    "$FINDINGS_EXIT") gitleaks_history_status="findings" ;;
    *) gitleaks_history_status="error" ;;
esac

jq -c --arg repo "$REPO_ROOT" '.[] | {
    tool: "gitleaks", scan: "history",
    rule: (.RuleID // "unknown"),
    verified: false,
    file: ((.File // "?") | ltrimstr($repo + "/")),
    line: (.StartLine // 0),
    commit: (if (.Commit // "") == "" then "?" else .Commit[0:8] end),
    redacted: null,
    tracked: true, ignored: false
}' "$TMP_DIR/gitleaks_history.json" >"$TMP_DIR/glh_norm.jsonl" \
    || gitleaks_history_status="error"

glh_count="$(jq -s 'length' "$TMP_DIR/glh_norm.jsonl")"
if [ "$gitleaks_history_status" = "error" ]; then
    echo "    ${RED}❌ history scan failed${RST}"
elif [ "$glh_count" -eq 0 ]; then
    echo "    ${GRN}✅ no pattern matches in history${RST}"
else
    echo "    ${YLW}⚠️  ${glh_count} pattern matches in commits${RST}"
fi

# --- Scan 3: Gitleaks (worktree, incl. gitignored files) ---------------------

echo ""
echo "${BLD}🔍 [3/3] Gitleaks — current worktree (incl. untracked & gitignored)${RST}"
echo "    also scans gitignored scratch dirs — git tracking status drives the triage below"

set +e
gitleaks dir . \
    --config "$REPO_ROOT/.gitleaks.toml" \
    --redact --no-banner \
    --report-format json \
    --report-path "$TMP_DIR/gitleaks_worktree.json" \
    --exit-code "$FINDINGS_EXIT" \
    >"$TMP_DIR/gitleaks_worktree.out" 2>"$TMP_DIR/gitleaks_worktree.err"
gitleaks_worktree_rc=$?
set -e

gitleaks_worktree_status="clean"
case "$gitleaks_worktree_rc" in
    0) gitleaks_worktree_status="clean" ;;
    "$FINDINGS_EXIT") gitleaks_worktree_status="findings" ;;
    *) gitleaks_worktree_status="error" ;;
esac

jq -c --arg repo "$REPO_ROOT" '.[] | {
    tool: "gitleaks", scan: "worktree",
    rule: (.RuleID // "unknown"),
    verified: false,
    file: ((.File // "?") | ltrimstr($repo + "/")),
    line: (.StartLine // 0),
    commit: "worktree",
    redacted: null,
    tracked: true, ignored: false
}' "$TMP_DIR/gitleaks_worktree.json" >"$TMP_DIR/glw_norm.jsonl" \
    || gitleaks_worktree_status="error"

# Annotate each worktree finding's file with git tracking status:
#   tracked  — content is (or was) committed; the history scan covers it
#   ignored  — gitignored; can never be committed (scratch / local-only)
#   neither  — untracked & unignored; ONE `git add .` away from a commit
TRACK_TSV="$TMP_DIR/tracking.tsv"
: >"$TRACK_TSV"
while IFS= read -r f; do
    [ -z "$f" ] && continue
    if git ls-files --error-unmatch -- "$f" >/dev/null 2>&1; then trk=1; else trk=0; fi
    if git check-ignore -q -- "$f" 2>/dev/null; then ign=1; else ign=0; fi
    printf '%s\t%s\t%s\n' "$f" "$trk" "$ign" >>"$TRACK_TSV"
done < <(jq -r '.file' "$TMP_DIR/glw_norm.jsonl" | sort -u)

glw_count="$(jq -s 'length' "$TMP_DIR/glw_norm.jsonl")"
if [ "$gitleaks_worktree_status" = "error" ]; then
    echo "    ${RED}❌ worktree scan failed${RST}"
elif [ "$glw_count" -eq 0 ]; then
    echo "    ${GRN}✅ nothing in the worktree${RST}"
else
    echo "    ${YLW}⚠️  ${glw_count} matches in current files (deduped against history in the triage)${RST}"
fi

# --- Combine, dedupe, classify ----------------------------------------------

# Worktree matches that are identical (rule+file+line) to a history match are
# duplicates — drop them so nothing is double-counted.
# Bucket matrix (content class × git status):
#   source: tracked|untracked-unignored → HIGH, ignored → MEDIUM
#   test:   tracked|untracked-unignored → MEDIUM, ignored → LOW
#   noise:  tracked|ignored → LOW, untracked-unignored → MEDIUM (hygiene)
jq -cs --rawfile track "$TRACK_TSV" '
    ($track | split("\n")
        | map(select(length > 0) | split("\t")
            | {(.[0]): {tracked: (.[1] == "1"), ignored: (.[2] == "1")}})
        | add // {}) as $tmap
    | (map(select(.tool == "gitleaks" and .scan == "history")
        | .rule + "|" + .file + "|" + (.line | tostring)) | unique) as $hist_keys
    | (map(select(.scan == "worktree")
        | . + ($tmap[.file] // {tracked: true, ignored: false}))) as $wt
    | ((map(select(.scan != "worktree"))) + $wt)
    | map(select(.scan != "worktree"
        or ((.rule + "|" + .file + "|" + (.line | tostring)) as $k
            | ($hist_keys | index($k)) | not)))
    | map(
        (if (.file | test("(?i)(^|/)[^/]*(sample|synthetic|demo|example)[^/]*(/|$)|\\.min\\.js|^assets/|^search/|^docs/|^site/|(^|/)readme|(^|/)\\.venv/|(^|/)node_modules/|(^|/)__pycache__/|(^|/)\\.pytest_cache/"))
         then "noise"
         elif (.file | test("^tests/|^examples/")) then "test"
         else "source" end) as $content
        | (if .verified then "critical"
           elif $content == "source" then
               (if .tracked == false and .ignored then "medium" else "high" end)
           elif $content == "test" then
               (if .ignored then "low" else "medium" end)
           else
               (if .tracked == false and .ignored == false then "medium" else "low" end)
           end) as $bucket
        | . + {content: $content, bucket: $bucket})
    # same rule hitting the same file:line in many commits = one review item
    | unique_by(.rule, .file, .line)
' "$TMP_DIR/th_norm.jsonl" "$TMP_DIR/glh_norm.jsonl" "$TMP_DIR/glw_norm.jsonl" \
    >"$TMP_DIR/classified.json"

critical_n="$(jq '[.[] | select(.bucket == "critical")] | length' "$TMP_DIR/classified.json")"
high_n="$(jq '[.[] | select(.bucket == "high")] | length' "$TMP_DIR/classified.json")"
medium_n="$(jq '[.[] | select(.bucket == "medium")] | length' "$TMP_DIR/classified.json")"
low_n="$(jq '[.[] | select(.bucket == "low")] | length' "$TMP_DIR/classified.json")"
total_n=$((critical_n + high_n + medium_n + low_n))
hyg_n="$(jq '[.[] | select(.tracked == false and .ignored == false)] | length' "$TMP_DIR/classified.json")"

# --- Dashboard ---------------------------------------------------------------

echo ""
echo "${BLD}📊 TRIAGE — ${total_n} match(es) after dedup${RST}  ${DIM}(context + git status heuristics, not a verdict)${RST}"
max_n="$critical_n"
if [ "$high_n" -gt "$max_n" ]; then max_n="$high_n"; fi
if [ "$medium_n" -gt "$max_n" ]; then max_n="$medium_n"; fi
if [ "$low_n" -gt "$max_n" ]; then max_n="$low_n"; fi
printf '   🔴 %-9s %5s  %s  verified live secrets\n' "CRITICAL" "$critical_n" "$(bar "$critical_n" "$max_n" "$RED")"
printf '   🟠 %-9s %5s  %s  tracked active source & config\n' "HIGH" "$high_n" "$(bar "$high_n" "$max_n" "$ORG")"
printf '   🟡 %-9s %5s  %s  tests, examples, gitignored files\n' "MEDIUM" "$medium_n" "$(bar "$medium_n" "$max_n" "$YLW")"
printf '   ⚪ %-9s %5s  %s  sample/docs/generated/gitignored scratch\n' "LOW" "$low_n" "$(bar "$low_n" "$max_n" "$GRY")"

# --- CRITICAL section --------------------------------------------------------

echo ""
echo "${BLD}${RED}🚨 CRITICAL — verified live secrets (${critical_n})${RST}"
if [ "$critical_n" -eq 0 ]; then
    echo "   ${GRN}✅ none — no credential was confirmed working by its provider.${RST}"
else
    jq -r 'sort_by(.file, .line) | .[] | select(.bucket == "critical") |
        "   🔑 \(.file):\(.line) @ \(.commit)  [\(.tool) · \(.rule)]"' \
        "$TMP_DIR/classified.json"
fi

# --- HIGH section ------------------------------------------------------------

echo ""
echo "${BLD}${ORG}🟠 HIGH — secret-shaped matches in active source/config (${high_n})${RST}"
echo "   ${DIM}these sit in code and tracked config — a real secret here ships to every clone.${RST}"
if [ "$high_n" -eq 0 ]; then
    echo "   ${GRN}✅ none — all matches sit in tests, examples or generated/sample content.${RST}"
else
    jq -r --arg dim "$DIM" --arg rst "$RST" '
        [.[] | select(.bucket == "high")] | sort_by(.file, .line) | .[] |
        (if .verified then "" elif .tool == "trufflehog" then " (unverified)" else "" end) as $sfx
        | "   🟠 \(.file):\($dim)\(.line)\($rst) \($dim)@ \(.commit) · \(.tool) · \(.rule)\($sfx)\($rst)"' \
        "$TMP_DIR/classified.json" >"$TMP_DIR/high_rows.txt"
    total_high=$(wc -l <"$TMP_DIR/high_rows.txt")
    sed -n '1,25p' "$TMP_DIR/high_rows.txt"
    if [ "$total_high" -gt 25 ]; then
        echo "   ${DIM}… +$((total_high - 25)) more — rerun with --json to dump everything${RST}"
    fi
    echo "   ${DIM}inspect one:  git show <commit>:<file> | sed -n '<line>p'${RST}"
fi

# --- MEDIUM section ----------------------------------------------------------

echo ""
echo "${BLD}${YLW}🟡 MEDIUM — tests, examples, gitignored files (${medium_n})${RST}"
echo "   ${DIM}can't ship from here directly; skim for anything that looks real.${RST}"
if [ "$medium_n" -eq 0 ]; then
    echo "   ${GRN}✅ none.${RST}"
else
    jq -c '.[] | select(.bucket == "medium")' "$TMP_DIR/classified.json" >"$TMP_DIR/medium.json"
    echo "   ${DIM}by rule:${RST}"
    while IFS=$'\t' read -r n rule nfiles examples; do
        printf '     🟡 %-36s × %-4s %s file(s), e.g. %s\n' "$rule" "$n" "$nfiles" "$examples"
        printf '         %s(%s)%s\n' "$DIM" "$(rule_desc "$rule")" "$RST"
    done < <(jq -sr 'group_by(.rule)[] |
            [(length),
             .[0].rule,
             ([.[].file] | unique | length),
             ([.[].file | split("/")[-2:] | join("/")] | unique | .[0:2] | join(", "))
            ] | @tsv' "$TMP_DIR/medium.json" | sort -rn)
    # Repo hygiene: untracked & unignored files with matches.
    if [ "$hyg_n" -gt 0 ]; then
        echo ""
        echo "   ${YLW}📌 repo hygiene — matches in files that are NOT gitignored:${RST}"
        echo "   ${DIM}these are one \`git add .\` away from entering history:${RST}"
        jq -r '[.[] | select(.tracked == false and .ignored == false)
                | .file | split("/")[0]] | group_by(.) | map({d: .[0], n: length})
                | sort_by(-.n) | .[] | "     \(.d) — \(.n) match(es)"' \
            "$TMP_DIR/classified.json"
        echo "   ${DIM}→ add them to .gitignore, or commit them intentionally.${RST}"
    fi
fi

# --- LOW section -------------------------------------------------------------

echo ""
echo "${BLD}${GRY}⚪ LOW — sample data, generated artifacts, gitignored scratch (${low_n})${RST}"
echo "   ${DIM}interpretation: keyword-shaped text in content that never ships and/or${RST}"
echo "   ${DIM}never enters git — the scanner matched the pattern, not a real credential.${RST}"
if [ "$low_n" -eq 0 ]; then
    echo "   ${GRN}✅ none.${RST}"
else
    echo "   ${DIM}by location (top-level entries):${RST}"
    jq -r '[.[] | select(.bucket == "low")] | sort_by(.file)
        | group_by(.file | split("/")[0])[]
        | (.[0].file | split("/")[0]) as $d
        | {d: $d, n: length, ign: ([.[] | select(.ignored)] | length)}
        | "     ⚪ \(.d) — \(.n) match(es)\(if .ign == .n then "  (gitignored — never enters git)" else "" end)"' \
        "$TMP_DIR/classified.json"
    echo ""
    echo "   ${DIM}paste-ready allowlist for .gitleaks.toml [allowlist] paths${RST}"
    echo "   ${DIM}(dirs that also hold HIGH/MEDIUM findings are narrowed to their noise subpaths):${RST}"
    jq -r --arg t "'''" '
        [.[] | select(.bucket == "low")] as $low
        | ([.[] | select(.bucket == "high" or .bucket == "medium")
            | .file | split("/")[0]] | unique) as $risky_dirs
        | ($risky_dirs | map({(.): true}) | add // {}) as $rmap
        | ([$low[].file | split("/")[0]] | unique) as $low_dirs
        | ([($low_dirs - $risky_dirs)[] as $d
            | {s: (if any($low[]; .file == $d)
                   then "^" + ($d | gsub("\\."; "[.]")) + "$"
                   else "^" + ($d | gsub("\\."; "[.]")) + "/" end),
               n: ([$low[] | select((.file | split("/")[0]) == $d)] | length)}]
          + [([$low[] | select($rmap[.file | split("/")[0]] == true)
                | .file | split("/")[0:2] | join("/")] | unique)[] as $p
              | {s: ("^" + ($p | gsub("\\."; "[.]"))),
                 n: ([$low[] | select(.file | startswith($p + "/"))] | length)}])
        | sort_by(-.n) | .[0:8][]
        | "     \($t)\(.s)\($t)"' \
        "$TMP_DIR/classified.json"
fi

# --- Action plan -------------------------------------------------------------

echo ""
echo "${BLD}🛠️  WHAT TO DO NEXT${RST}"
STEP=0
step() { STEP=$((STEP + 1)); printf '   %s%d.%s %s\n' "${BLD}" "$STEP" "${RST}" "$1"; }

if [ "$critical_n" -gt 0 ]; then
    step "${RED}Rotate & revoke the CRITICAL credentials at their provider NOW; purge them from history (git filter-repo / BFG) and audit provider logs.${RST}"
fi
if [ "$high_n" -gt 0 ]; then
    step "Review each 🟠 HIGH match: real secret → rotate + remove (load from env/config instead); intentional placeholder → allowlist its fingerprint in .gitleaks.toml."
fi
if [ "$hyg_n" -gt 0 ]; then
    step "Fix repo hygiene: gitignore (or deliberately commit) the untracked entries listed under 🟡."
fi
if [ "$low_n" -gt 0 ]; then
    step "Silence the ⚪ noise for good: add the suggested path prefixes to the [allowlist] in .gitleaks.toml, then iterate fast with: gitleaks dir . -c .gitleaks.toml --redact"
fi
if [ "$total_n" -eq 0 ]; then
    step "Nothing to do — consider running this before every push or in CI (exit 0 = clean)."
fi
step "Re-run ./scan.sh after fixes — the goal is exit code 0."
echo "   ${DIM}exit codes: 0 clean · 1 findings · 2 scan failure${RST}"

# --- Optional JSON dump ------------------------------------------------------

if [ -n "$JSON_OUT" ]; then
    mkdir -p "$(dirname "$JSON_OUT")"
    cp "$TMP_DIR/classified.json" "$JSON_OUT"
    chmod 600 "$JSON_OUT"
    echo ""
    echo "    💾 classified findings (secrets redacted) saved to: ${JSON_OUT}"
fi

# --- Verdict -----------------------------------------------------------------

echo ""
if [ "$trufflehog_status" = "error" ] || [ "$gitleaks_history_status" = "error" ] \
    || [ "$gitleaks_worktree_status" = "error" ]; then
    echo "${RED}❌ RESULT: SCAN FAILURE — do not treat this as a clean scan.${RST}" >&2
    exit 2
fi

if [ "$total_n" -gt 0 ]; then
    echo "${ORG}🚨 RESULT: FINDINGS DETECTED — see the TRIAGE table for what needs eyes.${RST}"
    exit 1
fi

echo "${GRN}✅ RESULT: CLEAN — no secrets found.${RST}"
exit 0
