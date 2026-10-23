# graphregistry/scripts/slide_text_redundancy_filter.py
"""Standalone experiment: redundant-slide filtering before concept extraction.

Reads an OCR dump with one slide frame per line:

    # lecture_id, slide_id, ocr_text_en, text_length
    '0_ki60hwas', '0_ki60hwas-0000', '+41216935488@epfl....', '21'

Fields are single-quoted and separated by "', '". Inside the OCR payload a
literal "\\n" encodes a newline and quotes are backslash-escaped
(d\\'installer). The declared text_length refers to the unescaped text and
is used to sanity-check the parser.

The script implements the V1 / V1.5 strategy discussed for compressing
lecture OCR before it reaches keyword/concept extraction:

  V1   normalize every line (NFKC + casefold + whitespace removal) into a
       matching key, drop junk lines below an information floor, then
       compare each slide against the last KEPT slide (not the previous
       frame) with exact-then-fuzzy line matching. A slide is kept when
       it introduces enough new, non-chrome content.
  V1.5 document-frequency chrome suppression: lines occurring in a large
       fraction of the lecture's slides are persistent UI ("chrome");
       they can neither justify keeping a frame nor clutter the projection.

The diff is descriptive, never generative: OCR text is only *selected*,
never rewritten, and raw OCR stays canonical on its slide. Dropped frames
are attached to the kept frame they were redundant with, so slide ->
timestamp attribution survives through redundancy groups. When a frame no
longer shows most of the anchor's content, the group is closed instead of
extended, so attribution never claims content was visible after it
disappeared.

A TOTAL block at the end aggregates across lectures: characters at every
stage, the overall compression ratio, and the estimated token count of the
projection text at a configurable chars-per-token rate. It also prints an
explicit FITS / DOES NOT FIT verdict against the budget left after the
output reserve and prompt overhead. The verdict assumes the GLM-5.3-Flash
context window (1M tokens per the Z.ai API documentation) unless
--context-tokens says otherwise.

Usage:
    python scripts/slide_text_redundancy_filter.py ocr_dump.txt
    python scripts/slide_text_redundancy_filter.py ocr_dump.txt --index-stats
    python scripts/slide_text_redundancy_filter.py ocr_dump.txt --json report.json
    python scripts/slide_text_redundancy_filter.py ocr_dump.txt --projection projection.txt --strip-chrome
    python scripts/slide_text_redundancy_filter.py ocr_dump.txt --projection proj.txt --strip-chrome --drop-revisits --context-tokens 131072
"""
from __future__ import annotations
import argparse
import json
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path

# RapidFuzz offers the same ratio semantics as difflib with a much faster
# C implementation; the script stays runnable on the stdlib without it.
try:
    from rapidfuzz import fuzz as _rapidfuzz
except ImportError:
    _rapidfuzz = None

# Precompiled patterns: whitespace stripping for matching keys and the row
# shape "<lecture>', '<slide>', '<text>', '<length>'" for dump parsing.
_WS_RE  = re.compile(r"\s+")
_ROW_RE = re.compile(r"^'(.+?)', '(.+?)', '(.+)', '(\d+)'$")

# Default context window: GLM-5.3-Flash serves a 1M-token context per the
# Z.ai API documentation (docs.z.ai/guides/llm/glm-5.3-flash). The fit
# verdict assumes this window; override with --context-tokens when your
# endpoint (for example an internal deployment) serves a different limit.
DEFAULT_CONTEXT_TOKENS = 1_000_000

# One parsed dump row: the canonical unescaped OCR text of a slide frame.
@dataclass

#==================#
# Class Definition #
#==================#
class SlideRecord:
    """One parsed dump row: canonical (unescaped) OCR text of a slide frame."""
    #--------------------#
    # Internal variables #
    #--------------------#
    lecture_id      : str
    slide_id        : str
    ocr_text        : str
    declared_length : int | None = None

# One non-empty OCR line with its normalized matching key and junk flag.
@dataclass

#==================#
# Class Definition #
#==================#
class LineInfo:
    """One non-empty OCR line with its normalized matching key and junk flag."""
    #--------------------#
    # Internal variables #
    #--------------------#
    raw     : str
    key     : str
    is_junk : bool

# Outcome of the keep/drop decision for one slide frame.
@dataclass

#==================#
# Class Definition #
#==================#
class SlideAnalysis:
    """Outcome of the keep/drop decision for one slide frame."""
    #--------------------#
    # Internal variables #
    #--------------------#
    record        : SlideRecord
    lines         : list[LineInfo]
    keys          : list[str] = field(default_factory=list)
    decision      : str       = ""
    reason        : str       = ""
    anchor_slide  : str | None = None
    group_index   : int | None = None
    new_raws      : list[str] = field(default_factory=list)
    new_chars     : int        = 0
    content_chars : int        = 0
    new_ratio     : float      = 0.0
    coverage      : float | None = None
    revisit_share : float      = 0.0
    revisit_of    : str | None = None

# One representative frame plus the later frames it makes redundant.
@dataclass

#==================#
# Class Definition #
#==================#
class RedundancyGroup:
    """One representative frame plus the later frames it makes redundant."""
    #--------------------#
    # Internal variables #
    #--------------------#
    index   : int
    rep     : SlideAnalysis
    members : list[SlideRecord] = field(default_factory=list)

# Internal Function: Chronological order key for slide records: lectures
# grouped together, numeric slide suffix ascending inside each lecture.
def _slide_sort_key(rec: SlideRecord) -> tuple:
    m = re.search(r"(\d+)$", rec.slide_id)
    suffix = int(m.group(1)) if m else -1
    return (rec.lecture_id, suffix, rec.slide_id)

# Function: Convert a raw OCR line into its matching key: Unicode NFKC
# normalization, case folding, and removal of all whitespace. Normalization
# is used to DISCOVER identity only; the raw line is kept for display and
# for the LLM projection, which is why math fragments and code survive.
def normalize_line(line: str) -> str:
    line = unicodedata.normalize("NFKC", line)
    line = line.casefold()
    return _WS_RE.sub("", line)

# Function: Decide whether a normalized line carries enough information to
# participate in redundancy decisions. Very short lines ("9", "V V") and
# lines without letters ("2023", "...") are OCR noise that would otherwise
# inflate both match counts and "new content" counts.
def is_junk_key(key: str, min_line_chars: int) -> bool:
    if len(key) < min_line_chars:
        return True
    return not any(unicodedata.category(ch).startswith("L") for ch in key)

# Function: Normalized string similarity in [0, 1] between two matching
# keys. RapidFuzz's ratio and difflib's SequenceMatcher ratio both count
# matching characters, so results are comparable; RapidFuzz is far faster.
def similarity(a: str, b: str) -> float:
    if _rapidfuzz is not None:
        return _rapidfuzz.ratio(a, b) / 100.0
    return SequenceMatcher(None, a, b).ratio()

# Function: Match the candidate frame's lines against the anchor frame's
# lines and return (matched_candidates, matched_anchors) as index sets.
# Matching is a two-tier cascade: exact equality on the normalized key
# first (most OCR noise is spacing/punctuation), then fuzzy similarity
# above the threshold, longest candidate lines first so short lines cannot
# steal the match of an informative one. Each anchor line is consumed at
# most once, so repeated boilerplate lines match in bulk.
def match_frames(
    anchor_keys    : list[str],
    candidate_keys : list[str],
    threshold      : float,
) -> tuple[set[int], set[int]]:
    matched_cand: set[int] = set()
    matched_anch: set[int] = set()
    anchor_spots: dict[str, list[int]] = {}
    for j, ak in enumerate(anchor_keys):
        anchor_spots.setdefault(ak, []).append(j)
    # Tier 1: exact equality on the normalized key.
    for i, ck in enumerate(candidate_keys):
        spots = anchor_spots.get(ck)
        if spots:
            matched_cand.add(i)
            matched_anch.add(spots.pop())
    # Tier 2: fuzzy similarity on leftovers, longest candidates first.
    leftovers = [i for i in range(len(candidate_keys)) if i not in matched_cand]
    leftovers.sort(key=lambda i: -len(candidate_keys[i]))
    for i in leftovers:
        ck = candidate_keys[i]
        best_j, best_score = -1, 0.0
        for j, ak in enumerate(anchor_keys):
            if j in matched_anch:
                continue
            # A length upper bound on the ratio skips hopeless pairs
            # without running the expensive similarity function.
            bound = 2.0 * min(len(ak), len(ck)) / (len(ak) + len(ck))
            if bound < threshold:
                continue
            score = similarity(ak, ck)
            if score > best_score:
                best_j, best_score = j, score
        if best_j >= 0 and best_score >= threshold:
            matched_cand.add(i)
            matched_anch.add(best_j)
    return matched_cand, matched_anch

# Internal Function: Find the earlier kept frame that already shows most of
# the candidate's "new" lines (exact-key overlap); used to label scroll
# revisits, where the lecturer returns to earlier content later on.
def _best_revisit_target(new_key_list: list[str], kept: list[SlideAnalysis]) -> SlideAnalysis | None:
    best: SlideAnalysis | None = None
    best_count = 0
    for prior in kept:
        prior_keys = set(prior.keys)
        count = sum(1 for k in new_key_list if k in prior_keys)
        if count > best_count:
            best, best_count = prior, count
    return best

# Function: Unescape the OCR payload: literal "\\n" becomes a newline,
# "\\t" a tab, "\\\\" a single backslash, and \\" / \\' their bare quotes.
# Unknown escapes keep their backslash. The text is canonical evidence and
# is never rewritten beyond this decoding step.
def unescape_text(text: str) -> str:
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "\\" and i + 1 < n:
            nxt = text[i + 1]
            if nxt == "n":
                out.append("\n")
                i += 2
                continue
            if nxt == "t":
                out.append("\t")
                i += 2
                continue
            if nxt == "\\":
                out.append("\\")
                i += 2
                continue
            if nxt in ("'", '"'):
                out.append(nxt)
                i += 2
                continue
        out.append(ch)
        i += 1
    return "".join(out)

# Function: Parse one data row of the dump. The OCR payload may contain
# quotes and "', '" look-alikes, so the row regex anchors on the final
# "', '<digits>'" boundary and lets the greedy payload absorb the rest.
def parse_row(line: str) -> tuple[str, str, str, int]:
    m = _ROW_RE.match(line)
    if not m:
        raise ValueError("row does not match \"'<lecture>', '<slide>', '<text>', '<length>'\"")
    lecture_id, slide_id, raw_text, declared = m.groups()
    return lecture_id, slide_id, unescape_text(raw_text), int(declared)

# Function: Read the whole dump: skip comment/blank lines, parse every data
# row, warn on repeated slide ids (keeping the first), and order slides
# chronologically within each lecture. A malformed row aborts with its
# location, because a silently misparsed payload would corrupt everything
# downstream.
def parse_dump(path: str | Path) -> list[SlideRecord]:
    path = Path(path)
    records: list[SlideRecord] = []
    seen: set[tuple[str, str]] = set()
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.rstrip("\r\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            try:
                lecture_id, slide_id, ocr_text, declared = parse_row(line)
            except ValueError as exc:
                raise SystemExit(f"{path}:{lineno}: {exc}")
            if (lecture_id, slide_id) in seen:
                print(f"WARNING {path}:{lineno}: duplicate slide_id '{slide_id}' in '{lecture_id}'; keeping first occurrence")
                continue
            seen.add((lecture_id, slide_id))
            records.append(
                SlideRecord(
                    lecture_id      = lecture_id,
                    slide_id        = slide_id,
                    ocr_text        = ocr_text,
                    declared_length = declared,
                )
            )
    records.sort(key=_slide_sort_key)
    return records

# Internal Function: Build the LLM projection for one representative: the
# raw OCR text, optionally with chrome lines removed. This is the only
# lossy step of the pipeline and it never touches the canonical record.
def _projection_text(analysis: SlideAnalysis, chrome: set[str], strip_chrome: bool) -> str:
    if not strip_chrome:
        return analysis.record.ocr_text
    kept_lines = []
    for raw in analysis.record.ocr_text.split("\n"):
        key = normalize_line(raw)
        if key and key in chrome:
            continue
        kept_lines.append(raw)
    return "\n".join(kept_lines)

# Function: Run the V1 / V1.5 strategy over one lecture's frames in
# chronological order. Returns a report dict with per-slide analyses,
# redundancy groups, the chrome index, and compression figures.
def analyze_lecture(records: list[SlideRecord], params: dict) -> dict:
    # Build per-line info up front; empty/whitespace-only lines carry no
    # information and are skipped entirely.
    slide_lines: list[list[LineInfo]] = []
    key_samples: dict[str, str] = {}
    for rec in records:
        infos: list[LineInfo] = []
        for raw in rec.ocr_text.split("\n"):
            key = normalize_line(raw)
            if not key:
                continue
            infos.append(LineInfo(raw=raw, key=key, is_junk=is_junk_key(key, params["min_line_chars"])))
            if key not in key_samples:
                key_samples[key] = raw
        slide_lines.append(infos)
    # Document-frequency chrome suppression (V1.5): a non-junk key present
    # in a large fraction of frames is persistent UI; it can neither
    # justify keeping a frame nor count as new content.
    df: Counter = Counter()
    for infos in slide_lines:
        df.update({info.key for info in infos if not info.is_junk})
    # An absolute floor keeps short lectures sane: in a 1-2 frame lecture
    # every line trivially persists across all frames, which would flag
    # genuine content as chrome.
    chrome = {
        key for key, count in df.items()
        if count >= params["chrome_min_frames"] and count / len(records) >= params["chrome_ratio"]
    }
    # Sequential pass: every frame is compared against the last KEPT frame
    # (the anchor), never the immediately previous one, so gradual drift
    # cannot slip through and anchor errors do not propagate.
    analyses: list[SlideAnalysis] = []
    groups: list[RedundancyGroup] = []
    kept: list[SlideAnalysis] = []
    seen_keys: set[str] = set()
    anchor: SlideAnalysis | None = None
    for rec, infos in zip(records, slide_lines):
        keys = [info.key for info in infos if not info.is_junk]
        raws = [info.raw for info in infos if not info.is_junk]
        analysis = SlideAnalysis(record=rec, lines=infos, keys=keys)
        content_pairs = [(k, r) for k, r in zip(keys, raws) if k not in chrome]
        content_chars = sum(len(k) for k, _ in content_pairs)
        if anchor is None:
            # First frame of a group sequence: all of its non-chrome
            # content is new by definition.
            new_pairs = list(content_pairs)
            new_chars = content_chars
            new_ratio = 1.0 if content_chars else 0.0
            coverage = None
        else:
            matched_cand, matched_anch = match_frames(anchor.keys, keys, params["fuzzy_threshold"])
            # New content: lines that matched nothing above the threshold
            # and are neither chrome nor junk.
            new_pairs = [
                (k, r) for i, (k, r) in enumerate(zip(keys, raws))
                if i not in matched_cand and k not in chrome
            ]
            new_chars = sum(len(k) for k, _ in new_pairs)
            new_ratio = (new_chars / content_chars) if content_chars else 0.0
            # Coverage: how much of the anchor's non-chrome content is
            # still visible here. Low coverage means the anchor's content
            # disappeared and the group must close rather than extend.
            anchor_content = [k for k in anchor.keys if k not in chrome]
            anchor_content_chars = sum(len(k) for k in anchor_content)
            matched_anchor_chars = sum(
                len(anchor.keys[j]) for j in matched_anch if anchor.keys[j] not in chrome
            )
            coverage = (matched_anchor_chars / anchor_content_chars) if anchor_content_chars else 1.0
        # Revisit diagnostics: how much of the "new" content was already
        # seen in earlier kept frames (scroll-backs and repeated visits).
        revisit_share, revisit_of = 0.0, None
        if anchor is not None and new_pairs:
            new_key_list = [k for k, _ in new_pairs]
            revisit_chars = sum(len(k) for k in new_key_list if k in seen_keys)
            revisit_share = revisit_chars / new_chars
            if revisit_share >= params["revisit_ratio"]:
                target = _best_revisit_target(new_key_list, kept)
                if target is not None:
                    revisit_of = target.record.slide_id
        # Decision: drop only when the frame adds little new content AND
        # still shows most of the anchor's content; otherwise it either
        # starts a new group or closes the current one.
        analysis.anchor_slide  = anchor.record.slide_id if anchor is not None else None
        analysis.new_raws      = [r for _, r in new_pairs]
        analysis.new_chars     = new_chars
        analysis.content_chars = content_chars
        analysis.new_ratio     = new_ratio
        analysis.coverage      = coverage
        analysis.revisit_share = revisit_share
        analysis.revisit_of    = revisit_of
        if anchor is None:
            if new_chars == 0:
                analysis.decision, analysis.reason = "DROP", "no-content"
            else:
                analysis.decision, analysis.reason = "KEEP", "first-of-group"
        elif new_chars >= params["min_new_chars"] or new_ratio >= params["min_new_ratio"]:
            if revisit_of is not None and params["drop_revisits"]:
                analysis.decision, analysis.reason = "DROP", "revisit"
            else:
                analysis.decision, analysis.reason = "KEEP", ("revisit" if revisit_of else "new-content")
        elif coverage is not None and coverage < params["min_coverage"]:
            analysis.decision, analysis.reason = "DROP", "content-cleared"
        else:
            analysis.decision, analysis.reason = "DROP", "redundant"
        # Every frame gets an analysis entry, whatever the decision, so
        # the report can show the full chronological picture.
        analyses.append(analysis)
        # Routing: KEEP opens a group; revisit-drops return to the group
        # of the frame they revisit; content-cleared drops close the span;
        # plain redundant drops extend the anchor's group.
        if analysis.decision == "KEEP":
            group = RedundancyGroup(index=len(groups), rep=analysis, members=[rec])
            analysis.group_index = group.index
            groups.append(group)
            kept.append(analysis)
            seen_keys.update(keys)
            anchor = analysis
        elif analysis.reason == "revisit" and revisit_of is not None:
            target_analysis = next((p for p in kept if p.record.slide_id == revisit_of), None)
            if target_analysis is not None and target_analysis.group_index is not None:
                groups[target_analysis.group_index].members.append(rec)
        elif analysis.reason == "content-cleared":
            anchor = None
        elif anchor is not None:
            groups[anchor.group_index].members.append(rec)
    ocr_chars = sum(len(r.ocr_text) for r in records)
    rep_chars = sum(len(g.rep.record.ocr_text) for g in groups)
    stripped_chars = sum(len(_projection_text(g.rep, chrome, True)) for g in groups)
    length_mismatches = [
        (r.slide_id, r.declared_length, len(r.ocr_text))
        for r in records
        if r.declared_length is not None and r.declared_length != len(r.ocr_text)
    ]
    return {
        "lecture_id"       : records[0].lecture_id,
        "n_frames"         : len(records),
        "analyses"         : analyses,
        "groups"           : groups,
        "chrome"           : chrome,
        "df"               : df,
        "key_samples"      : key_samples,
        "length_mismatches": length_mismatches,
        "ocr_chars"        : ocr_chars,
        "rep_chars"        : rep_chars,
        "stripped_chars"   : stripped_chars,
    }

# Function: Print the human-readable experiment report for one lecture:
# the document-frequency chrome ranking, the frame-by-frame decisions, the
# redundancy groups, and the compression summary.
def print_report(report: dict, params: dict, verbose: bool = False) -> None:
    print("=" * 78)
    print(f"Lecture '{report['lecture_id']}' - {report['n_frames']} slides, {report['ocr_chars']:,} OCR chars")
    print("=" * 78)
    print()
    # The document-frequency ranking is the TF-IDF-like view: lines that
    # persist across the lecture are UI chrome, not lecture content.
    n_frames = report["n_frames"]
    print("Document-frequency ranking (most persistent lines across the lecture):")
    top = [(key, count) for key, count in report["df"].most_common() if count >= min(3, n_frames)][:12]
    for key, count in top:
        sample = report["key_samples"].get(key, key)
        if len(sample) > 64:
            sample = sample[:61] + "..."
        flag = "  [chrome]" if key in report["chrome"] else ""
        print(f"  {count:>3}/{n_frames}  {count / n_frames:5.0%}  '{sample}'{flag}")
    print(
        f"  (chrome threshold {params['chrome_ratio']:.0%}: "
        f"{len(report['chrome'])} line(s) flagged as chrome)"
    )
    print()
    print("Frame-by-frame decisions (each slide compared against the last KEPT slide):")
    print(
        f"  {'slide':<18}{'decision':<9}{'reason':<42}{'new content':<22}{'cov':>5}{'chars':>8}"
    )
    for a in report["analyses"]:
        reason = a.reason
        if a.reason == "revisit" and a.revisit_of:
            reason = f"revisit {a.revisit_share:.0%} of #{a.revisit_of}"
        new_cell = f"{a.new_chars:>5} ch / {len(a.new_raws):>2} lines"
        cov_cell = f"{a.coverage:>5.0%}" if a.coverage is not None else f"{'-':>5}"
        print(
            f"  #{a.record.slide_id:<17}{a.decision:<8}{reason:<42}{new_cell:<22}{cov_cell}"
            f"{len(a.record.ocr_text):>8}"
        )
        # Verbose mode shows the actual new lines that justified a KEEP so
        # the thresholds can be tuned against real content.
        if verbose and a.decision == "KEEP" and a.new_raws:
            for raw in a.new_raws[:8]:
                print(f"      + {raw[:96]}")
            if len(a.new_raws) > 8:
                print(f"      + ... {len(a.new_raws) - 8} more new line(s)")
    print()
    print("Redundancy groups (representative frame + frames it makes redundant):")
    for g in report["groups"]:
        span_ids = [m.slide_id for m in g.members]
        span = span_ids[0] if len(span_ids) == 1 else f"{span_ids[0]}..{span_ids[-1]}"
        dropped = span_ids[1:]
        extra = f"  [dropped: {' '.join(f'#{s}' for s in dropped)}]" if dropped else ""
        print(
            f"  G{g.index + 1}  rep #{g.rep.record.slide_id}  span {span}  "
            f"{len(span_ids)} frame(s)  {len(g.rep.record.ocr_text):,} ch{extra}"
        )
    print()
    print("Summary:")
    kept_n = sum(1 for a in report["analyses"] if a.decision == "KEEP")
    dropped_n = report["n_frames"] - kept_n
    print(f"  slides: {report['n_frames']} -> {kept_n} kept (representatives), {dropped_n} dropped")
    ocr_chars, rep_chars, stripped = report["ocr_chars"], report["rep_chars"], report["stripped_chars"]
    if ocr_chars:
        print(
            f"  OCR chars: {ocr_chars:,} -> {rep_chars:,} as representatives "
            f"({rep_chars / ocr_chars:.0%} of original)"
        )
        print(
            f"  chrome-stripped projection: {stripped:,} chars "
            f"({stripped / ocr_chars:.0%} of original)"
        )
    if report["length_mismatches"]:
        print()
        print("Parser warnings (declared text_length != computed unescaped length):")
        for slide_id, declared, computed in report["length_mismatches"]:
            print(f"  #{slide_id}: declared {declared}, computed {computed}")

# Internal Function: Convert one lecture's report into JSON-serializable
# dicts, including the raw new lines of every kept frame.
def _lecture_to_json(report: dict) -> dict:
    n_frames = report["n_frames"]
    return {
        "lecture_id"     : report["lecture_id"],
        "ocr_chars"      : report["ocr_chars"],
        "rep_chars"      : report["rep_chars"],
        "stripped_chars" : report["stripped_chars"],
        "chrome"         : [
            {
                "key"    : key,
                "df"     : report["df"][key],
                "ratio"  : round(report["df"][key] / n_frames, 3),
                "sample" : report["key_samples"].get(key, ""),
            }
            for key in sorted(report["chrome"], key=lambda k: -report["df"][k])
        ],
        "length_mismatches": [
            {"slide_id": s, "declared": d, "computed": c}
            for s, d, c in report["length_mismatches"]
        ],
        "slides"     : [
            {
                "slide_id"       : a.record.slide_id,
                "decision"       : a.decision,
                "reason"         : a.reason,
                "anchor"         : a.anchor_slide,
                "group"          : a.group_index,
                "declared_length": a.record.declared_length,
                "computed_length": len(a.record.ocr_text),
                "n_lines"        : len(a.lines),
                "n_junk_lines"   : len(a.lines) - len(a.keys),
                "new_chars"      : a.new_chars,
                "new_lines"      : len(a.new_raws),
                "new_ratio"      : round(a.new_ratio, 3),
                "coverage"       : None if a.coverage is None else round(a.coverage, 3),
                "revisit_share"  : round(a.revisit_share, 3),
                "revisit_of"     : a.revisit_of,
                "new_raw"        : a.new_raws,
            }
            for a in report["analyses"]
        ],
        "groups"     : [
            {
                "index"    : g.index,
                "rep"      : g.rep.record.slide_id,
                "members"  : [m.slide_id for m in g.members],
                "rep_chars": len(g.rep.record.ocr_text),
            }
            for g in report["groups"]
        ],
    }

# Internal Function: Build the LLM projection lines for one lecture: one
# block per redundancy group with the representative's raw OCR and the
# frame span it stands for.
def _build_projection_lines(report: dict, params: dict, strip_chrome: bool) -> list[str]:
    lines = [
        f"# LLM projection for lecture '{report['lecture_id']}'",
        f"# {len(report['groups'])} representative frame(s) out of {report['n_frames']}",
        (
            f"# params: chrome_ratio={params['chrome_ratio']} "
            f"min_new_chars={params['min_new_chars']} min_new_ratio={params['min_new_ratio']} "
            f"min_coverage={params['min_coverage']} fuzzy_threshold={params['fuzzy_threshold']}"
        ),
        "",
    ]
    for g in report["groups"]:
        span_ids = [m.slide_id for m in g.members]
        span = span_ids[0] if len(span_ids) == 1 else f"{span_ids[0]}..{span_ids[-1]}"
        lines.append(
            f"=== slides {span} | rep {g.rep.record.slide_id} | {len(span_ids)} frame(s) ==="
        )
        lines.append(_projection_text(g.rep, report["chrome"], strip_chrome))
        lines.append("")
    return lines

# Function: Aggregate the per-lecture reports into totals: characters at
# every stage, the overall compression ratio, the estimated token count,
# and - when a context window is given - the FITS verdict against the
# budget left after output reserve and prompt overhead.
def _aggregate_totals(
    reports              : list[dict],
    per_lecture_projected: dict[str, int],
    args                 : argparse.Namespace,
) -> dict:
    total_ocr      = sum(r["ocr_chars"] for r in reports)
    total_rep      = sum(r["rep_chars"] for r in reports)
    total_stripped = sum(r["stripped_chars"] for r in reports)
    projected_chars = sum(per_lecture_projected.values())
    est_tokens = projected_chars / args.chars_per_token
    totals = {
        "n_lectures"       : len(reports),
        "n_frames"         : sum(r["n_frames"] for r in reports),
        "n_groups"         : sum(len(r["groups"]) for r in reports),
        "ocr_chars"        : total_ocr,
        "rep_chars"        : total_rep,
        "stripped_chars"   : total_stripped,
        "projected_chars"  : projected_chars,
        "compression_ratio": round(projected_chars / total_ocr, 3) if total_ocr else None,
        "estimated_tokens" : round(est_tokens),
        "chars_per_token"  : args.chars_per_token,
    }
    if args.context_tokens is not None:
        budget = args.context_tokens - args.output_reserve - args.prompt_overhead
        totals["context_tokens"] = args.context_tokens
        totals["context_budget"] = budget
        totals["fits"]           = budget > 0 and est_tokens <= budget
        totals["budget_share"]   = round(est_tokens / budget, 3) if budget > 0 else None
        largest_id, largest_chars = max(per_lecture_projected.items(), key=lambda kv: kv[1])
        totals["largest_lecture_id"]     = largest_id
        totals["largest_lecture_tokens"] = round(largest_chars / args.chars_per_token)
        totals["context_tokens_defaulted"] = getattr(args, "context_tokens_defaulted", False)
    return totals

# Function: Print the TOTAL block across lectures: characters at every
# stage, the compression ratio, the estimated token count, and - when a
# context window is given - the explicit FITS / DOES NOT FIT verdict plus
# the chunked-calls alternative when the total does not fit.
def print_total(totals: dict, args: argparse.Namespace) -> None:
    print("=" * 78)
    print(
        f"TOTAL across {totals['n_lectures']} lecture(s) - "
        f"{totals['n_frames']} slides, {totals['n_groups']} representative frame(s)"
    )
    print("=" * 78)
    ratio = f" ({totals['compression_ratio']:.0%} of original)" if totals["compression_ratio"] is not None else ""
    print(f"  OCR chars in:               {totals['ocr_chars']:>14,}")
    print(f"  representatives (raw):      {totals['rep_chars']:>14,}")
    print(f"  selected (chrome-stripped): {totals['stripped_chars']:>14,}")
    print(f"  projection text (sent):     {totals['projected_chars']:>14,}{ratio}")
    est_cell = f"~{totals['estimated_tokens']:,}"
    print(
        f"  estimated tokens:           {est_cell:>14}  "
        f"(at {totals['chars_per_token']:.2f} ch/tok)"
    )
    if "fits" not in totals:
        return
    verdict = "FITS" if totals["fits"] else "DOES NOT FIT"
    budget_tail = f" - {args.prompt_overhead:,} prompt overhead" if args.prompt_overhead else ""
    print(
        f"  context budget:             {totals['context_budget']:>14,}  "
        f"({totals['context_tokens']:,} - {args.output_reserve:,} output reserve{budget_tail})"
    )
    if totals["budget_share"] is not None:
        print(f"  verdict:                    {verdict:<12} (~{totals['budget_share']:.0%} of budget)")
    else:
        print(f"  verdict:                    {verdict:<12} (budget exhausted by reserves)")
    if args.context_tokens_defaulted:
        print(
            "  (context default: GLM-5.3-Flash serves 1M tokens per the Z.ai API docs; "
            "override with --context-tokens)"
        )
    # Only meaningful across several lectures: with one lecture the total
    # already IS the largest single call.
    if not totals["fits"] and totals["n_lectures"] > 1:
        if totals["largest_lecture_tokens"] <= totals["context_budget"]:
            print(
                f"  chunked alternative:        split calls per lecture - largest single "
                f"lecture '{totals['largest_lecture_id']}' fits "
                f"(~{totals['largest_lecture_tokens']:,} tokens)"
            )
        else:
            print(
                "  chunked alternative:        even the largest single lecture does not fit - "
                "chunk within lectures"
            )
    print()

# Function: Report the global line-index statistics per lecture and in
# total: line occurrences versus the unique-line union, the decomposition
# into one-off and repeated content, the top repeated lines, and the
# token estimate of the union-sized index projection - with a fit verdict
# when a context window is given. This measures the V2 compression
# ceiling without running the expensive per-frame analysis.
def print_index_stats(by_lecture: dict[str, list[SlideRecord]], args: argparse.Namespace) -> None:
    total_unique_chars, total_occurrence_chars = 0, 0
    for lecture_id, records in by_lecture.items():
        # Document frequency per lecture: a line seen in two different
        # lectures is coincidence, not repetition to collapse.
        occurrences: Counter = Counter()
        for rec in records:
            keys = {normalize_line(line) for line in rec.ocr_text.split("\n")}
            occurrences.update(k for k in keys if k and not is_junk_key(k, args.min_line_chars))
        occurrence_chars = sum(len(k) * c for k, c in occurrences.items())
        unique_chars     = sum(len(k) for k in occurrences)
        once_chars       = sum(len(k) for k, c in occurrences.items() if c == 1)
        total_unique_chars      += unique_chars
        total_occurrence_chars  += occurrence_chars
        est_tokens = unique_chars / args.chars_per_token
        print("=" * 78)
        print(f"INDEX STATS for lecture '{lecture_id}' - {len(records)} frames")
        print("=" * 78)
        print(f"  line occurrences:          {sum(occurrences.values()):>12,}  ({occurrence_chars:,} chars)")
        if occurrence_chars:
            print(
                f"  unique lines (union):      {len(occurrences):>12,}  "
                f"({unique_chars:,} chars, {unique_chars / occurrence_chars:.0%} of occurrences)"
            )
        else:
            print(f"  unique lines (union):      {len(occurrences):>12,}  ({unique_chars:,} chars)")
        print(f"    seen once (df = 1):        {once_chars:>12,} chars")
        print(f"    seen 2+ times:             {unique_chars - once_chars:>12,} chars")
        print(
            f"  index projection tokens:   ~{est_tokens:>11,.0f}  "
            f"(at {args.chars_per_token:.2f} ch/tok)"
        )
        if args.context_tokens is not None:
            budget = args.context_tokens - args.output_reserve - args.prompt_overhead
            verdict = "FITS" if budget > 0 and est_tokens <= budget else "DOES NOT FIT"
            print(f"  verdict vs budget:         {verdict:<12} (budget {budget:,})")
        print()
        # The most-repeated lines are the persistent UI: exactly what the
        # document-frequency chrome detection and the V2 index collapse.
        repeated = [(key, count) for key, count in occurrences.most_common(12) if count >= 2]
        print("  top repeated lines (normalized keys):")
        if not repeated:
            print("    (no line repeats)")
        for key, count in repeated:
            print(f"    {count:4d}x  {key[:70]}")
        print()
    # Cross-lecture total: unions stay per lecture, only figures add up.
    print("=" * 78)
    print(f"INDEX TOTAL across {len(by_lecture)} lecture(s)")
    print("=" * 78)
    print(f"  occurrences:               {total_occurrence_chars:>12,} chars")
    print(f"  unique-line union:         {total_unique_chars:>12,} chars")
    if total_occurrence_chars:
        print(
            f"  compression ceiling:       {total_unique_chars / total_occurrence_chars:>12.0%} "
            f"of occurrence chars"
        )
    est_tokens = total_unique_chars / args.chars_per_token
    print(
        f"  index projection tokens:   ~{est_tokens:>11,.0f}  "
        f"(at {args.chars_per_token:.2f} ch/tok)"
    )
    if args.context_tokens is not None:
        budget = args.context_tokens - args.output_reserve - args.prompt_overhead
        verdict = "FITS" if budget > 0 and est_tokens <= budget else "DOES NOT FIT"
        print(f"  verdict vs budget:         {verdict:<12} (budget {budget:,})")
        if getattr(args, "context_tokens_defaulted", False):
            print(
                "  (context default: GLM-5.3-Flash serves 1M tokens per the Z.ai API docs; "
                "override with --context-tokens)"
            )
    print()

# Function: CLI entry point: parse arguments, read the dump, analyze each
# lecture independently, print reports, and write the optional JSON report
# and LLM projection file.
def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description = "Redundant-slide filtering experiment for lecture OCR dumps (V1/V1.5 strategy).",
    )
    parser.add_argument("input", type=Path, help="OCR dump: one slide frame per line, single-quoted CSV-like fields")
    parser.add_argument("--json", dest="json_path", type=Path, default=None, help="write the full report as JSON")
    parser.add_argument("--projection", type=Path, default=None, help="write the LLM projection text file")
    parser.add_argument("--strip-chrome", action="store_true", help="remove chrome lines from the projection file")
    parser.add_argument("--index-stats", action="store_true", help="report the unique-line union vs occurrences (V2 compression ceiling) and exit; skips per-frame analysis")
    parser.add_argument("--min-line-chars", type=int, default=4, help="information floor for a line to participate (default 4)")
    parser.add_argument("--min-new-chars", type=int, default=50, help="new normalized chars that justify keeping a frame (default 50)")
    parser.add_argument("--min-new-ratio", type=float, default=0.25, help="or this share of new content in a small frame (default 0.25)")
    parser.add_argument("--min-coverage", type=float, default=0.5, help="below this anchor coverage the group closes (default 0.5)")
    parser.add_argument("--fuzzy-threshold", type=float, default=0.75, help="line similarity bar for the fuzzy tier (default 0.75)")
    parser.add_argument("--chrome-ratio", type=float, default=0.6, help="document-frequency ratio flagging chrome (default 0.6)")
    parser.add_argument("--chrome-min-frames", type=int, default=3, help="absolute frame count a chrome line must persist across (default 3)")
    parser.add_argument("--revisit-ratio", type=float, default=0.7, help="share of already-seen keys labelling a revisit (default 0.7)")
    parser.add_argument("--drop-revisits", action="store_true", help="route revisits to their original group instead of keeping them")
    parser.add_argument("--context-tokens", type=int, default=None, help="model context window size; enables the FITS / DOES NOT FIT verdict (default: 1M, the GLM-5.3-Flash context per Z.ai docs)")
    parser.add_argument("--chars-per-token", type=float, default=3.0, help="calibration rate for the token estimate; 3.0 is conservative for OCR-dense text (default 3.0)")
    parser.add_argument("--output-reserve", type=int, default=4000, help="tokens reserved for the model's answer (default 4000)")
    parser.add_argument("--prompt-overhead", type=int, default=0, help="tokens of the system/instruction text around the projection (default 0)")
    parser.add_argument("--verbose", action="store_true", help="print the new lines of every kept frame")
    args = parser.parse_args(argv)
    if args.chars_per_token <= 0:
        raise SystemExit("--chars-per-token must be positive")
    # Resolve the context window: fall back to the GLM-5.3-Flash default
    # and remember whether the user chose the value explicitly.
    context_defaulted = args.context_tokens is None
    if context_defaulted:
        args.context_tokens = DEFAULT_CONTEXT_TOKENS
    args.context_tokens_defaulted = context_defaulted
    params = {
        "min_line_chars"    : args.min_line_chars,
        "min_new_chars"     : args.min_new_chars,
        "min_new_ratio"     : args.min_new_ratio,
        "min_coverage"      : args.min_coverage,
        "fuzzy_threshold"   : args.fuzzy_threshold,
        "chrome_ratio"      : args.chrome_ratio,
        "chrome_min_frames" : args.chrome_min_frames,
        "revisit_ratio"     : args.revisit_ratio,
        "drop_revisits"     : args.drop_revisits,
        "context_tokens"    : args.context_tokens,
        "chars_per_token"   : args.chars_per_token,
        "output_reserve"    : args.output_reserve,
        "prompt_overhead"   : args.prompt_overhead,
    }
    records = parse_dump(args.input)
    if not records:
        raise SystemExit(f"No slide records parsed from {args.input}")
    by_lecture: dict[str, list[SlideRecord]] = {}
    for rec in records:
        by_lecture.setdefault(rec.lecture_id, []).append(rec)
    # Quick measurement mode: report the V2 compression ceiling and skip
    # the expensive per-frame matching entirely.
    if args.index_stats:
        print_index_stats(by_lecture, args)
        return
    # The projection text is built for every lecture up front: the TOTAL
    # block needs it even when no projection file is requested.
    lectures_json: list[dict] = []
    per_lecture_projected: dict[str, int] = {}
    reports: list[dict] = []
    for lecture_records in by_lecture.values():
        report = analyze_lecture(lecture_records, params)
        reports.append(report)
        print_report(report, params, verbose=args.verbose)
        print()
        lines = _build_projection_lines(report, params, args.strip_chrome)
        per_lecture_projected[report["lecture_id"]] = len("\n".join(lines))
        if args.json_path:
            lectures_json.append(_lecture_to_json(report))
    totals = _aggregate_totals(reports, per_lecture_projected, args)
    print_total(totals, args)
    if args.projection:
        args.projection.write_text("\n".join(projection_lines) + "\n", encoding="utf-8")
        print(f"LLM projection written to {args.projection}")
    if args.json_path:
        payload = {"params": params, "totals": totals, "lectures": lectures_json}
        args.json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"JSON report written to {args.json_path}")

# Entry point: run the CLI when the script is executed directly.
if __name__ == "__main__":
    main()
