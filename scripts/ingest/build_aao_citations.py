#!/usr/bin/env python3
"""
Casebase — AAO Citation Extractor
===================================
Scans every AAO decision's full_text and extracts outbound citations to:
  1. Other AAO decisions (by filename stem, e.g. "MAR262026_01B6203")
  2. BALCA PERM decisions (e.g. "2006-PER-20")
  3. Precedent decisions (I&N Dec., e.g. "25 I&N Dec. 369")
  4. Matter of … citations (raw party names used as cross-references)

Creates/populates the  aao_citations  table:

    CREATE TABLE IF NOT EXISTS aao_citations (
        id               SERIAL PRIMARY KEY,
        citing_id        INTEGER NOT NULL REFERENCES aao_decisions(id) ON DELETE CASCADE,
        cited_aao_id     INTEGER REFERENCES aao_decisions(id) ON DELETE SET NULL,
        cited_balca_id   INTEGER REFERENCES decisions(id) ON DELETE SET NULL,
        cited_precedent_id INTEGER REFERENCES precedent_decisions(id) ON DELETE SET NULL,
        cited_raw        TEXT NOT NULL,
        citation_type    TEXT NOT NULL,   -- 'aao'|'balca'|'precedent'|'matter_of'
        context_snippet  TEXT,
        UNIQUE (citing_id, cited_raw)
    );

Usage:
    # Dry-run — show counts without writing
    python build_aao_citations.py --dry-run

    # Full run (idempotent — safe to re-run; uses ON CONFLICT DO NOTHING)
    python build_aao_citations.py

    # Process only decisions ingested after a date (incremental sync)
    python build_aao_citations.py --since 2026-01-01

    # Limit rows for testing
    python build_aao_citations.py --limit 500
"""

import argparse
import logging
import os
import re
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "app", ".env"))
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://perm:perm_local_pw@localhost:5432/perm_decisions"
)

# ── Citation patterns ─────────────────────────────────────────────────────────

# I&N Dec. precedents: "25 I&N Dec. 369" or OCR variant "25 l&N Dec. 369"
RE_IND = re.compile(
    r'\b(\d{1,2}\s+[Il]&N\s+Dec\.\s+\d{1,4})\b',
    re.IGNORECASE,
)

# BALCA case numbers: "2006-PER-20", "2021-PER-00123"
RE_BALCA = re.compile(
    r'\b(\d{4}-PER-\d{1,5})\b',
    re.IGNORECASE,
)

# AAO filename stem (without extension): date-prefixed 12-char pattern like "MAR262026_01B6203"
# Also handles legacy numeric-only patterns
RE_AAO_STEM = re.compile(
    r'\b([A-Z]{3}\d{6}_\d{2}[A-Z]\d{4,5})\b',
)

# Matter of … — captures the party name up to a comma, period, or citation
# e.g. "Matter of Chawathe, 25 I&N Dec." → "Chawathe"
# OCR variants handled:
#   "Matter ofChawathe"  — no space after "of" (OCR merges words)
#   "Matter oJChawathe"  — capital J instead of f (common OCR substitution)
#   "Matter of\nPilch"   — newline between "of" and party name
# Lookahead requires either an I&N citation following the comma, or sentence end.
RE_MATTER_OF = re.compile(
    # Capture everything from the party name up to the I&N citation volume number.
    # Uses a possessive-style greedy match bounded by the lookahead for ", NN I&N Dec."
    r'Matter\s+o[fJj]\s*\n?\s*'
    r'([A-Z][A-Za-z0-9\'\-\.&/, ]+?)'
    r'(?=,\s*\d{1,2}\s+[Il]&N\s+Dec\.)',
    re.MULTILINE,
)

SNIPPET_WINDOW = 200   # chars either side of match for context


def snippet(text: str, start: int, end: int, window: int = SNIPPET_WINDOW) -> str:
    lo = max(0, start - window)
    hi = min(len(text), end + window)
    return text[lo:hi].replace("\n", " ").strip()


# ── Schema migration ──────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS aao_citations (
    id                  SERIAL PRIMARY KEY,
    citing_id           INTEGER NOT NULL
                            REFERENCES aao_decisions(id) ON DELETE CASCADE,
    cited_aao_id        INTEGER REFERENCES aao_decisions(id) ON DELETE SET NULL,
    cited_balca_id      INTEGER REFERENCES decisions(id) ON DELETE SET NULL,
    cited_precedent_id  INTEGER REFERENCES precedent_decisions(id) ON DELETE SET NULL,
    cited_raw           TEXT NOT NULL,
    citation_type       TEXT NOT NULL
                            CHECK (citation_type IN ('aao','balca','precedent','matter_of')),
    context_snippet     TEXT,
    UNIQUE (citing_id, cited_raw)
);
CREATE INDEX IF NOT EXISTS idx_aao_citations_citing   ON aao_citations(citing_id);
CREATE INDEX IF NOT EXISTS idx_aao_citations_aao      ON aao_citations(cited_aao_id);
CREATE INDEX IF NOT EXISTS idx_aao_citations_balca    ON aao_citations(cited_balca_id);
CREATE INDEX IF NOT EXISTS idx_aao_citations_prec     ON aao_citations(cited_precedent_id);
CREATE INDEX IF NOT EXISTS idx_aao_citations_type     ON aao_citations(citation_type);
"""


def ensure_schema(conn):
    with conn.cursor() as cur:
        for stmt in DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)
    conn.commit()
    log.info("Schema ready (aao_citations table exists)")


# ── Reference lookup tables ───────────────────────────────────────────────────

def load_reference_maps(conn):
    """
    Returns three dicts for O(1) resolution:
      aao_map:      {filename_without_ext → id}   — all AAO decisions
      balca_map:    {case_number_upper → id}       — all BALCA decisions
      precedent_map:{normalised_ind_citation → id} — I&N Dec. precedents
                    key format: "25 I&N Dec. 369"
    """
    with conn.cursor() as cur:
        # AAO — key by filename stem (without .pdf)
        cur.execute("SELECT id, filename FROM aao_decisions")
        aao_map = {}
        for row in cur.fetchall():
            stem = os.path.splitext(row[1])[0] if row[1] else None
            if stem:
                aao_map[stem.upper()] = row[0]

        # BALCA
        cur.execute("SELECT id, case_number FROM decisions WHERE case_number IS NOT NULL")
        balca_map = {row[1].upper(): row[0] for row in cur.fetchall()}

        # Precedents — normalise "l&N" OCR variant to "I&N"
        cur.execute("SELECT id, citation FROM precedent_decisions")
        precedent_map = {}
        for row in cur.fetchall():
            raw_cit = row[1] or ""
            m = re.search(r'(\d{1,2}\s+[Il]&N\s+Dec\.\s+\d{1,4})', raw_cit, re.IGNORECASE)
            if m:
                key = re.sub(r'[lL]&N', 'I&N', m.group(1)).strip()
                # normalise internal whitespace
                key = re.sub(r'\s+', ' ', key)
                precedent_map[key] = row[0]

    log.info(
        f"Reference maps loaded: {len(aao_map):,} AAO | "
        f"{len(balca_map):,} BALCA | {len(precedent_map):,} precedents"
    )
    return aao_map, balca_map, precedent_map


# ── Per-decision extraction ───────────────────────────────────────────────────

def extract_citations(decision_id: int, text: str,
                      aao_map: dict, balca_map: dict, precedent_map: dict) -> list:
    """
    Returns a list of dicts ready for insertion into aao_citations.
    Deduplicates by cited_raw within a single decision.
    """
    seen_raw: set = set()
    rows = []

    def add(cited_raw, citation_type, cited_aao_id, cited_balca_id,
            cited_precedent_id, ctx):
        cr = cited_raw.strip()
        if not cr or cr in seen_raw:
            return
        seen_raw.add(cr)
        rows.append({
            "citing_id":          decision_id,
            "cited_aao_id":       cited_aao_id,
            "cited_balca_id":     cited_balca_id,
            "cited_precedent_id": cited_precedent_id,
            "cited_raw":          cr,
            "citation_type":      citation_type,
            "context_snippet":    ctx,
        })

    # 1. I&N Dec. precedents
    for m in RE_IND.finditer(text):
        raw = re.sub(r'[lL]&N', 'I&N', m.group(1))
        raw = re.sub(r'\s+', ' ', raw).strip()
        prec_id = precedent_map.get(raw)
        add(raw, "precedent", None, None, prec_id,
            snippet(text, m.start(), m.end()))

    # 2. BALCA case numbers
    for m in RE_BALCA.finditer(text):
        raw = m.group(1).upper()
        balca_id = balca_map.get(raw)
        add(raw, "balca", None, balca_id, None,
            snippet(text, m.start(), m.end()))

    # 3. AAO filename stems
    for m in RE_AAO_STEM.finditer(text):
        raw = m.group(1).upper()
        aao_id = aao_map.get(raw)
        if aao_id and aao_id != decision_id:   # skip self-reference
            add(raw, "aao", aao_id, None, None,
                snippet(text, m.start(), m.end()))

    # 4. "Matter of …" citations (raw — for UI display / future linking)
    for m in RE_MATTER_OF.finditer(text):
        party = m.group(1).strip().rstrip(",.")
        if len(party) < 3:
            continue
        raw = f"Matter of {party}"
        add(raw, "matter_of", None, None, None,
            snippet(text, m.start(), m.end()))

    return rows


# ── Batch upsert ──────────────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT INTO aao_citations
        (citing_id, cited_aao_id, cited_balca_id, cited_precedent_id,
         cited_raw, citation_type, context_snippet)
    VALUES
        (%(citing_id)s, %(cited_aao_id)s, %(cited_balca_id)s, %(cited_precedent_id)s,
         %(cited_raw)s, %(citation_type)s, %(context_snippet)s)
    ON CONFLICT (citing_id, cited_raw) DO NOTHING
"""


def flush(conn, batch: list):
    if not batch:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, INSERT_SQL, batch, page_size=500)
    conn.commit()
    return len(batch)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extract and store inter-corpus citations from AAO decisions"
    )
    parser.add_argument(
        "--db-url", default=DB_URL,
        help="PostgreSQL connection URL",
    )
    parser.add_argument(
        "--since", default=None, metavar="YYYY-MM-DD",
        help="Only process decisions ingested on or after this date (incremental updates)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap the number of AAO decisions processed (useful for testing)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Extract citations but do not write to the database",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Delete all rows from aao_citations before rebuilding",
    )
    args = parser.parse_args()

    conn = psycopg2.connect(args.db_url)
    log.info(f"Connected: {args.db_url.split('@')[-1]}")

    if not args.dry_run:
        ensure_schema(conn)

    if args.reset and not args.dry_run:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM aao_citations")
        conn.commit()
        log.info("Reset: cleared all rows from aao_citations")

    aao_map, balca_map, precedent_map = load_reference_maps(conn)

    # Fetch decisions to process
    conditions = ["full_text != ''", "text_extracted = TRUE"]
    params = []

    # Skip decisions already fully processed (unless --reset was given)
    if not args.reset and not args.dry_run:
        conditions.append(
            "id NOT IN (SELECT DISTINCT citing_id FROM aao_citations)"
        )

    if args.since:
        conditions.append("ingested_at >= %s")
        params.append(args.since)

    where = " AND ".join(conditions)
    limit_clause = f"LIMIT {args.limit}" if args.limit else ""

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT id, filename, full_text FROM aao_decisions "
            f"WHERE {where} ORDER BY id {limit_clause}",
            params or None,
        )
        decisions = cur.fetchall()

    log.info(f"Processing {len(decisions):,} AAO decisions")

    total_inserted = 0
    total_citations = 0
    batch = []
    BATCH_SIZE = 2000

    type_counts = {"aao": 0, "balca": 0, "precedent": 0, "matter_of": 0}

    for i, row in enumerate(decisions, 1):
        cits = extract_citations(
            row["id"], row["full_text"] or "",
            aao_map, balca_map, precedent_map,
        )
        total_citations += len(cits)
        for c in cits:
            type_counts[c["citation_type"]] += 1

        if not args.dry_run:
            batch.extend(cits)
            if len(batch) >= BATCH_SIZE:
                total_inserted += flush(conn, batch)
                batch = []

        if i % 10_000 == 0:
            log.info(
                f"  {i:,}/{len(decisions):,} decisions | "
                f"citations found so far: {total_citations:,}"
            )

    if not args.dry_run and batch:
        total_inserted += flush(conn, batch)

    conn.close()

    log.info("=" * 60)
    log.info(f"Decisions processed : {len(decisions):,}")
    log.info(f"Citations extracted : {total_citations:,}")
    log.info(f"  Precedent (I&N Dec.): {type_counts['precedent']:,}")
    log.info(f"  BALCA case numbers  : {type_counts['balca']:,}")
    log.info(f"  AAO cross-refs      : {type_counts['aao']:,}")
    log.info(f"  Matter of (raw)     : {type_counts['matter_of']:,}")
    if args.dry_run:
        log.info("DRY RUN — nothing written to the database")
    else:
        log.info(f"Rows inserted       : {total_inserted:,}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
