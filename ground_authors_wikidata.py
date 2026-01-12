#!/usr/bin/env python3
"""
Ground Philosophical Transactions authors to Wikidata entities.

Uses the Wikidata API to search for author names and disambiguate using:
- Birth/death dates (must be alive during publication years)
- Royal Society membership (P463: Q123885)
- Instance of human (P31: Q5)

Outputs a table with QIDs, confidence scores, and Wikipedia links.
"""

import sqlite3
import json
import time
import re
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import requests

DB_PATH = Path("jstor_metadata.db")
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

# Royal Society QID
ROYAL_SOCIETY_QID = "Q123885"

# User-Agent required by Wikidata API
USER_AGENT = "PhilTransOCRBot/1.0 (https://github.com/jburnford/philosophical-transactions-ocr-1665-1869; jic823@usask.ca)"
HEADERS = {"User-Agent": USER_AGENT}

@dataclass
class WikidataCandidate:
    qid: str
    label: str
    description: str
    birth_year: Optional[int] = None
    death_year: Optional[int] = None
    is_human: bool = False
    is_frs: bool = False  # Fellow of Royal Society
    wikipedia_url: Optional[str] = None

@dataclass
class AuthorMatch:
    author_name: str
    first_pub: int
    last_pub: int
    article_count: int
    qid: Optional[str] = None
    wikidata_label: Optional[str] = None
    wikipedia_url: Optional[str] = None
    confidence: float = 0.0
    match_reason: str = ""
    candidates_json: str = "[]"


def init_db():
    """Create author_wikidata table if not exists."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS author_wikidata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author_name TEXT UNIQUE NOT NULL,
            first_pub_year INTEGER,
            last_pub_year INTEGER,
            article_count INTEGER,
            qid TEXT,
            wikidata_label TEXT,
            wikipedia_url TEXT,
            confidence REAL,
            match_reason TEXT,
            candidates_json TEXT,
            reviewed INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_aw_confidence ON author_wikidata(confidence)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_aw_qid ON author_wikidata(qid)")
    conn.commit()
    conn.close()
    print("Database initialized.")


def get_authors_to_process(limit: Optional[int] = None) -> list[dict]:
    """Get unique authors with publication date ranges."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Check which authors already processed
    existing = set()
    try:
        cursor = conn.execute("SELECT author_name FROM author_wikidata")
        existing = {row[0] for row in cursor}
    except sqlite3.OperationalError:
        pass  # Table doesn't exist yet

    query = """
        SELECT
            a.full_name as author_name,
            MIN(d.year) as first_pub,
            MAX(d.year) as last_pub,
            COUNT(*) as article_count
        FROM authors a
        JOIN documents d ON a.document_id = d.id
        WHERE a.full_name IS NOT NULL
          AND a.full_name <> ''
          AND length(a.full_name) > 2
        GROUP BY a.full_name
        ORDER BY article_count DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    cursor = conn.execute(query)
    authors = []
    for row in cursor:
        if row['author_name'] not in existing:
            authors.append(dict(row))
    conn.close()

    print(f"Found {len(authors)} authors to process (excluding {len(existing)} already done).")
    return authors


def search_wikidata(name: str, limit: int = 10) -> list[dict]:
    """Search Wikidata for entities matching name."""
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "language": "en",
        "type": "item",
        "limit": limit,
        "search": name
    }

    try:
        resp = requests.get(WIKIDATA_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("search", [])
    except Exception as e:
        print(f"  Error searching for '{name}': {e}")
        return []


def get_entity_details(qid: str) -> Optional[WikidataCandidate]:
    """Get detailed claims for a Wikidata entity using SPARQL."""
    query = f"""
    SELECT ?item ?itemLabel ?itemDescription ?birth ?death ?frs ?sitelink WHERE {{
      BIND(wd:{qid} AS ?item)

      OPTIONAL {{ ?item wdt:P569 ?birth. }}
      OPTIONAL {{ ?item wdt:P570 ?death. }}

      # Check if instance of human
      OPTIONAL {{
        ?item wdt:P31 wd:Q5.
        BIND(true AS ?isHuman)
      }}

      # Check Royal Society membership
      OPTIONAL {{
        ?item wdt:P463 wd:{ROYAL_SOCIETY_QID}.
        BIND(true AS ?frs)
      }}

      # Get English Wikipedia link
      OPTIONAL {{
        ?sitelink schema:about ?item;
                  schema:isPartOf <https://en.wikipedia.org/>.
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1
    """

    headers = {"Accept": "application/sparql-results+json"}
    try:
        resp = requests.get(
            WIKIDATA_SPARQL,
            params={"query": query},
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        bindings = data.get("results", {}).get("bindings", [])
        if not bindings:
            return None

        b = bindings[0]

        def parse_year(date_str: str) -> Optional[int]:
            if not date_str:
                return None
            # Handle various date formats
            match = re.search(r'(\d{4})', date_str)
            if match:
                return int(match.group(1))
            return None

        birth_year = parse_year(b.get("birth", {}).get("value", ""))
        death_year = parse_year(b.get("death", {}).get("value", ""))

        return WikidataCandidate(
            qid=qid,
            label=b.get("itemLabel", {}).get("value", ""),
            description=b.get("itemDescription", {}).get("value", ""),
            birth_year=birth_year,
            death_year=death_year,
            is_human="isHuman" in str(b) or birth_year is not None,  # Has birth = likely human
            is_frs=b.get("frs", {}).get("value") == "true" if "frs" in b else False,
            wikipedia_url=b.get("sitelink", {}).get("value")
        )
    except Exception as e:
        print(f"  Error getting details for {qid}: {e}")
        return None


def check_frs_membership(qid: str) -> bool:
    """Specifically check if entity is FRS (Fellow of Royal Society)."""
    query = f"""
    ASK {{
      wd:{qid} wdt:P463 wd:{ROYAL_SOCIETY_QID}.
    }}
    """
    headers = {"Accept": "application/sparql-results+json"}
    try:
        resp = requests.get(
            WIKIDATA_SPARQL,
            params={"query": query},
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()
        return resp.json().get("boolean", False)
    except:
        return False


def score_candidate(candidate: WikidataCandidate, first_pub: int, last_pub: int) -> tuple[float, str]:
    """
    Score a candidate match.

    Returns (confidence 0-1, reason string)
    """
    score = 0.0
    reasons = []

    # Must be human (or likely human)
    if not candidate.is_human and candidate.birth_year is None:
        return 0.0, "Not a human entity"

    # Check if alive during publication period
    if candidate.birth_year:
        # Must be born before last publication
        if candidate.birth_year > last_pub:
            return 0.0, f"Born {candidate.birth_year} after last pub {last_pub}"

        # Age at first publication (should be 15-90)
        age_at_first = first_pub - candidate.birth_year
        if age_at_first < 15:
            return 0.0, f"Too young ({age_at_first}) at first pub"
        if age_at_first > 90:
            return 0.0, f"Too old ({age_at_first}) at first pub"

        # Reasonable age bonus
        if 20 <= age_at_first <= 70:
            score += 0.3
            reasons.append(f"reasonable age {age_at_first} at first pub")

    if candidate.death_year:
        # Must die after first publication
        if candidate.death_year < first_pub:
            return 0.0, f"Died {candidate.death_year} before first pub {first_pub}"

        # Small penalty if died long before last pub (possible but less likely)
        if candidate.death_year < last_pub - 5:
            score -= 0.1
            reasons.append(f"died {candidate.death_year} before last pub {last_pub}")

    # FRS membership is strong signal
    if candidate.is_frs:
        score += 0.5
        reasons.append("Fellow of Royal Society")

    # Has birth/death dates adds confidence
    if candidate.birth_year and candidate.death_year:
        score += 0.2
        reasons.append("has birth/death dates")
    elif candidate.birth_year or candidate.death_year:
        score += 0.1

    # Has Wikipedia article
    if candidate.wikipedia_url:
        score += 0.1
        reasons.append("has Wikipedia")

    # Description mentions science/Royal Society
    desc_lower = candidate.description.lower() if candidate.description else ""
    science_terms = ['scientist', 'natural philosopher', 'physicist', 'chemist',
                     'astronomer', 'mathematician', 'botanist', 'surgeon', 'physician',
                     'royal society', 'frs', 'fellow']
    for term in science_terms:
        if term in desc_lower:
            score += 0.1
            reasons.append(f"desc mentions '{term}'")
            break

    # Cap at 1.0
    score = min(1.0, max(0.0, score))

    return score, "; ".join(reasons) if reasons else "basic match"


def process_author(author: dict) -> AuthorMatch:
    """Process a single author, searching Wikidata and scoring matches."""
    name = author['author_name']
    first_pub = author['first_pub']
    last_pub = author['last_pub']
    article_count = author['article_count']

    print(f"Processing: {name} ({article_count} articles, {first_pub}-{last_pub})")

    # Clean up name for search
    clean_name = name
    # Remove honorifics
    for prefix in ['Mr. ', 'Mrs. ', 'Dr. ', 'Sir ', 'Monsieur ', 'Signor ', 'M. ', 'Fr. ']:
        if clean_name.startswith(prefix):
            clean_name = clean_name[len(prefix):]

    # Search Wikidata
    search_results = search_wikidata(clean_name)

    if not search_results:
        # Try original name if cleaned version failed
        if clean_name != name:
            search_results = search_wikidata(name)

    candidates = []
    best_match = None
    best_score = 0.0

    for result in search_results[:5]:  # Check top 5
        qid = result.get("id")
        if not qid:
            continue

        time.sleep(0.2)  # Rate limit

        details = get_entity_details(qid)
        if not details:
            continue

        # Also check FRS specifically
        details.is_frs = details.is_frs or check_frs_membership(qid)

        score, reason = score_candidate(details, first_pub, last_pub)

        candidates.append({
            "qid": qid,
            "label": details.label,
            "description": details.description,
            "birth": details.birth_year,
            "death": details.death_year,
            "frs": details.is_frs,
            "wikipedia": details.wikipedia_url,
            "score": score,
            "reason": reason
        })

        if score > best_score:
            best_score = score
            best_match = details
            best_reason = reason

    # Create match result
    match = AuthorMatch(
        author_name=name,
        first_pub=first_pub,
        last_pub=last_pub,
        article_count=article_count,
        candidates_json=json.dumps(candidates)
    )

    if best_match and best_score > 0.3:  # Threshold for accepting match
        match.qid = best_match.qid
        match.wikidata_label = best_match.label
        match.wikipedia_url = best_match.wikipedia_url
        match.confidence = best_score
        match.match_reason = best_reason
        print(f"  -> Matched: {best_match.qid} ({best_match.label}) score={best_score:.2f}")
    else:
        print(f"  -> No confident match (best score: {best_score:.2f})")

    return match


def save_match(match: AuthorMatch):
    """Save match result to database."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO author_wikidata
        (author_name, first_pub_year, last_pub_year, article_count,
         qid, wikidata_label, wikipedia_url, confidence, match_reason,
         candidates_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, (
        match.author_name, match.first_pub, match.last_pub, match.article_count,
        match.qid, match.wikidata_label, match.wikipedia_url, match.confidence,
        match.match_reason, match.candidates_json
    ))
    conn.commit()
    conn.close()


def print_stats():
    """Print current matching statistics."""
    conn = sqlite3.connect(DB_PATH)

    total = conn.execute("SELECT COUNT(*) FROM author_wikidata").fetchone()[0]
    matched = conn.execute("SELECT COUNT(*) FROM author_wikidata WHERE qid IS NOT NULL").fetchone()[0]
    high_conf = conn.execute("SELECT COUNT(*) FROM author_wikidata WHERE confidence >= 0.7").fetchone()[0]
    med_conf = conn.execute("SELECT COUNT(*) FROM author_wikidata WHERE confidence >= 0.4 AND confidence < 0.7").fetchone()[0]
    low_conf = conn.execute("SELECT COUNT(*) FROM author_wikidata WHERE confidence > 0 AND confidence < 0.4").fetchone()[0]
    no_match = conn.execute("SELECT COUNT(*) FROM author_wikidata WHERE qid IS NULL").fetchone()[0]

    print("\n=== Matching Statistics ===")
    print(f"Total processed: {total}")
    print(f"Matched:         {matched} ({100*matched/total:.1f}%)" if total else "")
    print(f"  High conf:     {high_conf}")
    print(f"  Medium conf:   {med_conf}")
    print(f"  Low conf:      {low_conf}")
    print(f"No match:        {no_match}")

    conn.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Ground authors to Wikidata")
    parser.add_argument("--init", action="store_true", help="Initialize database table")
    parser.add_argument("--limit", type=int, help="Limit number of authors to process")
    parser.add_argument("--stats", action="store_true", help="Print statistics only")
    args = parser.parse_args()

    if args.init:
        init_db()
        return

    if args.stats:
        print_stats()
        return

    init_db()

    authors = get_authors_to_process(args.limit)

    for i, author in enumerate(authors):
        try:
            match = process_author(author)
            save_match(match)

            # Rate limit to be nice to Wikidata
            time.sleep(0.5)

            if (i + 1) % 10 == 0:
                print(f"\n--- Processed {i + 1}/{len(authors)} ---\n")

        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            break
        except Exception as e:
            print(f"  Error processing {author['author_name']}: {e}")
            continue

    print_stats()


if __name__ == "__main__":
    main()
