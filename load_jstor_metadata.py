#!/usr/bin/env python3
"""
Load JSTOR metadata JSONL into SQLite for fast lookups.
Then match against downloaded identifiers and create enriched database.
"""

import sqlite3
import json
import gzip
import re
from pathlib import Path
import sys

DB_PATH = Path(__file__).parent / "jstor_metadata.db"
JSTOR_METADATA = Path(__file__).parent / "jstor_metadata_2026-01-11.jsonl.gz"
IDS_FILE = Path(__file__).parent / "jstor_files.txt"

def create_tables():
    """Create database tables."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Main documents table with JSTOR metadata
    c.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            identifier TEXT UNIQUE NOT NULL,
            stable_id TEXT,
            ithaka_doi TEXT,
            title TEXT,
            creators_string TEXT,
            creators_json TEXT,
            journal_title TEXT,
            journal_code TEXT,
            publisher TEXT,
            published_date TEXT,
            year INTEGER,
            volume TEXT,
            issue TEXT,
            languages TEXT,
            disciplines TEXT,
            content_type TEXT,
            content_subtype TEXT,
            print_issn TEXT,
            online_issn TEXT,
            url TEXT,
            has_olmocr INTEGER DEFAULT 0,
            metadata_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Authors table for normalized author data
    c.execute("""
        CREATE TABLE IF NOT EXISTS authors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT,
            author_order INTEGER,
            FOREIGN KEY (document_id) REFERENCES documents(id)
        )
    """)

    # OCR text table (for OLMoCR results)
    c.execute("""
        CREATE TABLE IF NOT EXISTS ocr_text (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            ocr_type TEXT NOT NULL,
            full_text TEXT,
            page_count INTEGER,
            processing_date TIMESTAMP,
            FOREIGN KEY (document_id) REFERENCES documents(id),
            UNIQUE(document_id, ocr_type)
        )
    """)

    # Processing status
    c.execute("""
        CREATE TABLE IF NOT EXISTS processing_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER UNIQUE NOT NULL,
            download_status TEXT DEFAULT 'pending',
            olmocr_status TEXT DEFAULT 'pending',
            olmocr_job_id TEXT,
            error_message TEXT,
            FOREIGN KEY (document_id) REFERENCES documents(id)
        )
    """)

    # Create indexes
    c.execute("CREATE INDEX IF NOT EXISTS idx_doc_identifier ON documents(identifier)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_doc_stable_id ON documents(stable_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_doc_year ON documents(year)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_doc_journal ON documents(journal_title)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_authors_doc ON authors(document_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_authors_name ON authors(last_name)")

    conn.commit()
    conn.close()
    print(f"Database created: {DB_PATH}")

def parse_identifiers():
    """Parse JSTOR identifiers from the text file."""
    with open(IDS_FILE, 'r') as f:
        content = f.read()
    identifiers = re.findall(r'jstor-(\d+)', content)
    unique_ids = sorted(set(identifiers))
    print(f"Found {len(unique_ids)} unique identifiers")
    return unique_ids

def extract_year(date_str):
    """Extract year from JSTOR date format."""
    if not date_str:
        return None
    match = re.match(r'(\d{4})', str(date_str))
    return int(match.group(1)) if match else None

def load_and_match():
    """Load JSTOR metadata and match against our identifiers."""
    # Get our target identifiers
    target_ids = set(parse_identifiers())
    print(f"Looking for {len(target_ids)} identifiers in JSTOR metadata...")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    found = 0
    processed = 0
    batch = []
    batch_size = 1000

    with gzip.open(JSTOR_METADATA, 'rt', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 1000000 == 0:
                print(f"  Processed {line_num:,} records, found {found} matches...")

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Extract stable ID from ithaka_doi (format: 10.2307/XXXXXX)
            doi = record.get('ithaka_doi', '')
            stable_id = None
            if doi and doi.startswith('10.2307/'):
                stable_id = doi.split('/')[-1]

            # Check if this matches one of our targets
            if stable_id in target_ids:
                found += 1
                identifier = f"jstor-{stable_id}"

                # Extract fields
                creators = record.get('creators') or []
                year = extract_year(record.get('published_date'))

                batch.append((
                    identifier,
                    stable_id,
                    doi,
                    record.get('title'),
                    record.get('creators_string'),
                    json.dumps(creators) if creators else None,
                    record.get('is_part_of'),  # journal title
                    record.get('identifiers', {}).get('journal_code'),
                    json.dumps(record.get('publishers')) if record.get('publishers') else None,
                    record.get('published_date'),
                    year,
                    record.get('issue_volume'),
                    record.get('issue_number'),
                    json.dumps(record.get('languages')) if record.get('languages') else None,
                    json.dumps(record.get('discipline_names')) if record.get('discipline_names') else None,
                    record.get('content_type'),
                    record.get('content_subtype'),
                    record.get('identifiers', {}).get('print_issn'),
                    record.get('identifiers', {}).get('online_issn'),
                    record.get('url'),
                    json.dumps(record),
                    creators  # for author insertion
                ))

                # Insert in batches
                if len(batch) >= batch_size:
                    insert_batch(conn, batch)
                    batch = []

            processed += 1

    # Insert remaining
    if batch:
        insert_batch(conn, batch)

    conn.commit()
    conn.close()

    print(f"\nComplete!")
    print(f"  Total records scanned: {processed:,}")
    print(f"  Matches found: {found}")
    print(f"  Missing: {len(target_ids) - found}")

    return found

def insert_batch(conn, batch):
    """Insert a batch of records."""
    c = conn.cursor()
    for row in batch:
        *doc_fields, creators = row
        try:
            c.execute("""
                INSERT OR REPLACE INTO documents (
                    identifier, stable_id, ithaka_doi, title, creators_string,
                    creators_json, journal_title, journal_code, publisher,
                    published_date, year, volume, issue, languages, disciplines,
                    content_type, content_subtype, print_issn, online_issn, url,
                    metadata_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, doc_fields)

            doc_id = c.lastrowid

            # Insert authors
            for author in (creators or []):
                c.execute("""
                    INSERT INTO authors (document_id, first_name, last_name, full_name, author_order)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    doc_id,
                    author.get('first_name'),
                    author.get('last_name'),
                    f"{author.get('first_name', '')} {author.get('last_name', '')}".strip(),
                    author.get('order')
                ))

            # Initialize processing status
            c.execute("INSERT OR IGNORE INTO processing_status (document_id) VALUES (?)", (doc_id,))

        except Exception as e:
            print(f"  Error inserting {doc_fields[0]}: {e}")

    conn.commit()

def get_stats():
    """Print database statistics."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM documents")
    total = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM authors")
    total_authors = c.fetchone()[0]

    c.execute("SELECT COUNT(DISTINCT document_id) FROM authors")
    docs_with_authors = c.fetchone()[0]

    c.execute("SELECT MIN(year), MAX(year) FROM documents WHERE year IS NOT NULL")
    year_range = c.fetchone()

    c.execute("""
        SELECT journal_title, COUNT(*) as cnt
        FROM documents
        WHERE journal_title IS NOT NULL
        GROUP BY journal_title
        ORDER BY cnt DESC
        LIMIT 10
    """)
    top_journals = c.fetchall()

    c.execute("""
        SELECT a.last_name, a.first_name, COUNT(*) as cnt
        FROM authors a
        GROUP BY a.last_name, a.first_name
        ORDER BY cnt DESC
        LIMIT 10
    """)
    top_authors = c.fetchall()

    conn.close()

    print(f"\n=== Database Statistics ===")
    print(f"Total documents: {total}")
    print(f"Documents with authors: {docs_with_authors}")
    print(f"Total author records: {total_authors}")
    if year_range[0]:
        print(f"Year range: {year_range[0]} - {year_range[1]}")

    print(f"\nTop 10 Journals:")
    for journal, count in top_journals:
        print(f"  {count:5d}  {journal[:60] if journal else 'Unknown'}")

    print(f"\nTop 10 Authors:")
    for last, first, count in top_authors:
        print(f"  {count:3d}  {first or ''} {last or ''}")

def show_sample():
    """Show sample records."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        SELECT d.identifier, d.title, d.creators_string, d.journal_title, d.year
        FROM documents d
        LIMIT 5
    """)

    print("\n=== Sample Records ===")
    for row in c.fetchall():
        print(f"\n{row[0]}")
        print(f"  Title: {row[1][:80] if row[1] else 'N/A'}...")
        print(f"  Authors: {row[2] or 'N/A'}")
        print(f"  Journal: {row[3] or 'N/A'}")
        print(f"  Year: {row[4] or 'N/A'}")

    conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "create":
            create_tables()
        elif cmd == "load":
            create_tables()
            load_and_match()
        elif cmd == "stats":
            get_stats()
        elif cmd == "sample":
            show_sample()
        else:
            print("Usage: python load_jstor_metadata.py [create|load|stats|sample]")
    else:
        create_tables()
        load_and_match()
        get_stats()
