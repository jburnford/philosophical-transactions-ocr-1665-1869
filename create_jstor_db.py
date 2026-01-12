#!/usr/bin/env python3
"""
Create and populate SQLite database with Internet Archive JSTOR metadata.
Uses the internetarchive library for API access.
"""

import sqlite3
import json
import re
import time
from pathlib import Path
from typing import Optional
import sys

try:
    import internetarchive as ia
except ImportError:
    print("Please install internetarchive: pip install internetarchive")
    sys.exit(1)

DB_PATH = Path(__file__).parent / "jstor_royalsociety.db"
IDS_FILE = Path(__file__).parent / "jstor_files.txt"

def create_database():
    """Create the SQLite database with appropriate schema."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Main documents table
    c.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            identifier TEXT UNIQUE NOT NULL,
            title TEXT,
            article_type TEXT,
            date TEXT,
            year INTEGER,
            journal_title TEXT,
            journal_abbrev TEXT,
            volume TEXT,
            issue TEXT,
            page_range TEXT,
            page_start INTEGER,
            page_end INTEGER,
            issn TEXT,
            language TEXT,
            description TEXT,
            contributor TEXT,
            publisher TEXT,
            image_count INTEGER,
            ppi INTEGER,
            collection TEXT,
            source TEXT,
            mediatype TEXT,
            added_date TEXT,
            public_date TEXT,
            item_size INTEGER,
            has_ia_ocr INTEGER DEFAULT 0,
            has_olmocr INTEGER DEFAULT 0,
            metadata_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # OCR text table (for future use with OLMoCR results)
    c.execute("""
        CREATE TABLE IF NOT EXISTS ocr_text (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            ocr_type TEXT NOT NULL,  -- 'ia_ocr' or 'olmocr'
            page_number INTEGER,
            text_content TEXT,
            confidence REAL,
            processing_date TIMESTAMP,
            FOREIGN KEY (document_id) REFERENCES documents(id),
            UNIQUE(document_id, ocr_type, page_number)
        )
    """)

    # Files table to track associated files
    c.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            filename TEXT NOT NULL,
            format TEXT,
            size INTEGER,
            source TEXT,
            FOREIGN KEY (document_id) REFERENCES documents(id),
            UNIQUE(document_id, filename)
        )
    """)

    # Processing status table
    c.execute("""
        CREATE TABLE IF NOT EXISTS processing_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER UNIQUE NOT NULL,
            download_status TEXT DEFAULT 'pending',
            olmocr_status TEXT DEFAULT 'pending',
            olmocr_job_id TEXT,
            olmocr_started_at TIMESTAMP,
            olmocr_completed_at TIMESTAMP,
            error_message TEXT,
            FOREIGN KEY (document_id) REFERENCES documents(id)
        )
    """)

    # Create indexes
    c.execute("CREATE INDEX IF NOT EXISTS idx_documents_identifier ON documents(identifier)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_documents_year ON documents(year)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_documents_journal ON documents(journal_title)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ocr_document ON ocr_text(document_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ocr_type ON ocr_text(ocr_type)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_processing_status ON processing_status(olmocr_status)")

    conn.commit()
    conn.close()
    print(f"Database created at: {DB_PATH}")

def parse_identifiers():
    """Parse JSTOR identifiers from the column-formatted text file."""
    with open(IDS_FILE, 'r') as f:
        content = f.read()

    # Split on whitespace and filter for valid identifiers
    identifiers = re.findall(r'jstor-\d+', content)
    unique_ids = sorted(set(identifiers))
    print(f"Found {len(unique_ids)} unique identifiers")
    return unique_ids

def extract_year(date_str: str) -> Optional[int]:
    """Extract year from various date formats."""
    if not date_str:
        return None
    match = re.search(r'\b(1[6-9]\d{2}|20[0-2]\d)\b', str(date_str))
    return int(match.group(1)) if match else None

def parse_page_range(page_range: str) -> tuple:
    """Parse page range into start and end pages."""
    if not page_range:
        return None, None
    # Handle formats like "1-10", "1 - 10", "pp. 1-10", etc.
    match = re.search(r'(\d+)\s*[-–—]\s*(\d+)', str(page_range))
    if match:
        return int(match.group(1)), int(match.group(2))
    # Single page
    match = re.search(r'^(\d+)$', str(page_range).strip())
    if match:
        page = int(match.group(1))
        return page, page
    return None, None

def get_first(val):
    """Get first item if list, otherwise return value."""
    if isinstance(val, list):
        return val[0] if val else None
    return val

def insert_document(conn, identifier: str, item):
    """Insert document metadata into database."""
    c = conn.cursor()

    meta = item.metadata

    date = get_first(meta.get('date', ''))
    year = extract_year(date)
    page_range = get_first(meta.get('pagerange', ''))
    page_start, page_end = parse_page_range(page_range)

    # Check if IA OCR exists by looking at files
    has_ia_ocr = False
    files_list = []
    try:
        for f in item.files:
            files_list.append(f)
            fmt = f.get('format', '').lower()
            name = f.get('name', '')
            if fmt in ['text', 'djvutxt', 'full text'] or name.endswith('_djvu.txt'):
                has_ia_ocr = True
    except Exception as e:
        print(f"  Warning: Could not get files for {identifier}: {e}")

    try:
        c.execute("""
            INSERT OR REPLACE INTO documents (
                identifier, title, article_type, date, year, journal_title,
                journal_abbrev, volume, issue, page_range, page_start, page_end,
                issn, language, description, contributor, publisher,
                image_count, ppi, collection, source, mediatype,
                added_date, public_date, item_size, has_ia_ocr, metadata_json,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            identifier,
            get_first(meta.get('title')),
            get_first(meta.get('article-type')),
            date,
            year,
            get_first(meta.get('journaltitle')),
            get_first(meta.get('journalabbrv')),
            get_first(meta.get('volume')),
            get_first(meta.get('issue')),
            page_range,
            page_start,
            page_end,
            get_first(meta.get('issn')),
            get_first(meta.get('language')),
            get_first(meta.get('description')),
            get_first(meta.get('contributor')),
            get_first(meta.get('publisher')),
            meta.get('imagecount'),
            meta.get('ppi'),
            json.dumps(meta.get('collection', [])) if isinstance(meta.get('collection'), list) else meta.get('collection'),
            get_first(meta.get('source')),
            get_first(meta.get('mediatype')),
            get_first(meta.get('addeddate')),
            get_first(meta.get('publicdate')),
            item.item_size if hasattr(item, 'item_size') else None,
            1 if has_ia_ocr else 0,
            json.dumps(dict(meta))
        ))

        doc_id = c.lastrowid

        # Insert file information
        for f in files_list:
            if f.get('name'):
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO files (document_id, filename, format, size, source)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        doc_id,
                        f.get('name'),
                        f.get('format'),
                        f.get('size'),
                        f.get('source')
                    ))
                except:
                    pass

        # Initialize processing status
        c.execute("""
            INSERT OR IGNORE INTO processing_status (document_id)
            VALUES (?)
        """, (doc_id,))

        return True
    except Exception as e:
        print(f"  Error inserting {identifier}: {e}")
        return False

def populate_database(batch_size: int = 100, delay: float = 0.3):
    """Fetch metadata and populate the database using internetarchive library."""
    identifiers = parse_identifiers()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Get already processed identifiers
    c.execute("SELECT identifier FROM documents")
    existing = set(row[0] for row in c.fetchall())

    to_process = [i for i in identifiers if i not in existing]
    print(f"Already processed: {len(existing)}")
    print(f"Remaining to process: {len(to_process)}")

    if not to_process:
        print("Nothing to process!")
        conn.close()
        return

    success_count = 0
    error_count = 0

    for i, identifier in enumerate(to_process):
        print(f"[{i+1}/{len(to_process)}] Fetching {identifier}...", end="", flush=True)

        try:
            item = ia.get_item(identifier)
            if item.exists:
                if insert_document(conn, identifier, item):
                    success_count += 1
                    print(" OK")
                else:
                    error_count += 1
                    print(" FAILED (insert)")
            else:
                error_count += 1
                print(" NOT FOUND")
        except Exception as e:
            error_count += 1
            print(f" ERROR: {e}")

        # Commit in batches
        if (i + 1) % batch_size == 0:
            conn.commit()
            print(f"  Committed batch, {success_count} successful, {error_count} errors")

        # Rate limiting
        time.sleep(delay)

    conn.commit()
    conn.close()

    print(f"\nComplete! Success: {success_count}, Errors: {error_count}")

def get_stats():
    """Print database statistics."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM documents")
    total = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM documents WHERE has_ia_ocr = 1")
    with_ocr = c.fetchone()[0]

    c.execute("SELECT MIN(year), MAX(year) FROM documents WHERE year IS NOT NULL")
    year_range = c.fetchone()

    c.execute("SELECT SUM(image_count) FROM documents WHERE image_count IS NOT NULL")
    total_pages = c.fetchone()[0]

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
        SELECT year, COUNT(*) as cnt
        FROM documents
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)
    by_year = c.fetchall()

    conn.close()

    print(f"\n=== Database Statistics ===")
    print(f"Total documents: {total}")
    print(f"Total pages (images): {total_pages or 0:,}")
    print(f"Documents with IA OCR: {with_ocr}")
    if year_range[0]:
        print(f"Year range: {year_range[0]} - {year_range[1]}")
    print(f"\nTop 10 Journals:")
    for journal, count in top_journals:
        print(f"  {count:5d}  {journal[:60]}")

    if by_year:
        print(f"\nDocuments by decade:")
        decades = {}
        for year, count in by_year:
            decade = (year // 10) * 10
            decades[decade] = decades.get(decade, 0) + count
        for decade in sorted(decades.keys()):
            print(f"  {decade}s: {decades[decade]:5d}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "create":
            create_database()
        elif sys.argv[1] == "populate":
            populate_database()
        elif sys.argv[1] == "stats":
            get_stats()
        elif sys.argv[1] == "all":
            create_database()
            populate_database()
        else:
            print("Usage: python create_jstor_db.py [create|populate|stats|all]")
    else:
        # Default: create and populate
        create_database()
        populate_database()
