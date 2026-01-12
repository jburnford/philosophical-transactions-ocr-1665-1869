#!/usr/bin/env python3
"""
Import OLMoCR JSON files into SQLite database.
Creates an LLM-friendly schema with both structured fields and full JSON.
"""

import sqlite3
import json
from pathlib import Path
import sys

DB_PATH = Path(__file__).parent / "jstor_metadata.db"
JSON_DIR = Path(__file__).parent / "json"


def update_schema():
    """Update database schema for OCR data."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Drop old ocr_text table if exists (we're replacing it)
    c.execute("DROP TABLE IF EXISTS ocr_text")

    # Create new ocr table with clear, LLM-friendly structure
    c.execute("""
        CREATE TABLE ocr (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Link to documents table
            document_id INTEGER NOT NULL,
            identifier TEXT NOT NULL,

            -- Core OCR content
            full_text TEXT,

            -- Key metadata (extracted for easy querying)
            page_count INTEGER,
            primary_language TEXT,
            olmocr_version TEXT,
            total_input_tokens INTEGER,
            total_output_tokens INTEGER,

            -- Processing info
            source TEXT DEFAULT 'olmocr',
            processed_date TEXT,

            -- Full JSON for complete data access
            -- Contains: attributes.pdf_page_numbers (character offsets per page),
            --           attributes.is_table (per-page table detection),
            --           attributes.primary_language (per-page),
            --           and other detailed metadata
            full_json TEXT,

            FOREIGN KEY (document_id) REFERENCES documents(id),
            UNIQUE(identifier)
        )
    """)

    # Create indexes for common queries
    c.execute("CREATE INDEX idx_ocr_document_id ON ocr(document_id)")
    c.execute("CREATE INDEX idx_ocr_identifier ON ocr(identifier)")
    c.execute("CREATE INDEX idx_ocr_page_count ON ocr(page_count)")

    # Create full-text search virtual table for the OCR text
    c.execute("DROP TABLE IF EXISTS ocr_fts")
    c.execute("""
        CREATE VIRTUAL TABLE ocr_fts USING fts5(
            identifier,
            full_text,
            content='ocr',
            content_rowid='id'
        )
    """)

    # Create triggers to keep FTS in sync
    c.execute("""
        CREATE TRIGGER ocr_ai AFTER INSERT ON ocr BEGIN
            INSERT INTO ocr_fts(rowid, identifier, full_text)
            VALUES (new.id, new.identifier, new.full_text);
        END
    """)

    c.execute("""
        CREATE TRIGGER ocr_ad AFTER DELETE ON ocr BEGIN
            INSERT INTO ocr_fts(ocr_fts, rowid, identifier, full_text)
            VALUES ('delete', old.id, old.identifier, old.full_text);
        END
    """)

    c.execute("""
        CREATE TRIGGER ocr_au AFTER UPDATE ON ocr BEGIN
            INSERT INTO ocr_fts(ocr_fts, rowid, identifier, full_text)
            VALUES ('delete', old.id, old.identifier, old.full_text);
            INSERT INTO ocr_fts(rowid, identifier, full_text)
            VALUES (new.id, new.identifier, new.full_text);
        END
    """)

    conn.commit()
    conn.close()
    print("Schema updated with OCR table and full-text search")


def import_json_files():
    """Import all JSON files into the database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Get document ID mapping
    c.execute("SELECT identifier, id FROM documents")
    doc_map = {row[0]: row[1] for row in c.fetchall()}

    json_files = list(JSON_DIR.glob("jstor-*.json"))
    print(f"Found {len(json_files)} JSON files to import")

    imported = 0
    errors = 0

    for i, json_file in enumerate(sorted(json_files)):
        identifier = json_file.stem  # e.g., "jstor-104588"

        if identifier not in doc_map:
            print(f"  Warning: {identifier} not in documents table")
            errors += 1
            continue

        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Handle both single object and array format
            if isinstance(data, list):
                data = data[0] if data else {}

            metadata = data.get('metadata', {})
            attributes = data.get('attributes', {})

            # Extract primary language (most common in the document)
            languages = attributes.get('primary_language', [])
            if languages:
                from collections import Counter
                primary_lang = Counter(languages).most_common(1)[0][0]
            else:
                primary_lang = None

            c.execute("""
                INSERT OR REPLACE INTO ocr (
                    document_id, identifier, full_text, page_count,
                    primary_language, olmocr_version, total_input_tokens,
                    total_output_tokens, source, processed_date, full_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                doc_map[identifier],
                identifier,
                data.get('text'),
                metadata.get('pdf-total-pages'),
                primary_lang,
                metadata.get('olmocr-version'),
                metadata.get('total-input-tokens'),
                metadata.get('total-output-tokens'),
                data.get('source', 'olmocr'),
                data.get('created'),
                json.dumps(data, ensure_ascii=False)
            ))

            imported += 1
            if (i + 1) % 500 == 0:
                conn.commit()
                print(f"  Imported {i + 1}/{len(json_files)}...")

        except Exception as e:
            print(f"  Error importing {identifier}: {e}")
            errors += 1

    conn.commit()
    conn.close()

    print(f"\nImport complete: {imported} imported, {errors} errors")


def add_schema_documentation():
    """Add a documentation table explaining the schema for LLMs."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS schema_docs")
    c.execute("""
        CREATE TABLE schema_docs (
            table_name TEXT PRIMARY KEY,
            description TEXT,
            key_columns TEXT,
            example_queries TEXT
        )
    """)

    docs = [
        (
            'documents',
            'Bibliographic metadata for Royal Society Philosophical Transactions articles (1665-1869). Contains 8,128 scholarly articles with author, title, journal, and publication information.',
            'identifier (jstor-XXXXXX), title, creators_string (authors), year, journal_title, volume, issue',
            '''-- Find articles by author
SELECT identifier, title, year FROM documents WHERE creators_string LIKE '%Newton%';

-- Articles by decade
SELECT (year/10)*10 as decade, COUNT(*) FROM documents GROUP BY decade ORDER BY decade;

-- Search by title keyword
SELECT identifier, title, creators_string, year FROM documents WHERE title LIKE '%eclipse%';'''
        ),
        (
            'authors',
            'Normalized author names linked to documents. Each row is one author of one document.',
            'document_id, first_name, last_name, author_order',
            '''-- Most prolific authors
SELECT last_name, first_name, COUNT(*) as papers FROM authors GROUP BY last_name, first_name ORDER BY papers DESC LIMIT 20;

-- Find all papers by a specific author
SELECT d.identifier, d.title, d.year FROM documents d JOIN authors a ON d.id = a.document_id WHERE a.last_name = 'Herschel';'''
        ),
        (
            'ocr',
            'OCR text from OLMoCR processing. Contains full text and metadata for each document.',
            'identifier, full_text, page_count, primary_language, full_json (complete OCR output with page boundaries)',
            '''-- Get OCR text for a document
SELECT full_text FROM ocr WHERE identifier = 'jstor-104588';

-- Find long documents
SELECT o.identifier, d.title, o.page_count FROM ocr o JOIN documents d ON o.document_id = d.id ORDER BY o.page_count DESC LIMIT 10;

-- Documents with tables (check full_json for attributes.is_table)
SELECT identifier FROM ocr WHERE full_json LIKE '%"is_table": true%';'''
        ),
        (
            'ocr_fts',
            'Full-text search index on OCR content. Use FTS5 syntax for searching.',
            'identifier, full_text',
            '''-- Full-text search for a term
SELECT identifier FROM ocr_fts WHERE ocr_fts MATCH 'electricity';

-- Phrase search
SELECT identifier FROM ocr_fts WHERE ocr_fts MATCH '"solar eclipse"';

-- Boolean search
SELECT identifier FROM ocr_fts WHERE ocr_fts MATCH 'newton AND gravity';'''
        ),
        (
            'processing_status',
            'Tracks OCR processing status for each document.',
            'document_id, olmocr_status (pending/completed/failed), error_message',
            '''-- Check failed documents
SELECT d.identifier, d.title, p.error_message FROM documents d JOIN processing_status p ON d.id = p.document_id WHERE p.olmocr_status = 'failed';'''
        )
    ]

    c.executemany("INSERT INTO schema_docs VALUES (?, ?, ?, ?)", docs)
    conn.commit()
    conn.close()
    print("Added schema documentation table")


def print_stats():
    """Print database statistics."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    print("\n" + "=" * 60)
    print("DATABASE STATISTICS")
    print("=" * 60)

    c.execute("SELECT COUNT(*) FROM documents")
    print(f"Documents: {c.fetchone()[0]}")

    c.execute("SELECT COUNT(*) FROM authors")
    print(f"Author records: {c.fetchone()[0]}")

    c.execute("SELECT COUNT(*) FROM ocr")
    print(f"OCR records: {c.fetchone()[0]}")

    c.execute("SELECT SUM(page_count) FROM ocr")
    print(f"Total pages OCR'd: {c.fetchone()[0]:,}")

    c.execute("SELECT SUM(LENGTH(full_text)) FROM ocr")
    total_chars = c.fetchone()[0]
    print(f"Total OCR text: {total_chars:,} characters ({total_chars/1000000:.1f} MB)")

    c.execute("SELECT MIN(year), MAX(year) FROM documents WHERE year IS NOT NULL")
    years = c.fetchone()
    print(f"Year range: {years[0]} - {years[1]}")

    print("\nTop 5 languages:")
    c.execute("""
        SELECT primary_language, COUNT(*) as cnt
        FROM ocr
        WHERE primary_language IS NOT NULL
        GROUP BY primary_language
        ORDER BY cnt DESC
        LIMIT 5
    """)
    for row in c.fetchall():
        print(f"  {row[0]}: {row[1]}")

    conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "stats":
        print_stats()
    else:
        update_schema()
        import_json_files()
        add_schema_documentation()
        print_stats()
