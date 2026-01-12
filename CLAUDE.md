# Philosophical Transactions OCR Archive

## Project Overview

This project creates a complete OCR archive of the *Philosophical Transactions of the Royal Society* (1665-1869), the world's first scientific journal. The archive includes 8,128 articles spanning 204 years of scientific history.

## Data Pipeline

### 1. Source Data
- **PDFs**: Downloaded from Internet Archive (JSTOR Early Journal Content)
- **Identifiers**: 8,128 JSTOR stable IDs in `jstor_files.txt`
- **Metadata**: Downloaded from JSTOR Text Analysis Support (12.5M record JSONL file)

### 2. OCR Processing
- **Tool**: OLMoCR (Allen Institute for AI)
- **Hardware**: H100 GPUs on Digital Research Alliance of Canada's Nibi cluster
- **Output**: JSONL files with full text, page boundaries, language detection
- **Results**: 8,121 successful, 7 failed (complex tables)

### 3. Database
- **File**: `jstor_metadata.db` (489 MB SQLite)
- **Tables**:
  - `documents` - Bibliographic metadata (8,128 rows)
  - `authors` - Normalized author names (8,708 rows)
  - `ocr` - OCR text and JSON (8,121 rows)
  - `ocr_fts` - Full-text search index
  - `processing_status` - OCR status tracking
  - `schema_docs` - LLM-friendly documentation

### 4. Static Website
- **Location**: `docs/` (for GitHub Pages)
- **Structure**: Home → Volumes (159) → Articles (8,128)
- **Downloads**: MD and JSON files for each article
- **GitHub**: https://github.com/jburnford/philosophical-transactions-ocr-1665-1869
- **Live Site**: https://jburnford.github.io/philosophical-transactions-ocr-1665-1869/

## Key Scripts

| Script | Purpose |
|--------|---------|
| `load_jstor_metadata.py` | Parse JSTOR metadata JSONL, create database |
| `split_olmocr_jsonl.py` | Split OLMoCR output into per-article JSON |
| `import_ocr_to_db.py` | Import OCR JSON into SQLite with FTS |
| `docs/generate_site.py` | Generate static HTML site from database |

## Database Queries

```sql
-- Find articles by author
SELECT identifier, title, year FROM documents
WHERE creators_string LIKE '%Newton%';

-- Full-text search
SELECT identifier FROM ocr_fts
WHERE ocr_fts MATCH 'electricity';

-- Get OCR text for an article
SELECT full_text FROM ocr WHERE identifier = 'jstor-104588';

-- Check failed OCR
SELECT d.identifier, d.title FROM documents d
JOIN processing_status p ON d.id = p.document_id
WHERE p.olmocr_status = 'failed';
```

## File Locations

| Item | Path | Size |
|------|------|------|
| SQLite database | `jstor_metadata.db` | 489 MB |
| Static site | `docs/` | 696 MB |
| Article downloads | `docs/downloads/` | 436 MB |
| Original JSTOR IDs | `jstor_files.txt` | 116 KB |
| JSONL files | `*.jsonl` (gitignored) | ~200 MB |
| JSTOR metadata dump | `jstor_metadata_2026-01-11.jsonl.gz` (gitignored) | 1.2 GB |

## Collection Statistics

- **Articles**: 8,128
- **Pages OCR'd**: 106,829
- **OCR Text**: 197 MB
- **Year Range**: 1665-1869
- **Languages**: English (7,572), Latin (489), French (17)
- **Top Authors**: Everard Home (117), Joseph Banks (66), William Herschel (66)

## Journal Series

1. **Philosophical Transactions (1665-1678)** - 919 articles
2. **Philosophical Transactions (1683-1775)** - 4,392 articles
3. **Philosophical Transactions of the Royal Society of London (1776-1869)** - 2,817 articles

## Notes

- Database excluded from GitHub due to 100 MB file limit
- 7 articles failed OCR (complex tables): jstor-106385, jstor-108400, jstor-108453, jstor-108480, jstor-108864, jstor-3701573, jstor-41206194
- OLMoCR preserves markdown formatting including tables
- JSON downloads include page boundary offsets for text extraction
