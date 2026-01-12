# Philosophical Transactions OCR Archive (1665-1869)

Complete OCR archive of the *Philosophical Transactions of the Royal Society*, the world's first and longest-running scientific journal.

## Contents

- **8,128 articles** from 1665 to 1869
- **106,829 pages** of OCR text
- **197 MB** of searchable text
- Covers 204 years of scientific history

## Data Sources

### PDF Source Files
The original PDF scans were digitized by **JSTOR** as part of their Early Journal Content initiative and are made freely accessible through the **Internet Archive** (archive.org).

- Host: [Internet Archive - JSTOR Early Journal Content](https://archive.org/details/jstor_earlyjournal)
- Digitization: JSTOR
- Format: PDF scans of original printed volumes

### Bibliographic Metadata
Article metadata (titles, authors, dates, volume/issue information) was obtained from **JSTOR** via their [Text Analysis metadata download](https://www.jstor.org/ta-support).

- Source: JSTOR Bibliographic Metadata (JSONL format)
- Fields: Title, authors, publication date, volume, journal series

### OCR Processing
The PDFs were processed using **[OLMoCR](https://github.com/allenai/olmocr)** (Allen Institute for AI), a vision-language model optimized for academic document OCR. Processing was performed on Compute Canada's Nibi cluster using H100 GPUs.

## Public Domain

All content in this archive is in the **public domain**:

- The original *Philosophical Transactions* articles (1665-1869) are out of copyright
- JSTOR Early Journal Content is freely available for public use
- This OCR derivative work is released to the public domain

## Repository Structure

```
├── site/                    # Static website (GitHub Pages)
│   ├── index.html          # Home page with volume browser
│   ├── volumes/            # Volume index pages (159 volumes)
│   ├── articles/           # Individual article pages (8,128)
│   └── downloads/          # Downloadable files
│       ├── *.md            # Markdown with metadata header
│       └── *.json          # Full OCR JSON with page structure
├── jstor_metadata.db       # SQLite database with all data
├── *.py                    # Python scripts for processing
└── README.md
```

## Using the Data

### Browse Online
Visit the GitHub Pages site to browse articles by volume and download individual files.

### SQLite Database
The `jstor_metadata.db` file contains all metadata and OCR text in a queryable format:

```sql
-- Find articles by author
SELECT identifier, title, year
FROM documents
WHERE creators_string LIKE '%Newton%';

-- Full-text search
SELECT identifier
FROM ocr_fts
WHERE ocr_fts MATCH 'electricity';

-- Get OCR text
SELECT full_text
FROM ocr
WHERE identifier = 'jstor-104588';
```

Tables:
- `documents` - Bibliographic metadata (8,128 rows)
- `authors` - Normalized author names (8,708 rows)
- `ocr` - OCR text and processing metadata (8,121 rows)
- `ocr_fts` - Full-text search index
- `schema_docs` - Documentation for LLM queries

### Download Formats

**Markdown (.md)** - Human-readable with metadata header:
```markdown
# Article Title

**Author(s):** Isaac Newton
**Year:** 1672
**Journal:** Philosophical Transactions
...

---

[Full OCR text]
```

**JSON (.json)** - Complete OLMoCR output including:
- Full text
- Page boundaries (character offsets)
- Per-page language detection
- Table detection flags
- Token counts

## Notable Authors

The archive includes works by many foundational figures in science:

- **Isaac Newton** - Optics, calculus, gravity
- **Robert Hooke** - Microscopy, elasticity
- **Edmond Halley** - Comets, actuarial science
- **Benjamin Franklin** - Electricity
- **William Herschel** - Astronomy, discovered Uranus
- **Michael Faraday** - Electromagnetism
- **Charles Darwin** - Evolution (early papers)

## Processing Notes

- **7 articles** failed OCR processing (complex tables or formatting issues)
- Primary languages: English (7,572), Latin (489), French (17)
- OCR quality is generally high but may contain errors, especially for:
  - Mathematical notation
  - Tables with complex formatting
  - Non-English text
  - Degraded original prints

## License

This work is dedicated to the **public domain** under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).

## Acknowledgments

- [JSTOR](https://www.jstor.org/) for digitization and bibliographic metadata
- [Internet Archive](https://archive.org/) for hosting and access
- [Allen Institute for AI](https://allenai.org/) for OLMoCR
- [Compute Canada](https://www.computecanada.ca/) for HPC resources
