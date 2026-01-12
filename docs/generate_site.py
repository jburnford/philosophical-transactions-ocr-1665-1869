#!/usr/bin/env python3
"""
Generate static GitHub Pages site from the Philosophical Transactions database.
"""

import sqlite3
import json
import html
import re
from pathlib import Path
from collections import defaultdict

DB_PATH = Path(__file__).parent.parent / "jstor_metadata.db"
SITE_DIR = Path(__file__).parent
VOLUMES_DIR = SITE_DIR / "volumes"
ARTICLES_DIR = SITE_DIR / "articles"
DOWNLOADS_DIR = SITE_DIR / "downloads"


def sanitize_filename(s):
    """Create safe filename from string."""
    return re.sub(r'[^\w\-]', '_', s)[:50]


def escape_html(s):
    """Escape HTML special characters."""
    if s is None:
        return ""
    return html.escape(str(s))


def get_db_connection():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def generate_home_page():
    """Generate the main index.html page."""
    conn = get_db_connection()
    c = conn.cursor()

    # Get stats
    c.execute("SELECT COUNT(*) FROM documents")
    total_docs = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM ocr")
    total_ocr = c.fetchone()[0]

    c.execute("SELECT SUM(page_count) FROM ocr")
    total_pages = c.fetchone()[0] or 0

    c.execute("SELECT COUNT(DISTINCT last_name || first_name) FROM authors WHERE last_name IS NOT NULL")
    total_authors = c.fetchone()[0]

    # Get journal series with volumes
    c.execute("""
        SELECT journal_title, MIN(year) as first_year, MAX(year) as last_year,
               COUNT(*) as articles, GROUP_CONCAT(DISTINCT volume) as volumes
        FROM documents
        GROUP BY journal_title
        ORDER BY first_year
    """)
    series = c.fetchall()

    # Build volume data per series
    series_volumes = {}
    for s in series:
        c.execute("""
            SELECT DISTINCT volume, year
            FROM documents
            WHERE journal_title = ?
            ORDER BY CAST(volume AS INTEGER), year
        """, (s['journal_title'],))
        series_volumes[s['journal_title']] = c.fetchall()

    conn.close()

    # Generate HTML
    volumes_html = ""
    for s in series:
        vols = series_volumes[s['journal_title']]
        vol_links = ""
        for v in vols:
            if v['volume']:
                vol_links += f'<a href="volumes/{v["volume"]}/" class="volume-link">Vol. {v["volume"]}</a>\n'

        volumes_html += f'''
        <div class="journal-series">
            <h3>{escape_html(s['journal_title'])}</h3>
            <p class="years">{s['first_year']} - {s['last_year']} | {s['articles']:,} articles</p>
            <div class="volume-grid">
                {vol_links}
            </div>
        </div>
        '''

    page_html = f'''---
layout: default
title: Home
---

<h1>Philosophical Transactions Archive</h1>
<p>Complete OCR archive of the world's first scientific journal, published by the Royal Society of London from 1665 to 1869.</p>

<div class="stats">
    <div class="stat-card">
        <div class="number">{total_docs:,}</div>
        <div class="label">Articles</div>
    </div>
    <div class="stat-card">
        <div class="number">{total_pages:,}</div>
        <div class="label">Pages</div>
    </div>
    <div class="stat-card">
        <div class="number">{total_authors:,}</div>
        <div class="label">Authors</div>
    </div>
    <div class="stat-card">
        <div class="number">204</div>
        <div class="label">Years</div>
    </div>
</div>

<h2>Browse by Volume</h2>
{volumes_html}

<h2>About This Archive</h2>
<p>This archive contains OCR-processed text from the Philosophical Transactions,
the oldest continuously published scientific journal in the world. The journal
was founded in 1665 by Henry Oldenburg, the first Secretary of the Royal Society.</p>

<p>Notable contributors include Isaac Newton, Robert Hooke, Edmond Halley,
Benjamin Franklin, William Herschel, Michael Faraday, and Charles Darwin.</p>

<p>Each article is available for viewing and download in plain text and JSON formats.</p>
'''

    with open(SITE_DIR / "index.html", 'w', encoding='utf-8') as f:
        f.write(page_html)

    print("Generated index.html")


def generate_volume_pages():
    """Generate a page for each volume."""
    conn = get_db_connection()
    c = conn.cursor()

    # Get all volumes
    c.execute("""
        SELECT DISTINCT volume, journal_title, MIN(year) as year
        FROM documents
        WHERE volume IS NOT NULL
        GROUP BY volume, journal_title
        ORDER BY CAST(volume AS INTEGER)
    """)
    volumes = c.fetchall()

    for vol in volumes:
        vol_num = vol['volume']
        vol_dir = VOLUMES_DIR / str(vol_num)
        vol_dir.mkdir(parents=True, exist_ok=True)

        # Get articles in this volume
        c.execute("""
            SELECT d.identifier, d.title, d.creators_string, d.year,
                   d.content_subtype, o.page_count
            FROM documents d
            LEFT JOIN ocr o ON d.id = o.document_id
            WHERE d.volume = ? AND d.journal_title = ?
            ORDER BY d.identifier
        """, (vol_num, vol['journal_title']))
        articles = c.fetchall()

        articles_html = ""
        for a in articles:
            authors = escape_html(a['creators_string']) if a['creators_string'] else "Anonymous"
            title = escape_html(a['title']) if a['title'] else "Untitled"
            page_count = f"({a['page_count']} pages)" if a['page_count'] else ""

            articles_html += f'''
            <li class="article-item">
                <div class="article-title">
                    <a href="../../articles/{a['identifier']}/">{title}</a>
                </div>
                <div class="article-meta">
                    <span class="authors">{authors}</span>
                    <span class="pages">{page_count}</span>
                </div>
            </li>
            '''

        page_html = f'''---
layout: default
title: "Volume {vol_num} ({vol['year']})"
---

<div class="breadcrumb">
    <a href="../../">Home</a>
    <span>&raquo;</span>
    Volume {vol_num}
</div>

<h1>Volume {vol_num} ({vol['year']})</h1>
<p>{escape_html(vol['journal_title'])}</p>
<p>{len(articles)} articles</p>

<h2>Articles</h2>
<ul class="article-list">
{articles_html}
</ul>
'''

        with open(vol_dir / "index.html", 'w', encoding='utf-8') as f:
            f.write(page_html)

    conn.close()
    print(f"Generated {len(volumes)} volume pages")


def generate_article_pages():
    """Generate a page for each article with download links."""
    conn = get_db_connection()
    c = conn.cursor()

    # Get all articles with OCR
    c.execute("""
        SELECT d.identifier, d.title, d.creators_string, d.year, d.volume,
               d.issue, d.journal_title, d.content_subtype,
               d.disciplines, o.full_text, o.page_count, o.primary_language,
               o.full_json
        FROM documents d
        LEFT JOIN ocr o ON d.id = o.document_id
        ORDER BY d.year, CAST(d.volume AS INTEGER), d.identifier
    """)
    articles = c.fetchall()

    # Create downloads directory
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for a in articles:
        identifier = a['identifier']
        article_dir = ARTICLES_DIR / identifier
        article_dir.mkdir(parents=True, exist_ok=True)

        # Prepare data
        title = escape_html(a['title']) if a['title'] else "Untitled"
        authors = escape_html(a['creators_string']) if a['creators_string'] else "Anonymous"
        year = a['year'] or "Unknown"
        volume = a['volume'] or ""
        journal = escape_html(a['journal_title']) if a['journal_title'] else ""
        page_count = a['page_count'] or 0
        language = a['primary_language'] or "en"
        ocr_text = a['full_text'] or "OCR text not available for this article."

        # Generate download files
        md_file = f"{identifier}.md"
        json_file = f"{identifier}.json"

        # Save as Markdown (preserves OLMoCR formatting)
        with open(DOWNLOADS_DIR / md_file, 'w', encoding='utf-8') as f:
            f.write(f"# {a['title'] or 'Untitled'}\n\n")
            f.write(f"**Author(s):** {a['creators_string'] or 'Anonymous'}  \n")
            f.write(f"**Year:** {year}  \n")
            f.write(f"**Journal:** {a['journal_title'] or ''}  \n")
            f.write(f"**Volume:** {volume}  \n")
            f.write(f"**Pages:** {page_count} pages  \n")
            f.write(f"**Identifier:** {identifier}  \n")
            f.write(f"**JSTOR URL:** <https://www.jstor.org/stable/{identifier.replace('jstor-', '')}>  \n")
            f.write("\n---\n\n")
            f.write(a['full_text'] or "OCR text not available.")

        # Save JSON (use the full OCR JSON if available, otherwise create minimal)
        if a['full_json']:
            json_data = json.loads(a['full_json'])
            # Add metadata
            json_data['jstor_metadata'] = {
                'identifier': identifier,
                'title': a['title'],
                'authors': a['creators_string'],
                'year': year,
                'volume': volume,
                'journal': a['journal_title'],
                'page_count': page_count,
                'jstor_url': f"https://www.jstor.org/stable/{identifier.replace('jstor-', '')}"
            }
        else:
            json_data = {
                'identifier': identifier,
                'title': a['title'],
                'authors': a['creators_string'],
                'year': year,
                'text': None,
                'error': 'OCR not available'
            }

        with open(DOWNLOADS_DIR / json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

        # Truncate OCR text for display (keep full in downloads)
        display_text = escape_html(ocr_text)

        # Generate article page with Pagefind data attributes
        page_html = f'''---
layout: default
title: "{title[:100]}"
---

<div class="breadcrumb" data-pagefind-ignore>
    <a href="../../">Home</a>
    <span>&raquo;</span>
    <a href="../../volumes/{volume}/">Volume {volume}</a>
    <span>&raquo;</span>
    Article
</div>

<article data-pagefind-body>
<div class="article-header">
    <h1 data-pagefind-meta="title">{title}</h1>

    <div class="article-details">
        <div class="detail-item">
            <span class="detail-label">Author(s)</span>
            <span class="detail-value" data-pagefind-meta="author" data-pagefind-filter="author">{authors}</span>
        </div>
        <div class="detail-item">
            <span class="detail-label">Year</span>
            <span class="detail-value" data-pagefind-meta="year" data-pagefind-filter="year">{year}</span>
        </div>
        <div class="detail-item">
            <span class="detail-label">Volume</span>
            <span class="detail-value" data-pagefind-meta="volume" data-pagefind-filter="volume">{volume}</span>
        </div>
        <div class="detail-item">
            <span class="detail-label">Pages</span>
            <span class="detail-value">{page_count} pages</span>
        </div>
        <div class="detail-item">
            <span class="detail-label">Language</span>
            <span class="detail-value" data-pagefind-filter="language">{language}</span>
        </div>
        <div class="detail-item">
            <span class="detail-label">Journal</span>
            <span class="detail-value">{journal}</span>
        </div>
    </div>

    <div class="download-links" data-pagefind-ignore>
        <a href="../../downloads/{md_file}" class="download-btn" download>Download MD</a>
        <a href="../../downloads/{json_file}" class="download-btn secondary" download>Download JSON</a>
        <a href="https://www.jstor.org/stable/{identifier.replace('jstor-', '')}" class="download-btn secondary" target="_blank">View on JSTOR</a>
    </div>
</div>

<h2 data-pagefind-ignore>Full Text (OCR)</h2>
<div class="ocr-text">{display_text}</div>
</article>
'''

        with open(article_dir / "index.html", 'w', encoding='utf-8') as f:
            f.write(page_html)

        count += 1
        if count % 500 == 0:
            print(f"  Generated {count} article pages...")

    conn.close()
    print(f"Generated {count} article pages and download files")


def main():
    print("Generating Philosophical Transactions Archive site...")
    print("=" * 60)

    # Create directories
    VOLUMES_DIR.mkdir(parents=True, exist_ok=True)
    ARTICLES_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    generate_home_page()
    generate_volume_pages()
    generate_article_pages()

    print("=" * 60)
    print("Site generation complete!")
    print(f"Output directory: {SITE_DIR}")
    print("\nTo preview locally:")
    print("  cd site && bundle exec jekyll serve")
    print("\nTo deploy to GitHub Pages:")
    print("  1. Create a new GitHub repository")
    print("  2. Push this site directory to the repository")
    print("  3. Enable GitHub Pages in repository settings")


if __name__ == "__main__":
    main()
