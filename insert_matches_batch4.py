#!/usr/bin/env python3
"""Insert fourth batch of author-Wikidata matches."""

import sqlite3

DB_PATH = "jstor_metadata.db"

MATCHES = [
    # Batch 4 - more scientists and variant spellings
    ("J. Frederic Daniell", "Q315752", "John Frederic Daniell", "https://en.wikipedia.org/wiki/John_Frederic_Daniell", 0.90, "English chemist/physicist, FRS"),
    ("J. F. W. Herschel", "Q14278", "John Herschel", "https://en.wikipedia.org/wiki/John_Herschel", 0.95, "English polymath (1792-1871), FRS"),
    ("Mark Catesby", "Q334914", "Mark Catesby", "https://en.wikipedia.org/wiki/Mark_Catesby", 0.90, "English naturalist (1683-1749), FRS"),
    ("Charles Bell", "Q451727", "Charles Bell", "https://en.wikipedia.org/wiki/Sir_Charles_Bell", 0.90, "Scottish physician (1774-1842), FRS"),
    ("Abraham Trembley", "Q115810", "Abraham Trembley", "https://en.wikipedia.org/wiki/Abraham_Trembley", 0.95, "Genevan naturalist (1710-1784), FRS"),
    ("B. C. Brodie", "Q693204", "Benjamin Collins Brodie", "https://en.wikipedia.org/wiki/Sir_Benjamin_Collins_Brodie,_1st_Baronet", 0.85, "British surgeon, FRS"),
    ("George Graham", "Q964704", "George Graham", "https://en.wikipedia.org/wiki/George_Graham_(clockmaker)", 0.90, "British clockmaker/geophysicist (1673-1751), FRS"),
    ("Henry Bence Jones", "Q901039", "Henry Bence Jones", "https://en.wikipedia.org/wiki/Henry_Bence_Jones", 0.90, "English physician/chemist (1813-1873), FRS"),
    ("James Jurin", "Q1959556", "James Jurin", "https://en.wikipedia.org/wiki/James_Jurin", 0.90, "British mathematician/doctor (1684-1750), FRS"),
    ("John Hadley", "Q445128", "John Hadley", "https://en.wikipedia.org/wiki/John_Hadley", 0.90, "English mathematician/astronomer (1682-1744), FRS"),
    ("George Newport", "Q1216232", "George Newport", "https://en.wikipedia.org/wiki/George_Newport", 0.90, "British entomologist (1803-1854), FRS"),
    ("Signor Cassini", "Q14279", "Giovanni Domenico Cassini", "https://en.wikipedia.org/wiki/Giovanni_Domenico_Cassini", 0.85, "Italian/French astronomer (1625-1712)"),
    ("John Martyn", "Q1373287", "John Martyn", "https://en.wikipedia.org/wiki/John_Martyn_(botanist)", 0.85, "British botanist (1699-1768), FRS"),
    ("Samuel Dale", "Q1538175", "Samuel Dale", "https://en.wikipedia.org/wiki/Samuel_Dale_(naturalist)", 0.85, "British botanist (1659-1739), FRS"),
    ("Martin Barry", "Q6774947", "Martin Barry", "https://en.wikipedia.org/wiki/Martin_Barry_(physician)", 0.85, "British physician, FRS"),
    ("M. Maty", "Q763707", "Matthew Maty", "https://en.wikipedia.org/wiki/Matthew_Maty", 0.85, "Dutch physician, FRS"),
    ("Henry Foster", "Q1365806", "Henry Foster", "https://en.wikipedia.org/wiki/Henry_Foster_(explorer)", 0.85, "Royal Navy officer/scientist, FRS"),
    ("John Hellins", "Q6238574", "John Hellins", "https://en.wikipedia.org/wiki/John_Hellins", 0.85, "British astronomer, FRS"),
    ("Jeremiah Milles", "Q6180913", "Jeremiah Milles", "https://en.wikipedia.org/wiki/Jeremiah_Milles", 0.85, "Dean of Exeter/antiquarian (1714-1784), FRS"),
    # Variant spellings that map to existing matches
    ("Antony Van Leeuwenhoek", "Q43522", "Antonie van Leeuwenhoek", "https://en.wikipedia.org/wiki/Antonie_van_Leeuwenhoek", 0.90, "Dutch scientist (1632-1723), FRS"),
    ("H. Davy", "Q131761", "Humphry Davy", "https://en.wikipedia.org/wiki/Humphry_Davy", 0.95, "British chemist (1778-1829), FRS"),
    ("Mr. Flamstead", "Q242388", "John Flamsteed", "https://en.wikipedia.org/wiki/John_Flamsteed", 0.90, "First Astronomer Royal, FRS"),
    ("J. Smeaton", "Q460922", "John Smeaton", "https://en.wikipedia.org/wiki/John_Smeaton", 0.90, "English engineer (1724-1792), FRS"),
    ("Dr. Wallis", "Q208359", "John Wallis", "https://en.wikipedia.org/wiki/John_Wallis", 0.90, "English mathematician (1616-1703), FRS"),
    ("John William Lubbock", "Q568443", "John Lubbock, 3rd Baronet", "https://en.wikipedia.org/wiki/Sir_John_Lubbock,_3rd_Baronet", 0.90, "English banker/mathematician, FRS"),
    ("John Bevis", "Q437582", "John Bevis", "https://en.wikipedia.org/wiki/John_Bevis", 0.90, "English astronomer (1695-1771), FRS"),
]

def get_author_stats(conn, author_name):
    cursor = conn.execute("""
        SELECT MIN(d.year), MAX(d.year), COUNT(*)
        FROM authors a
        JOIN documents d ON a.document_id = d.id
        WHERE a.full_name = ?
    """, (author_name,))
    return cursor.fetchone()

def main():
    conn = sqlite3.connect(DB_PATH)

    inserted = 0
    skipped = 0
    for author_name, qid, label, wiki_url, confidence, reason in MATCHES:
        stats = get_author_stats(conn, author_name)
        if stats[0] is None:
            print(f"Author not found in corpus: {author_name}")
            skipped += 1
            continue

        first_pub, last_pub, article_count = stats

        existing = conn.execute(
            "SELECT qid FROM author_wikidata WHERE author_name = ?",
            (author_name,)
        ).fetchone()

        if existing:
            print(f"Already exists: {author_name}")
            skipped += 1
            continue

        conn.execute("""
            INSERT INTO author_wikidata
            (author_name, first_pub_year, last_pub_year, article_count,
             qid, wikidata_label, wikipedia_url, confidence, match_reason,
             candidates_json, reviewed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', 1)
        """, (author_name, first_pub, last_pub, article_count,
              qid, label, wiki_url, confidence, reason))
        inserted += 1
        print(f"Inserted: {author_name} -> {qid} ({label})")

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM author_wikidata").fetchone()[0]
    matched = conn.execute("SELECT COUNT(*) FROM author_wikidata WHERE qid IS NOT NULL").fetchone()[0]
    articles = conn.execute("SELECT SUM(article_count) FROM author_wikidata WHERE qid IS NOT NULL").fetchone()[0]

    print(f"\n=== Summary ===")
    print(f"New inserts: {inserted}")
    print(f"Skipped: {skipped}")
    print(f"Total matched authors: {total}")
    print(f"Articles covered: {articles}")

    conn.close()

if __name__ == "__main__":
    main()
