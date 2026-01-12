#!/usr/bin/env python3
"""Insert third batch of author-Wikidata matches."""

import sqlite3

DB_PATH = "jstor_metadata.db"

MATCHES = [
    # Batch 3 - more scientists
    ("John Swinton", "Q6259828", "John Swinton", "https://en.wikipedia.org/wiki/John_Swinton_(orientalist)", 0.80, "British cleric and orientalist, FRS"),
    ("William Hunter", "Q944360", "William Hunter", "https://en.wikipedia.org/wiki/William_Hunter_(anatomist)", 0.90, "British anatomist (1718-1783), FRS"),
    ("Edward Tyson", "Q1293932", "Edward Tyson", "https://en.wikipedia.org/wiki/Edward_Tyson", 0.90, "English scientist/physician (1651-1708), FRS"),
    ("William Molyneux", "Q2096504", "William Molyneux", "https://en.wikipedia.org/wiki/William_Molyneux", 0.90, "Irish natural philosopher, FRS"),
    ("Monsieur Hevelius", "Q57963", "Johannes Hevelius", "https://en.wikipedia.org/wiki/Johannes_Hevelius", 0.90, "Polish astronomer (1611-1687), FRS"),
    ("Peter Barlow", "Q390352", "Peter Barlow", "https://en.wikipedia.org/wiki/Peter_Barlow_(mathematician)", 0.90, "British mathematician/physicist, FRS"),
    ("Thomas Percival", "Q549582", "Thomas Percival", "https://en.wikipedia.org/wiki/Thomas_Percival", 0.85, "British physician, FRS"),
    ("Carlo Matteucci", "Q280250", "Carlo Matteucci", "https://en.wikipedia.org/wiki/Carlo_Matteucci", 0.90, "Italian physicist (1811-1868), FRS"),
    ("Thomas Molyneux", "Q7529211", "Thomas Molyneux", "https://en.wikipedia.org/wiki/Sir_Thomas_Molyneux,_1st_Baronet", 0.85, "Irish physician (1661-1733), FRS"),
    ("Francis Wollaston", "Q3081739", "Francis Wollaston", "https://en.wikipedia.org/wiki/Francis_Wollaston_(astronomer)", 0.85, "English astronomer, FRS"),
    ("Edward Pigott", "Q535209", "Edward Pigott", "https://en.wikipedia.org/wiki/Edward_Pigott", 0.90, "British astronomer, FRS"),
    ("J. Bevis", "Q437582", "John Bevis", "https://en.wikipedia.org/wiki/John_Bevis", 0.90, "English astronomer (1695-1771), FRS"),
    ("George Pearson", "Q5543290", "George Pearson", "https://en.wikipedia.org/wiki/George_Pearson_(physician)", 0.85, "British physician, FRS"),
    ("Erasmus Darwin", "Q234050", "Erasmus Darwin", "https://en.wikipedia.org/wiki/Erasmus_Darwin", 0.95, "English physician (1731-1802), FRS"),
    ("Cotton Mather", "Q380719", "Cotton Mather", "https://en.wikipedia.org/wiki/Cotton_Mather", 0.90, "American minister/scientific writer, FRS"),
    ("Samuel Horsley", "Q3471152", "Samuel Horsley", "https://en.wikipedia.org/wiki/Samuel_Horsley", 0.90, "Bishop/scientist (1733-1806), FRS"),
    ("Daniel Rutherford", "Q313067", "Daniel Rutherford", "https://en.wikipedia.org/wiki/Daniel_Rutherford", 0.90, "British chemist/botanist (1749-1819), FRS"),
    ("William Henry", "Q182915", "William Henry", "https://en.wikipedia.org/wiki/William_Henry_(chemist)", 0.85, "British chemist (Henry's law), FRS"),
    ("John Michell", "Q373097", "John Michell", "https://en.wikipedia.org/wiki/John_Michell", 0.90, "English natural philosopher, FRS"),
    # George Biddell Airy variant
    ("George Biddell Airy", "Q20018", "George Biddell Airy", "https://en.wikipedia.org/wiki/George_Biddell_Airy", 0.95, "English Astronomer Royal (1801-1892), FRS"),
    # More famous scientists who might be in corpus
    ("Ole Rømer", "Q160187", "Ole Rømer", "https://en.wikipedia.org/wiki/Ole_R%C3%B8mer", 0.90, "Danish astronomer (1644-1710)"),
    # Variant spellings
    ("Wm. Watson", "Q462269", "William Watson", "https://en.wikipedia.org/wiki/William_Watson_(scientist)", 0.90, "English physician/scientist (1715-1787), FRS"),
    ("C. Mortimer", "Q5187670", "Cromwell Mortimer", "https://en.wikipedia.org/wiki/Cromwell_Mortimer", 0.85, "British physician, FRS Secretary"),
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

    print(f"\n=== Summary ===")
    print(f"New inserts: {inserted}")
    print(f"Skipped: {skipped}")
    print(f"Total in table: {total}")
    print(f"With QID: {matched}")

    conn.close()

if __name__ == "__main__":
    main()
