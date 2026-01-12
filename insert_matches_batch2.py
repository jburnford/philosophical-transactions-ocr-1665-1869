#!/usr/bin/env python3
"""Insert second batch of author-Wikidata matches."""

import sqlite3

DB_PATH = "jstor_metadata.db"

MATCHES = [
    # Batch 2 - more famous scientists
    ("Isaac Newton", "Q935", "Isaac Newton", "https://en.wikipedia.org/wiki/Isaac_Newton", 0.98, "English mathematician/physicist (1642-1727), FRS President"),
    ("Henry Kater", "Q1107900", "Henry Kater", "https://en.wikipedia.org/wiki/Henry_Kater", 0.90, "British astronomer (1777-1835), FRS"),
    ("John Ray", "Q316949", "John Ray", "https://en.wikipedia.org/wiki/John_Ray", 0.95, "British botanist (1627-1705), FRS"),
    ("Benjamin Franklin", "Q34969", "Benjamin Franklin", "https://en.wikipedia.org/wiki/Benjamin_Franklin", 0.98, "American polymath (1706-1790), FRS"),
    ("Stephen Hales", "Q312017", "Stephen Hales", "https://en.wikipedia.org/wiki/Stephen_Hales", 0.95, "British scientist (1677-1761), FRS"),
    ("Thomas Young", "Q25820", "Thomas Young", "https://en.wikipedia.org/wiki/Thomas_Young_(scientist)", 0.95, "English polymath (1773-1829), FRS"),
    ("John Tyndall", "Q360808", "John Tyndall", "https://en.wikipedia.org/wiki/John_Tyndall", 0.95, "Irish physicist (1820-1893), FRS"),
    ("John Canton", "Q550798", "John Canton", "https://en.wikipedia.org/wiki/John_Canton", 0.90, "British physicist, FRS"),
    ("Brook Taylor", "Q212085", "Brook Taylor", "https://en.wikipedia.org/wiki/Brook_Taylor", 0.95, "English mathematician (1685-1731), FRS"),
    ("Daines Barrington", "Q667280", "Daines Barrington", "https://en.wikipedia.org/wiki/Daines_Barrington", 0.90, "British lawyer, antiquary, naturalist, FRS"),
    ("Edward Waring", "Q323028", "Edward Waring", "https://en.wikipedia.org/wiki/Edward_Waring", 0.90, "English mathematician, FRS"),
    ("Richard Price", "Q561101", "Richard Price", "https://en.wikipedia.org/wiki/Richard_Price", 0.90, "British philosopher/mathematician (1723-1791), FRS"),
    ("James Ivory", "Q497656", "James Ivory", "https://en.wikipedia.org/wiki/James_Ivory_(mathematician)", 0.85, "Scottish mathematician, FRS"),
    ("William Musgrave", "Q8015973", "William Musgrave", "https://en.wikipedia.org/wiki/William_Musgrave_(physician)", 0.85, "British physician/antiquary (1655-1721), FRS"),
    ("Thomas Graham", "Q333762", "Thomas Graham", "https://en.wikipedia.org/wiki/Thomas_Graham_(chemist)", 0.90, "British chemist (1805-1869), FRS"),
    ("G. B. Airy", "Q20018", "George Biddell Airy", "https://en.wikipedia.org/wiki/George_Biddell_Airy", 0.95, "English Astronomer Royal (1801-1892), FRS"),
    ("Benjamin Wilson", "Q817749", "Benjamin Wilson", "https://en.wikipedia.org/wiki/Benjamin_Wilson_(painter)", 0.85, "British painter/scientist (1721-1788), FRS"),
    ("Baden Powell", "Q1071769", "Baden Powell", "https://en.wikipedia.org/wiki/Baden_Powell_(mathematician)", 0.85, "English mathematician (1796-1860), FRS"),
    ("William Heberden", "Q330036", "William Heberden", "https://en.wikipedia.org/wiki/William_Heberden", 0.90, "English physician (1710-1801), FRS"),
    ("James Ferguson", "Q558101", "James Ferguson", "https://en.wikipedia.org/wiki/James_Ferguson_(astronomer)", 0.90, "Scottish astronomer (1710-1776), FRS"),
    ("W. Whewell", "Q333922", "William Whewell", "https://en.wikipedia.org/wiki/William_Whewell", 0.95, "English philosopher (1794-1866), FRS"),
    ("Christopher Wren", "Q170373", "Christopher Wren", "https://en.wikipedia.org/wiki/Christopher_Wren", 0.98, "English architect (1632-1723), FRS President"),
    ("William Borlase", "Q450914", "William Borlase", "https://en.wikipedia.org/wiki/William_Borlase", 0.90, "English antiquarian (1695-1772), FRS"),
    ("Charles Hatchett", "Q378065", "Charles Hatchett", "https://en.wikipedia.org/wiki/Charles_Hatchett", 0.90, "British chemist, FRS"),
    ("William Arderon", "Q15994271", "William Arderon", None, 0.85, "Naturalist, FRS"),
    ("Cromwell Mortimer", "Q5187670", "Cromwell Mortimer", "https://en.wikipedia.org/wiki/Cromwell_Mortimer", 0.90, "British physician, FRS Secretary"),
    ("William Hamilton", "Q15462", "William Hamilton", "https://en.wikipedia.org/wiki/William_Hamilton_(diplomat)", 0.90, "Scottish diplomat/vulcanologist (1730-1803), FRS"),
    ("William Cowper", "Q1047779", "William Cowper", "https://en.wikipedia.org/wiki/William_Cowper_(anatomist)", 0.85, "English surgeon/anatomist, FRS"),
    ("J. W. Lubbock", "Q568443", "John Lubbock, 3rd Baronet", "https://en.wikipedia.org/wiki/Sir_John_Lubbock,_3rd_Baronet", 0.90, "English banker/mathematician (1803-1865), FRS"),
    ("Robert Hooke", "Q46830", "Robert Hooke", "https://en.wikipedia.org/wiki/Robert_Hooke", 0.98, "English natural philosopher (1635-1703), FRS"),
    ("John Flamsteed", "Q242388", "John Flamsteed", "https://en.wikipedia.org/wiki/John_Flamsteed", 0.98, "First Astronomer Royal, FRS"),
    ("Paul Dudley", "Q7150387", "Paul Dudley", "https://en.wikipedia.org/wiki/Paul_Dudley_(jurist)", 0.85, "Colonial Massachusetts chief justice, FRS"),
    ("John Winthrop", "Q3182777", "John Winthrop", "https://en.wikipedia.org/wiki/John_Winthrop_(mathematician)", 0.85, "American astronomer (1714-1779), FRS"),
    ("Charles Darwin", "Q1035", "Charles Darwin", "https://en.wikipedia.org/wiki/Charles_Darwin", 0.98, "English naturalist (1809-1882), FRS"),
    ("John Smeaton", "Q460922", "John Smeaton", "https://en.wikipedia.org/wiki/John_Smeaton", 0.95, "English engineer (1724-1792), FRS"),
    ("James Bradley", "Q312278", "James Bradley", "https://en.wikipedia.org/wiki/James_Bradley", 0.95, "English Astronomer Royal, FRS"),
    ("Henry Oldenburg", "Q700422", "Henry Oldenburg", "https://en.wikipedia.org/wiki/Henry_Oldenburg", 0.95, "German theologian, first Phil Trans editor, FRS Secretary"),
    ("Nehemiah Grew", "Q507790", "Nehemiah Grew", "https://en.wikipedia.org/wiki/Nehemiah_Grew", 0.90, "English plant anatomist (1641-1712), FRS"),
    # Variant spellings/forms for people already added
    ("William Derham", "Q1398235", "William Derham", "https://en.wikipedia.org/wiki/William_Derham", 0.90, "English clergyman (1657-1735), FRS"),
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

        # Check if already exists
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
