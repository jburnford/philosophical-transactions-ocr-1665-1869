#!/usr/bin/env python3
"""Insert confirmed author-Wikidata matches into the database."""

import sqlite3

DB_PATH = "jstor_metadata.db"

# Matches found via MCP Wikidata tools
# Format: (author_name, qid, wikidata_label, wikipedia_url, confidence, match_reason)
MATCHES = [
    ("Everard Home", "Q969604", "Everard Home", "https://en.wikipedia.org/wiki/Everard_Home", 0.95, "British surgeon (1756-1832), FRS"),
    ("William Herschel", "Q14277", "William Herschel", "https://en.wikipedia.org/wiki/William_Herschel", 0.95, "German-born British astronomer (1738-1822), FRS"),
    ("Joseph Banks", "Q153408", "Joseph Banks", "https://en.wikipedia.org/wiki/Joseph_Banks", 0.95, "English naturalist (1743-1820), FRS President"),
    ("Hans Sloane", "Q310326", "Hans Sloane", "https://en.wikipedia.org/wiki/Hans_Sloane", 0.95, "Irish botanist (1660-1753), FRS President"),
    ("Nevil Maskelyne", "Q450757", "Nevil Maskelyne", "https://en.wikipedia.org/wiki/Nevil_Maskelyne", 0.95, "Astronomer Royal (1732-1811), FRS"),
    ("J. T. Desaguliers", "Q658008", "John Theophilus Desaguliers", "https://en.wikipedia.org/wiki/John_Theophilus_Desaguliers", 0.90, "French-born British natural philosopher (1683-1744), FRS"),
    ("Henry Baker", "Q928236", "Henry Baker", "https://en.wikipedia.org/wiki/Henry_Baker_(naturalist)", 0.90, "English naturalist (1698-1774), FRS"),
    ("Edward Sabine", "Q311223", "Edward Sabine", "https://en.wikipedia.org/wiki/Edward_Sabine", 0.95, "British Army general, astronomer (1788-1883), FRS"),
    ("William Watson", "Q462269", "William Watson", "https://en.wikipedia.org/wiki/William_Watson_(scientist)", 0.90, "English physician and scientist (1715-1787), FRS"),
    ("Michael Faraday", "Q8750", "Michael Faraday", "https://en.wikipedia.org/wiki/Michael_Faraday", 0.98, "British scientist (1791-1867), FRS"),
    ("James Parsons", "Q6140971", "James Parsons", "https://en.wikipedia.org/wiki/James_Parsons_(physician)", 0.85, "English physician and antiquarian (1705-1770), FRS"),
    ("Humphry Davy", "Q131761", "Humphry Davy", "https://en.wikipedia.org/wiki/Humphry_Davy", 0.98, "British chemist (1778-1829), FRS"),
    ("William Hyde Wollaston", "Q312975", "William Hyde Wollaston", "https://en.wikipedia.org/wiki/William_Hyde_Wollaston", 0.95, "English chemist and physicist (1766-1828), FRS"),
    ("David Brewster", "Q168468", "David Brewster", "https://en.wikipedia.org/wiki/David_Brewster", 0.95, "Scottish astronomer and mathematician (1781-1868), FRS"),
    ("Martin Lister", "Q513593", "Martin Lister", "https://en.wikipedia.org/wiki/Martin_Lister", 0.90, "English naturalist and physician (1639-1712), FRS"),
    ("Fr. Hauksbee", "Q1378752", "Francis Hauksbee", "https://en.wikipedia.org/wiki/Francis_Hauksbee", 0.90, "British physicist (1660-1713), FRS"),
    ("W. Derham", "Q1398235", "William Derham", "https://en.wikipedia.org/wiki/William_Derham", 0.90, "English clergyman and natural philosopher (1657-1735), FRS"),
    ("Robert Boyle", "Q43393", "Robert Boyle", "https://en.wikipedia.org/wiki/Robert_Boyle", 0.98, "Anglo-Irish natural philosopher (1627-1691), FRS founding member"),
    ("Arthur Cayley", "Q159430", "Arthur Cayley", "https://en.wikipedia.org/wiki/Arthur_Cayley", 0.95, "English mathematician (1821-1895), FRS"),
    ("John Wallis", "Q208359", "John Wallis", "https://en.wikipedia.org/wiki/John_Wallis", 0.95, "English mathematician (1616-1703), FRS founding member"),
    ("John Ellis", "Q934747", "John Ellis", "https://en.wikipedia.org/wiki/John_Ellis_(naturalist)", 0.90, "Linen merchant, botanist (1710-1776), FRS"),
    ("James Short", "Q957681", "James Short", "https://en.wikipedia.org/wiki/James_Short_(mathematician)", 0.90, "British mathematician (1710-1768), FRS"),
    ("John Pringle", "Q1348255", "John Pringle", "https://en.wikipedia.org/wiki/Sir_John_Pringle,_1st_Baronet", 0.90, "Scottish physician (1707-1782), FRS President"),
    ("John Hunter", "Q505981", "John Hunter", "https://en.wikipedia.org/wiki/John_Hunter_(surgeon)", 0.90, "Scottish surgeon (1728-1793), FRS"),
    ("John Davy", "Q1699774", "John Davy", "https://en.wikipedia.org/wiki/John_Davy_(chemist)", 0.90, "British physician and chemist (1790-1868), FRS"),
    ("Anthony Van Leeuwenhoek", "Q43522", "Antonie van Leeuwenhoek", "https://en.wikipedia.org/wiki/Antonie_van_Leeuwenhoek", 0.95, "Dutch scientist, Father of Microbiology (1632-1723), FRS"),
    ("E. Halley", "Q47434", "Edmond Halley", "https://en.wikipedia.org/wiki/Edmond_Halley", 0.95, "English astronomer (1656-1742), FRS"),
    ("Edmund Halley", "Q47434", "Edmond Halley", "https://en.wikipedia.org/wiki/Edmond_Halley", 0.95, "English astronomer (1656-1742), FRS"),
    ("Edm. Halley", "Q47434", "Edmond Halley", "https://en.wikipedia.org/wiki/Edmond_Halley", 0.95, "English astronomer (1656-1742), FRS"),
    ("Professor Owen", "Q151556", "Richard Owen", "https://en.wikipedia.org/wiki/Richard_Owen", 0.90, "English biologist and paleontologist (1804-1892), FRS"),
    ("James Petiver", "Q1680975", "James Petiver", "https://en.wikipedia.org/wiki/James_Petiver", 0.90, "British biologist and pharmacist (1663-1718), FRS"),
    ("Henry Cavendish", "Q131733", "Henry Cavendish", "https://en.wikipedia.org/wiki/Henry_Cavendish", 0.98, "English natural philosopher (1731-1810), FRS"),
    ("Charles Blagden", "Q1063743", "Charles Blagden", "https://en.wikipedia.org/wiki/Charles_Blagden", 0.90, "British physician and scientist (1748-1820), FRS Secretary"),
    ("Joseph Priestley", "Q159636", "Joseph Priestley", "https://en.wikipedia.org/wiki/Joseph_Priestley", 0.98, "English chemist and theologian (1733-1804), FRS"),
    ("Stephen Gray", "Q315197", "Stephen Gray", "https://en.wikipedia.org/wiki/Stephen_Gray_(scientist)", 0.90, "British astronomer (1666-1736), FRS"),
    ("Thomas Andrew Knight", "Q940986", "Thomas Andrew Knight", "https://en.wikipedia.org/wiki/Thomas_Andrew_Knight", 0.90, "British botanist (1759-1838), FRS"),
    ("Peter Collinson", "Q1350818", "Peter Collinson", "https://en.wikipedia.org/wiki/Peter_Collinson_(botanist)", 0.90, "English botanist (1694-1768), FRS"),
    ("Tiberius Cavallo", "Q1398771", "Tiberius Cavallo", "https://en.wikipedia.org/wiki/Tiberius_Cavallo", 0.90, "Italian physicist (1749-1809), FRS"),
    ("John Pond", "Q742464", "John Pond", "https://en.wikipedia.org/wiki/John_Pond", 0.90, "British Astronomer Royal (1767-1836), FRS"),
    ("John Huxham", "Q686218", "John Huxham", "https://en.wikipedia.org/wiki/John_Huxham", 0.90, "English surgeon (1692-1768), FRS"),
    # Additional variations
    ("Ralph Thoresby", "Q7288161", "Ralph Thoresby", "https://en.wikipedia.org/wiki/Ralph_Thoresby", 0.90, "British historian (1658-1725), FRS"),
]

def get_author_stats(conn, author_name):
    """Get publication stats for an author."""
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
    for author_name, qid, label, wiki_url, confidence, reason in MATCHES:
        stats = get_author_stats(conn, author_name)
        if stats[0] is None:
            print(f"Author not found: {author_name}")
            continue

        first_pub, last_pub, article_count = stats

        conn.execute("""
            INSERT OR REPLACE INTO author_wikidata
            (author_name, first_pub_year, last_pub_year, article_count,
             qid, wikidata_label, wikipedia_url, confidence, match_reason,
             candidates_json, reviewed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', 1)
        """, (author_name, first_pub, last_pub, article_count,
              qid, label, wiki_url, confidence, reason))
        inserted += 1
        print(f"Inserted: {author_name} -> {qid} ({label})")

    conn.commit()

    # Print stats
    total = conn.execute("SELECT COUNT(*) FROM author_wikidata").fetchone()[0]
    matched = conn.execute("SELECT COUNT(*) FROM author_wikidata WHERE qid IS NOT NULL").fetchone()[0]

    print(f"\n=== Summary ===")
    print(f"Inserted: {inserted}")
    print(f"Total in table: {total}")
    print(f"With QID: {matched}")

    conn.close()

if __name__ == "__main__":
    main()
