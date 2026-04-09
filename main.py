import re
import sqlite3

# reading and parsing a file (Just reading the entire file in one line) 
raw = open("task1_d.json").read()

# the function turns a string into a Python dictionary
def parse_ruby_record(record_str: str) -> dict:
    result = {}
    # the first RegEx takes the lines, pattern :key=>"value"
    for m in re.finditer(r':(\w+)=>"([^"]*)"', record_str):
        result[m.group(1)] = m.group(2)
    # the second RegEx takes the numbers, pattern :key=>"number"
    for m in re.finditer(r':(\w+)=>(\d+)', record_str):
        key = m.group(1)
        # if the key is not yet in the dictionary, save it as an integer.
        if key not in result:
            result[key] = int(m.group(2))
    return result

# splitting the overall row into separate entries using the separator "}, {"
record_strings = re.split(r'\},\s*\{', raw.strip().lstrip('[').rstrip(']'))

records = []
for rs in record_strings:
    rec = parse_ruby_record(rs)
    if rec:
        records.append(rec)

print(f"Parsed {len(records)} records") # output of the number of successfully processed records

# the database file is created here and the structure of the main books.db table is determined
conn = sqlite3.connect("books.db") # creating a connection to the database file
cur = conn.cursor() # cursor object for executing SQL commands

# recreating the table so that the data is not duplicated when running it again
cur.execute("DROP TABLE IF EXISTS books")
cur.execute("""
    CREATE TABLE books (
        id        TEXT,
        title     TEXT,
        author    TEXT,
        genre     TEXT,
        publisher TEXT,
        year      INTEGER,
        price     TEXT -- stored the price as a string, because in the data it comes with the currency symbol ($ or €), we will do the conversion later in SQL
    )
""")

# passing through the dictionary list and insert each entry into the SQL table
for r in records:
    cur.execute("""
        INSERT INTO books (id, title, author, genre, publisher, year, price)
        VALUES (?,?,?,?,?,?,?)
    """, (
        str(r.get("id")),
        r.get("title"),
        r.get("author"),
        r.get("genre"),
        r.get("publisher"),
        r.get("year"),
        r.get("price"),
    ))
 
conn.commit() # fixing changes in the database
raw_count = cur.execute("SELECT COUNT(*) FROM books").fetchone()[0]
print(f"Loaded {raw_count} rows into `books`")

# the script creates a new table based on the data from the first one, converting currencies
cur.execute("DROP TABLE IF EXISTS year_summary")
cur.execute("""
    CREATE TABLE year_summary AS
    SELECT
        year AS publication_year, -- grouping by year
        COUNT(*) AS book_count, -- counting the number of books this year
        ROUND(
            AVG(
                CASE
                    WHEN price LIKE '$%' THEN CAST(SUBSTR(price,2) AS REAL) -- if the price is in dollars, we cut off the $ and convert it to a number
                    WHEN price LIKE '€%' THEN CAST(SUBSTR(price,2) AS REAL) * 1.2 -- if it is in euros, we convert it to dollars at the rate of 1.2
                    ELSE NULL
                END
            ), 2
        ) AS average_price -- calculating the average price, rounding up to 2 digits
    FROM books
    WHERE year IS NOT NULL
    GROUP BY year -- grouping
    ORDER BY year -- sorting
""")
conn.commit()
 
summary_count = cur.execute("SELECT COUNT(*) FROM year_summary").fetchone()[0]
print(f"Built `year_summary` with {summary_count} rows\n")

# formatting
print("=" * 50)
print(f"TABLE: books  ({raw_count} rows)")
print("=" * 50)
print(f"TABLE: year_summary  ({summary_count} rows)")
print("=" * 50)
print(f"{'publication_year':>16}  {'book_count':>10}  {'average_price':>13}")
print("-" * 44)
rows = cur.execute("SELECT publication_year, book_count, average_price FROM year_summary ORDER BY publication_year").fetchall()
for row in rows:
    print(f"{row[0]:>16}  {row[1]:>10}  {row[2]:>13.2f}")
 
conn.close() # closing the connection to the database
print("\nDone. Database saved to /home/claude/books.db")