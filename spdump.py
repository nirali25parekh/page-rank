# import necessary packages
import sqlite3

# connect to database and get cursor
conn = sqlite3.connect("spider.sqlite")
cur = conn.cursor()

cur.execute(
    """SELECT COUNT(from_id) AS inbound, old_rank, new_rank, id, url 
     FROM Pages JOIN Links ON Pages.id = Links.to_id
     WHERE html IS NOT NULL
     GROUP BY id ORDER BY inbound DESC"""
)

# Pages.id = from_id from links
count = 0
for row in cur:
    if count < 50:
        print(row)
    count = count + 1
# ~ printing order -> count(from_id) , old_rank , new_rank , id , url
print(count, "rows.")
cur.close()
