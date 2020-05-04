import sqlite3

# Connect to our database
conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

# Reset all the ranks back to 1.0
cur.execute('''UPDATE Pages SET new_rank=1.0, old_rank=0.0''')
conn.commit()

cur.close()

print("All pages set to a rank of 1.0")
