#  Import packages
import sqlite3

#  Connect to database and make cursor
conn = sqlite3.connect("spider.sqlite")
cur = conn.cursor()

# Find the ids that send out page rank - we only are interested
# in pages in the SCC that have in and out links
cur.execute("""SELECT DISTINCT from_id FROM Links""")
# from_ids array consists of all the distinct from ids
from_ids = list()  # ~ contains (from_id) array
for row in cur:
    from_ids.append(row[0])

# Find the ids that receive page rank
to_ids = list()  # ~ contains (to_id) array
links = list()  # ~ conatins (from_id, to_id) array
cur.execute("""SELECT DISTINCT from_id, to_id FROM Links""")
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id:
        continue  # We don't want links that goto and come from same url
    if from_id not in from_ids:
        continue  # We don't want links that are not in from_ids array
    if to_id not in from_ids:
        continue  # We don't want links that are not already retrieved
    links.append(row)
    if to_id not in to_ids:
        to_ids.append(to_id)

# Get latest page ranks for strongly connected component
# ~ loop through each from id, fetch new_rank from Pages Table, put in dict @ key: from_id, value: new_rank
# ~ means dict[Pages.id] = Pages.new_rank
prev_ranks = dict()  # ~ prev_ranks will hold { from_id : current_rank}
for node in from_ids:
    cur.execute("""SELECT new_rank FROM Pages WHERE id = ?""", (node,))
    row = cur.fetchone()
    prev_ranks[node] = row[0]

# How many times recalculate?
sval = input("How many iterations:")
many = 1
if len(sval) > 0:
    many = int(sval)

# Sanity check
if len(prev_ranks) < 1:
    print("Nothing to page rank.  Check data.")
    quit()

# Lets do Page Rank in memory so it is really fast
for i in range(many):
    next_ranks = dict()  # ~ next_ranks dict will contain { from_id : new_rank }
    total = 0.0
    for (node, old_rank) in list(
        prev_ranks.items()
    ):  # (prev_ranks[from_id] = new_rank)
        total = total + old_rank  # summing up all ranks
        next_ranks[node] = 0.0  # put next_ranks[from_id] = 0.0

    # Find the number of outbound links and sent the page rank down each
    # ~ Loop through each from_ids in our list, then goto our many to many table and take those particular to_ids that they point to.
    # ~ Then, increase ranks of those ids.
    for (node, old_rank) in list(
        prev_ranks.items()
    ):  # (prev_ranks[from_id] = new_rank)
        give_ids = list()  # give_ids will contain to_ids of those particular from_id
        for (from_id, to_id) in links:
            if from_id != node:
                continue  # We want the from_id from Links many to many table that matches our node wala from_id

            if to_id not in to_ids:
                continue  # We don't want a to_id if we had filtered it out before
            give_ids.append(to_id)
            # ~ now give_ids will contain all the to_ids for that particular from_id

        if len(give_ids) < 1:
            continue  # if no to_id found for that from_id
        amount = old_rank / len(
            give_ids
        )  # eg amount = 1.0/3 = 0.3333 (if that from_id has 3 to_ids)

        # all those to_ids, add the amount, hence score got better
        for id in give_ids:
            next_ranks[id] = (
                next_ranks[id] + amount
            )  # eg. next_rank[myId] = 0 + 0.3333 = 0.33333

    # ~ Now, next_ranks conatins dict of { from_id: new_tentative_rank } for all from_ids
    newtot = 0
    for (node, next_rank) in list(
        next_ranks.items()
    ):  # eg. here, node => current from_id & next_rank => 0.3333
        newtot = newtot + next_rank  # eg. newtot = 0 + 0.333 = 0.333
    evap = (total - newtot) / len(next_ranks)  # eg. evap = (20 - 0.333)/(8) = 2.809

    for node in next_ranks:
        next_ranks[node] = (
            next_ranks[node] + evap
        )  # eg. next_ranks[myId] = 0.333 + 2.809 = 3.142

    newtot = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtot = newtot + next_rank  # eg. newtot = 0 + 3.142 = 3.142

    # Compute the per-page average change from old rank to new rank
    # As indication of convergence of the algorithm
    # ~ as iterations go on, difference gets smaller and smaller
    totdiff = 0
    for (node, old_rank) in list(prev_ranks.items()):
        new_rank = next_ranks[node]
        diff = abs(old_rank - new_rank)  # eg. diff = abs(1-3.142) = abs(-2.142) = 2.142
        totdiff = totdiff + diff  # eg. totdiff = totdiff + 2.142

    avediff = totdiff / len(prev_ranks)  # eg. avediff = totdiff/len(from_ids)

    # rotate for the next iteration, put next_ranks as the current_ranks
    prev_ranks = next_ranks

# Put the current ranks in old_ranks
cur.execute("""UPDATE Pages SET old_rank=new_rank""")

# Put the final ranks back into the database
for (id, new_rank) in list(next_ranks.items()):
    cur.execute("""UPDATE Pages SET new_rank=? WHERE id=?""", (new_rank, id))
conn.commit()
cur.close()
