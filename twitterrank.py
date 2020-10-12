import sqlite3

conn = sqlite3.connect("twitter.sqlite")
cur = conn.cursor()

# Find the ids that send out page rank - we are only interested
# in pages in the SCC that have in and out links
cur.execute("SELECT DISTINCT from_id FROM Links")
from_ids = list()
for row in cur:
    from_ids.append(row[0])

# Find the ids that recieve page rank
to_ids = list()
links = list()
cur.execute("SELECT DISTINCT from_id, to_id FROM Links")
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id : continue
    if from_id not in from_ids : continue
    if to_id not in from_ids : continue
    links.append(row)
    if to_id not in to_ids : to_ids.append(to_id)

# Get latest page ranks for strongly connected component
prev_ranks = dict()
for node in from_ids:
    cur.execute("SELECT new_rank FROM Users WHERE id=?", (node,))
    prev_ranks[node] = row[0]

sval = input("How many iterations: ")
many = 1
if(len(sval) > 0) : many = int(sval)

# Sanity check 
if len(prev_ranks) < 1:
    print("Nothing to rank. Check Data.")
    quit()

# Lets do User Rank in memory so it is really fast
for i in range(many):
    # Print prev_ranks.items()[:5]
    next_ranks = dict()
    total = 0.0
    for (node, old_rank) in list(prev_ranks.items()):
        total = total + old_rank 
        next_ranks[node] = 0.0
    
    # Find the number of outbound links and set the user rank down each
    for (node, old_rank) in list(prev_ranks.items()):
        give_ids = list()
        for (from_id, to_id) in links:
            if from_id != node : continue
            if to_id not in to_ids : continue
            give_ids.append(to_id)
        if(len(give_ids) < 1) : continue
        amount = old_rank / len(give_ids)

        for id in give_ids:
            next_ranks[id] = next_ranks[id] + amount

    newtotal = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtotal = newtotal + next_rank
    evap = (total - newtotal) / len(next_ranks)

    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap

    newtotal =  0 
    for (node, next_rank) in list(next_ranks.items()):
        newtotal = newtotal + next_rank

    # Compute the per-page average change from old rank to new rank 
    # As indication of convergence of the algorithm

    totaldiff = 0 
    for  (node, old_rank) in list(prev_ranks.items()):
        new_rank = next_ranks[node]
        diff = abs(old_rank - new_rank)
        totaldiff = totaldiff + diff

    avgdiff = totaldiff / len(prev_ranks)
    print(i + 1, avgdiff)

    # rotate 
    prev_ranks = next_ranks

# Put the final ranks back into the database 
print(list(next_ranks.items())[:5])
cur.execute("UPDATE Users SET old_rank=new_rank")
for (id, new_rank) in list(next_ranks.items()):
    cur.execute("UPDATE Users SET new_rank=? WHERE id=?", (new_rank, id))
conn.commit()
cur.close()

