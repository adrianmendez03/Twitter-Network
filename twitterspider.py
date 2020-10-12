import sqlite3
import ssl
import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False 
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('twitter.sqlite')
cur = conn.cursor()

cur.execute(''' 
CREATE TABLE IF NOT EXISTS Users
    (id INTEGER PRIMARY KEY, username TEXT UNIQUE, url TEXT UNIQUE, following INTEGER, error INTEGER, old_rank REAL, new_rank REAL)
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER)
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS Websites
    (url TEXT UNIQUE)
''')

# Check to see if we are already in progress ...
cur.execute('SELECT id, url FROM Users WHERE following is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()
if row is not None:
    print("Restarting existing crawl, Remove spider.sqlite to start a fresh crawl")
else:
    username = input("Enter a username to sift through: ")
    starturl = "https://mobile.twitter.com/" + username + "/following"
    if(len(starturl) < 1) : starturl = "https://mobile.twitter.com/Adrianmendez03/following"
    if(starturl.endswith("/")) : starturl = starturl[:-1]
    website = starturl 
    if(starturl.endswith(".htm") or starturl.endswith("html")):
        pos = starturl.find("/")
        website = starturl[:pos]  

    if(len(website) > 1):
        cur.execute("INSERT OR IGNORE INTO Websites (url) VALUES (?)", (website,))
        cur.execute("INSERT OR IGNORE INTO Users (username, url, following, new_rank) VALUES (?, ?, NULL, 1.0)", (username, starturl,))
        conn.commit()

# Get the current user
cur.execute('''SELECT url FROM Users''')
users = list()
for row in cur:
    users.append(str(row[0]))

print(users)

many = 0
while True:
    if(many < 1):
        sval = input('How many pages: ')
        if(len(sval) < 1) : break 
        many = int(sval)
    many = many - 1

    cur.execute('SELECT id, username, url FROM Users WHERE following is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    try: 
        row = cur.fetchone()
        # Print row
        fromid = row[0]
        username = row[1]
        url = row[2]
    except:
        print("No unretrieved users found")
        many = 0 
        break

    print(fromid, username, url, end=" ")

    # If we are retrieving this page there should be no links from it
    cur.execute('DELETE from Links WHERE from_id=?', (fromid,))
    try:
        document = urllib.request.urlopen(url, context=ctx)

        html = document.read()
        if document.getcode() != 200:
            print("Error on Page: ", document.getcode())
            cur.execute('UPDATE Users SET error=? WHERE url=?', (document.getcode(), url))

        if 'text/html' != document.info().get_content_type():
            print("Ignore non text/html page")
            cur.execute('DELETE FROM Users WHERE url=?', (url,))
            conn.commit()
            continue

        soup = BeautifulSoup(html, "html.parser")
        following = soup.findAll("span", ({"class" : "username"}))
        following = following[1:4]
    except KeyboardInterrupt:
        print("")
        print("Program interrupted by user.")
        break
    except:
        print("Unable to retrieve or parse page")
        cur.execute("UPDATE Users SET error-=1 WHERE url=?", (url,))
        conn.commit()
        continue

    cur.execute("INSERT OR IGNORE INTO Users (username, url, following, new_rank) VALUES (?, ?, NULL, 1.0)", (username, url, ))
    cur.execute("UPDATE Users SET following=? WHERE url=?", (len(following), url))
    conn.commit()

    count = 0
    for user in following:
        username = user.contents[1]
        url = "https://mobile.twitter.com/" + username + "/following"
        cur.execute("INSERT OR IGNORE INTO USERS (username, url, following, new_rank) VALUES (?, ?, NULL, 1.0)", (username, url,))
        count += 1
        conn.commit()

        cur.execute("SELECT id FROM Users WHERE url=? LIMIT 1", (url, ))
        try: 
            row = cur.fetchone()
            toid = row[0]
        except:
            print("Could not retrieve id")
            continue
        #print from id, to id
        cur.execute("INSERT OR IGNORE INTO Links (from_id, to_id) VALUES (?, ?)", ( fromid, toid ))

cur.close()
