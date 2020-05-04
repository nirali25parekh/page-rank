# Import necessary packages
import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Connect to database, and get cursor object
conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

# Create Table
# ~ TableName:- Pages
# ~ Column Names:-
# ~ id : primary key
# ~ url : the link
# ~ html : entire html of the page
# ~ error : whether error occurs or not
# ~ old_rank , new_rank : for page rank algo calculations
cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')

# Create Table (many to many table)
# ~ TableName:- Links
# ~ Column Names:-
# ~ from_id : link in Pages table
# ~ to_id : link in Pages Table
cur.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER)''')

# Create Table
# ~ TableName:- Websites
# ~ (assuming we have more than one websites, in our case this table is usless only)
cur.execute('''CREATE TABLE IF NOT EXISTS Websites (url TEXT UNIQUE)''')

# Fetch one row from the table Pages
cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()

# Check to see if we are already in progress
if row is not None:
    print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")

# If started for the first time
else :
    starturl = input('Enter web url or enter: ')
    if ( len(starturl) < 1 ) : starturl = 'http://www.dr-chuck.com/'    # if no input
    if ( starturl.endswith('/') ) : starturl = starturl[:-1]    # if http://www.abc.com/, then starturl = http://www.abc.com
    website = starturl
    if ( starturl.endswith('.htm') or starturl.endswith('.html') ) :     # if http://www.abc.com/file.html 
        pos = starturl.rfind('/')   # last occurance of '/'
        website = starturl[:pos]        # website = http://www.abc.com

    # if website is not empty string,
    # put into Table Websites
    # ~column url <= website  (eg. http://www.abc.com)
    # put into Table Pages
    # ~column url <= starturl (eg. http://www.abc.com)
    # ~column html <= NULL
    # ~column new_rank <= 1.0
    if ( len(website) > 1 ) :
        cur.execute('INSERT OR IGNORE INTO Websites (url) VALUES ( ? )', ( website, ) )
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( starturl, ) )
        conn.commit()

# Get the current websites (in the first run, only 1 url)
cur.execute('''SELECT url FROM Websites''')
websites = list()
for row in cur:
    websites.append(str(row[0]))

print('list of websites: ', websites)

many = 0
while True:
    if ( many < 1 ) :
        sval = input('How many pages:')
        if ( len(sval) < 1 ) : break    # if no input given, exit program
        many = int(sval)
    many = many - 1

    # fetch one url from Pages table (in first run, only 1 link is there, the input url)
    cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    try:
        row = cur.fetchone()
        fromid = row[0]     # put Page.id => Links.fromid
        url = row[1]        # put the url in url
        print('fromid:', fromid , '\turl: ', url, end='\t')
    except:
        print('No unretrieved HTML pages found')
        many = 0
        break

    # If we are retrieving this page, there should be no links from it
    cur.execute('DELETE from Links WHERE from_id=?', (fromid, ) )
    try:
        document = urlopen(url, context=ctx)

        html = document.read()
        # document throws non 200 code
        if document.getcode() != 200 :
            print("Error on page: ",document.getcode())
            cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url) )

        # BeautifulSoup can only parse html pages, so check if no jpeg or something
        if 'text/html' != document.info().get_content_type() :
            print("Ignore non text/html page")
            cur.execute('DELETE FROM Pages WHERE url=?', ( url, ) )
            conn.commit()
            continue

        print('length of html = ', str(len(html)), end='\t')

        # parse html through beautiful soup
        soup = BeautifulSoup(html, "html.parser")

    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break
    # some error in accessing url
    except:
        print("Unable to retrieve or parse page")
        cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
        conn.commit()
        continue

    #~ If NO ERROR TILL NOW, put the page url and html into Pages table.
    cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( url, ) )
    cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url ) )
    conn.commit()

    # Retrieve all of the anchor tags
    tags = soup('a')
    count = 0
    for tag in tags:
        href = tag.get('href', None)   # get urls from href attribute
        if ( href is None ) : continue

        # Resolve relative references like href="/contact"
        up = urlparse(href)
        if ( len(up.scheme) < 1 ) :
            href = urljoin(url, href)

        # if '#' on first position, means internal link to same page
        ipos = href.find('#')
        if ( ipos > 1 ) : href = href[:ipos]

        if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) : continue       # if media files
        if ( href.endswith('/') ) : href = href[:-1]    # http://www.xyz.com/    => http://www.xyz.com
        if ( len(href) < 1 ) : continue

		# Check if the URL is in any of the websites
        found = False
        for website in websites:
            if ( href.startswith(website) ) :
                found = True
                break
        if not found : continue

        #~ put the href url into Pages table (making a queue of all urls)
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( href, ) )
        count = count + 1
        conn.commit()

        # select that href that we just added and get its id
        cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
        try:
            row = cur.fetchone()
            toid = row[0]
        except:
            print('Could not retrieve id')
            continue
        # put the toid and fromid in Links Table
        cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( fromid, toid ) )
        
    print('count' , count)

cur.close()
