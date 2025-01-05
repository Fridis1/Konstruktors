import sqlite3
connect = sqlite3.connect("test.db")
cursor = connect.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS email_address (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email_address TEXT UNIQUE
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS domain_name (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_name TEXT UNIQUE
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS weekday (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    weekday TEXT UNIQUE
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS spam_confidence_level (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    spam_confidence_level REAL
)
""")
connect.commit()

def get_domain(email):
    return email.split('@')[1]

with open('mbox-short.txt') as f:
    email_id = None
    domain_id = None
    weekday_id = None
    for line in f:
        if line.startswith('From '):
            parts = line.split()
            email = parts[1]
            weekday = parts[2]
            
            domain = get_domain(email)
            
            cursor.execute('INSERT OR IGNORE INTO email_address (email_address) VALUES (?)', (email,))
            cursor.execute('SELECT id FROM email_address WHERE email_address = ?', (email,))
            email_id = cursor.fetchone()[0]
            
            cursor.execute('INSERT OR IGNORE INTO domain_name (domain_name) VALUES (?)', (domain,))
            cursor.execute('SELECT id FROM domain_name WHERE domain_name = ?', (domain,))
            domain_id = cursor.fetchone()[0]
            
            cursor.execute('INSERT OR IGNORE INTO weekday (weekday) VALUES (?)', (weekday,))
            cursor.execute('SELECT id FROM weekday WHERE weekday = ?', (weekday,))
            weekday_id = cursor.fetchone()[0]
        
        if line.startswith('X-DSPAM-Confidence:') and email_id and domain_id and weekday_id:
            spam_confidence = float(line.split(':')[1].strip())
            
            cursor.execute('INSERT INTO spam_confidence_level (spam_confidence_level) VALUES (?)', (spam_confidence,))
            spam_confidence_id = cursor.lastrowid
            
connect.commit()


cursor.execute('SELECT domain_name FROM domain_name')
domains = cursor.fetchall()
 
print("Unique Domains:")
for domain in domains:
    print(domain[0])
connect.close()