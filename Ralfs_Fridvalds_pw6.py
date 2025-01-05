import sqlite3
import configparser

# Load configuration variables from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

DB_NAME = config['database']['db_name']
INPUT_FILE = config['file']['input_file']

# Connect to the SQLite database (or create it if it doesn't exist)
connect = sqlite3.connect(DB_NAME)
cursor = connect.cursor()

# Create the email_address table if it doesn't already exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS email_address (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  
    email_address TEXT UNIQUE              
)
""")

# Create the domain_name table if it doesn't already exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS domain_name (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  
    domain_name TEXT UNIQUE                
)
""")

# Create the weekday table if it doesn't already exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS weekday (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  
    weekday TEXT UNIQUE                    
)
""")

# Create the spam_confidence_level table if it doesn't already exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS spam_confidence_level (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  
    spam_confidence_level REAL             
)
""")

# Commit changes to the database
connect.commit()

# Function to extract the domain part of an email address
def get_domain(email):
    return email.split('@')[1]  # Split email by '@' and return the domain part

# Open the input file and process it line by line
with open('mbox-short.txt') as f:
    email_id = None    # Placeholder for the email ID
    domain_id = None   # Placeholder for the domain ID
    weekday_id = None  # Placeholder for the weekday ID
    for line in f:
        # Process lines that start with 'From '
        if line.startswith('From '):
            parts = line.split()  # Split the line into parts
            email = parts[1]      # Extract the email address
            weekday = parts[2]    # Extract the weekday

            # Get the domain from the email
            domain = get_domain(email)

            # Insert the email into the email_address table (if not already present)
            cursor.execute('INSERT OR IGNORE INTO email_address (email_address) VALUES (?)', (email,))
            cursor.execute('SELECT id FROM email_address WHERE email_address = ?', (email,))
            email_id = cursor.fetchone()[0]  # Get the ID of the inserted/retrieved email

            # Insert the domain into the domain_name table (if not already present)
            cursor.execute('INSERT OR IGNORE INTO domain_name (domain_name) VALUES (?)', (domain,))
            cursor.execute('SELECT id FROM domain_name WHERE domain_name = ?', (domain,))
            domain_id = cursor.fetchone()[0]  # Get the ID of the inserted/retrieved domain

            # Insert the weekday into the weekday table (if not already present)
            cursor.execute('INSERT OR IGNORE INTO weekday (weekday) VALUES (?)', (weekday,))
            cursor.execute('SELECT id FROM weekday WHERE weekday = ?', (weekday,))
            weekday_id = cursor.fetchone()[0]  # Get the ID of the inserted/retrieved weekday

        # Process lines that start with 'X-DSPAM-Confidence:'
        if line.startswith('X-DSPAM-Confidence:') and email_id and domain_id and weekday_id:
            # Extract the spam confidence value from the line
            spam_confidence = float(line.split(':')[1].strip())

            # Insert the spam confidence into the spam_confidence_level table
            cursor.execute('INSERT INTO spam_confidence_level (spam_confidence_level) VALUES (?)', (spam_confidence,))
            spam_confidence_id = cursor.lastrowid  # Get the ID of the inserted spam confidence

# Commit changes to the database after processing the file
connect.commit()

# Retrieve and print all unique domain names from the database
cursor.execute('SELECT domain_name FROM domain_name')
domains = cursor.fetchall()

print("Unique Domains:")
for domain in domains:
    print(domain[0])

# Close the database connection
connect.close()
