import sqlite3
import configparser
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set the default logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),  # Log to a file
        logging.StreamHandler()         # Log to the console
    ]
)

logging.info("Starting the application.")

# Load configuration variables from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

DB_NAME = config['database']['db_name']
INPUT_FILE = config['file']['input_file']

# Connect to the SQLite database (or create it if it doesn't exist)
try:
    connect = sqlite3.connect(DB_NAME)
    cursor = connect.cursor()
    logging.info(f"Connected to database: {DB_NAME}")
except sqlite3.Error as e:
    logging.error(f"Database connection error: {e}")
    raise

# Create database tables
tables = {
    "email_address": """
        CREATE TABLE IF NOT EXISTS email_address (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_address TEXT UNIQUE
        )
    """,
    "domain_name": """
        CREATE TABLE IF NOT EXISTS domain_name (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain_name TEXT UNIQUE
        )
    """,
    "weekday": """
        CREATE TABLE IF NOT EXISTS weekday (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            weekday TEXT UNIQUE
        )
    """,
    "spam_confidence_level": """
        CREATE TABLE IF NOT EXISTS spam_confidence_level (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spam_confidence_level REAL
        )
    """
}

for table_name, create_sql in tables.items():
    try:
        cursor.execute(create_sql)
        logging.info(f"Ensured table '{table_name}' exists.")
    except sqlite3.Error as e:
        logging.error(f"Error creating table '{table_name}': {e}")
        raise

connect.commit()

# Function to extract the domain part of an email address
def get_domain(email):
    return email.split('@')[1]  # Split email by '@' and return the domain part

# Open the input file and process it line by line
try:
    with open(INPUT_FILE) as f:
        logging.info(f"Processing file: {INPUT_FILE}")
        email_id = None
        domain_id = None
        weekday_id = None

        for line in f:
            if line.startswith('From '):
                parts = line.split()
                email = parts[1]
                weekday = parts[2]
                domain = get_domain(email)

                # Insert the email
                cursor.execute('INSERT OR IGNORE INTO email_address (email_address) VALUES (?)', (email,))
                cursor.execute('SELECT id FROM email_address WHERE email_address = ?', (email,))
                email_id = cursor.fetchone()[0]
                logging.debug(f"Processed email: {email} (ID: {email_id})")

                # Insert the domain
                cursor.execute('INSERT OR IGNORE INTO domain_name (domain_name) VALUES (?)', (domain,))
                cursor.execute('SELECT id FROM domain_name WHERE domain_name = ?', (domain,))
                domain_id = cursor.fetchone()[0]
                logging.debug(f"Processed domain: {domain} (ID: {domain_id})")

                # Insert the weekday
                cursor.execute('INSERT OR IGNORE INTO weekday (weekday) VALUES (?)', (weekday,))
                cursor.execute('SELECT id FROM weekday WHERE weekday = ?', (weekday,))
                weekday_id = cursor.fetchone()[0]
                logging.debug(f"Processed weekday: {weekday} (ID: {weekday_id})")

            if line.startswith('X-DSPAM-Confidence:') and email_id and domain_id and weekday_id:
                spam_confidence = float(line.split(':')[1].strip())
                cursor.execute('INSERT INTO spam_confidence_level (spam_confidence_level) VALUES (?)', (spam_confidence,))
                logging.debug(f"Inserted spam confidence level: {spam_confidence}")
except FileNotFoundError as e:
    logging.error(f"Input file not found: {e}")
    raise
except Exception as e:
    logging.error(f"Error processing file: {e}")
    raise

# Commit changes to the database
connect.commit()
logging.info("Database changes committed.")

# Retrieve and print all unique domain names from the database
try:
    cursor.execute('SELECT domain_name FROM domain_name')
    domains = cursor.fetchall()
    logging.info(f"Retrieved {len(domains)} unique domains.")
    print("Unique Domains:")
    for domain in domains:
        print(domain[0])
except sqlite3.Error as e:
    logging.error(f"Error retrieving domains: {e}")

# Close the database connection
connect.close()
logging.info("Database connection closed.")
