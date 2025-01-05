import sqlite3
import os
import logging
import configparser

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)

# Read configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Get database name and migrations directory
DB_NAME = config['database']['db_name']
MIGRATIONS_DIR = 'migrations'  # Default folder for migrations

def connect_to_database(db_name):
    """Connect to the SQLite database and return the connection and cursor."""
    try:
        connection = sqlite3.connect(db_name)
        cursor = connection.cursor()
        logging.info(f"Connected to database: {db_name}")
        return connection, cursor
    except sqlite3.Error as e:
        logging.error(f"Database connection error: {e}")
        raise

def ensure_migration_table(cursor):
    """Create the schema_migrations table if it doesn't exist."""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS schema_migrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT UNIQUE
    )
    """)
    logging.info("Ensured 'schema_migrations' table exists.")

def get_applied_migrations(cursor):
    """Fetch all applied migrations from the schema_migrations table."""
    cursor.execute("SELECT filename FROM schema_migrations")
    applied_migrations = {row[0] for row in cursor.fetchall()}
    logging.debug(f"Applied migrations: {applied_migrations}")
    return applied_migrations

def apply_migration(cursor, connection, migration_file):
    """Apply a single migration."""
    logging.info(f"Applying migration: {migration_file}")
    migration_path = os.path.join(MIGRATIONS_DIR, migration_file)
    try:
        with open(migration_path, 'r') as file:
            sql_script = file.read()
            cursor.executescript(sql_script)
            cursor.execute("INSERT INTO schema_migrations (filename) VALUES (?)", (migration_file,))
            connection.commit()
            logging.info(f"Migration applied successfully: {migration_file}")
    except Exception as e:
        logging.error(f"Error applying migration {migration_file}: {e}")
        connection.rollback()
        raise

def apply_all_migrations(cursor, connection):
    """Apply all unapplied migrations."""
    applied_migrations = get_applied_migrations(cursor)
    if not os.path.exists(MIGRATIONS_DIR):
        logging.error(f"Migrations directory '{MIGRATIONS_DIR}' does not exist.")
        raise FileNotFoundError(f"Migrations directory '{MIGRATIONS_DIR}' does not exist.")
        
    migration_files = sorted(f for f in os.listdir(MIGRATIONS_DIR) if f.endswith('.sql'))

    for migration_file in migration_files:
        if migration_file not in applied_migrations:
            apply_migration(cursor, connection, migration_file)

def main():
    """Main script to perform migrations."""
    connection, cursor = connect_to_database(DB_NAME)
    ensure_migration_table(cursor)
    apply_all_migrations(cursor, connection)
    connection.close()
    logging.info("All migrations applied and database connection closed.")

if __name__ == "__main__":
    main()
