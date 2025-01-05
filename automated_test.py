import unittest
import os
import sqlite3
import shutil
from Ralfs_Fridvalds_pw6 import main as main_processing_script
from migration_script import main as migration_script

TEST_DB_NAME = "test2.db"
TEST_INPUT_FILE = "mbox-short_test.txt"
TEST_MIGRATIONS_DIR = "migrations_test"
CONFIG_FILE = "config_test.ini"

class TestAutomatedScripts(unittest.TestCase):
    def setUp(self):
        """Set up the test environment."""
        # Create test config.ini
        with open(CONFIG_FILE, "w") as config_file:
            config_file.write(f"""
[database]
db_name = {TEST_DB_NAME}

[file]
input_file = {TEST_INPUT_FILE}
            """)

        # Create a clean test migrations directory
        if os.path.exists(TEST_MIGRATIONS_DIR):
            shutil.rmtree(TEST_MIGRATIONS_DIR)
        os.makedirs(TEST_MIGRATIONS_DIR)

        # Create test SQL migrations
        migration_1 = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
        """
        with open(os.path.join(TEST_MIGRATIONS_DIR, "001_create_test_table.sql"), "w") as f:
            f.write(migration_1)

        migration_2 = """
        CREATE TABLE IF NOT EXISTS email_address (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_address TEXT UNIQUE
        );
        """
        with open(os.path.join(TEST_MIGRATIONS_DIR, "002_create_email_address.sql"), "w") as f:
            f.write(migration_2)

        # Create a test mbox-short.txt file
        with open(TEST_INPUT_FILE, "w") as f:
            f.write("""From user1@example.com Mon
X-DSPAM-Confidence: 0.8475
From user2@example.net Tue
X-DSPAM-Confidence: 0.7654
""")

    def tearDown(self):
        """Clean up after tests."""
        if os.path.exists(TEST_DB_NAME):
            os.remove(TEST_DB_NAME)
        if os.path.exists(TEST_INPUT_FILE):
            os.remove(TEST_INPUT_FILE)
        if os.path.exists(TEST_MIGRATIONS_DIR):
            shutil.rmtree(TEST_MIGRATIONS_DIR)
        if os.path.exists(CONFIG_FILE):
            os.remove(CONFIG_FILE)
        if os.path.exists("app.log"):
            os.remove("app.log")

    def test_migration_script(self):
        """Test the migration script."""
        migration_script()
        connection = sqlite3.connect(TEST_DB_NAME)
        cursor = connection.cursor()

        # Verify migrations were applied
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        self.assertIn("test_table", tables)
        self.assertIn("email_address", tables)

        connection.close()

    def test_main_processing_script(self):
        """Test the main processing script."""
        # Run migrations first
        migration_script()

        # Run the main processing script
        main_processing_script()

        connection = sqlite3.connect(TEST_DB_NAME)
        cursor = connection.cursor()

        # Verify data in email_address table
        cursor.execute("SELECT email_address FROM email_address")
        emails = {row[0] for row in cursor.fetchall()}
        self.assertIn("user1@example.com", emails)
        self.assertIn("user2@example.net", emails)

        # Verify data in domain_name table
        cursor.execute("SELECT domain_name FROM domain_name")
        domains = {row[0] for row in cursor.fetchall()}
        self.assertIn("example.com", domains)
        self.assertIn("example.net", domains)

        # Verify data in spam_confidence_level table
        cursor.execute("SELECT spam_confidence_level FROM spam_confidence_level")
        confidence_levels = [row[0] for row in cursor.fetchall()]
        self.assertIn(0.8475, confidence_levels)
        self.assertIn(0.7654, confidence_levels)

        connection.close()

if __name__ == "__main__":
    unittest.main()
