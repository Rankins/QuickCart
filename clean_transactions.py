import json
import pandas as pd
import psycopg2
from datetime import datetime
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, ConnectionFailure

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Connect to PostgreSQL database
conn = psycopg2.connect(dbname="quickcart", user="postgres", password="postgres", host="localhost")
cursor = conn.cursor()
logger.info("Connected to PostgreSQL quickcart database")

# Connect to MongoDB for archival
try:
    mongo_client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ping')
    mongo_db = mongo_client['QuickCart']
    mongo_collection = mongo_db['raw_transaction_logs']
    # Create index on event_id for uniqueness and query performance
    mongo_collection.create_index('event.id', unique=False)
    logger.info("Connected to MongoDB QuickCart database")
except ConnectionFailure as e:
    logger.warning(f"MongoDB connection failed: {e}. Continuing without MongoDB archival.")
    mongo_client = None
    mongo_collection = None

# Function to clean and process raw logs
def process_raw_logs(file_path):
    """Process raw transaction logs from JSONL file with error handling."""
    logs = []
    skipped = 0
    
    logger.info(f"Processing raw logs from {file_path}")
    
    try:
        with open(file_path, 'r') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    if not line.strip():
                        continue
                    
                    log = json.loads(line.strip())
                    
                    # Extract necessary fields and standardize currency formats
                    amount = log.get('payload', {}).get('Amount', None)
                    if amount is not None:
                        amount_usd = standardize_currency(amount)
                        if amount_usd is not None:
                            log['payload']['Amount'] = amount_usd
                            logs.append(log)
                        else:
                            logger.warning(f"Line {line_num}: Could not convert amount '{amount}' to float, setting to 0.0")
                            log['payload']['Amount'] = 0.0
                            logs.append(log)
                    else:
                        logger.warning(f"Line {line_num}: Missing Amount field")
                        skipped += 1
                except json.JSONDecodeError as e:
                    logger.warning(f"Line {line_num}: JSON decode error - {e}")
                    skipped += 1
                except Exception as e:
                    logger.warning(f"Line {line_num}: Unexpected error - {e}")
                    skipped += 1
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return logs
    
    logger.info(f"Processed {len(logs)} valid logs, skipped {skipped} records")
    return logs

# Convert various currency formats to standardized USD float.
def standardize_currency(amount):
    
    try:
        if amount is None or amount == '':
            return 0.0
        
        if isinstance(amount, (int, float)):
            return float(amount) if amount >= 0 else 0.0
        
        if isinstance(amount, str):
            # Remove common currency symbols, codes, and formatting
            amount = amount.strip()
            # Remove currency codes (USD, EUR, GBP, etc.) 
            import re
            amount = re.sub(r'^[A-Z]{3}\s+', '', amount)
            # Remove currency symbols
            amount = amount.replace('$', '').replace('€', '').replace('£', '')
            amount = amount.replace(',', '').replace(' ', '')
            
            if amount:
                converted = float(amount)
                return converted if converted >= 0 else 0.0
            return 0.0
        
        return 0.0
    except (ValueError, TypeError):
        return None

# Archive raw logs to MongoDB
def archive_to_mongodb(logs, collection):
    """Archive raw transaction logs to MongoDB for long-term archival."""
    if not collection:
        logger.warning("MongoDB collection not available, skipping archival")
        return 0
    
    if not logs:
        logger.warning("No logs to archive")
        return 0
    
    inserted = 0
    duplicates = 0
    failed = 0
    
    logger.info(f"Archiving {len(logs)} logs to MongoDB")
    
    try:
        # Attempt bulk insert
        result = mongo_collection.insert_many(logs, ordered=False)
        inserted = len(result.inserted_ids)
        logger.info(f"Successfully archived {inserted} logs to MongoDB")
    except Exception as e:
        logger.warning(f"Bulk insert failed: {e}. Attempting individual inserts...")
        
        # Fall back to individual inserts to capture partial success
        for idx, log in enumerate(logs):
            try:
                # Use event_id as unique identifier for upsert
                event_id = log.get('event', {}).get('id')
                if event_id:
                    mongo_collection.update_one(
                        {'event.id': event_id},
                        {'$set': log},
                        upsert=True
                    )
                    inserted += 1
                else:
                    # No event_id, insert without upsert
                    mongo_collection.insert_one(log)
                    inserted += 1
            except DuplicateKeyError:
                duplicates += 1
                logger.debug(f"Log {idx}: Duplicate event_id, already archived")
            except Exception as e:
                failed += 1
                logger.warning(f"Log {idx}: Failed to archive - {e}")
    
    logger.info(f"MongoDB archival complete: {inserted} inserted, {duplicates} duplicates, {failed} failed")
    return inserted

# Process the raw data
logs = process_raw_logs('scriptt/output_dir/raw_data.jsonl')

if not logs:
    logger.error("No valid logs to process")
    cursor.close()
    conn.close()
    if mongo_client:
        mongo_client.close()
    exit(1)

# Archive raw logs to MongoDB for long-term storage
if mongo_collection:
    archive_to_mongodb(logs, mongo_collection)
else:
    logger.warning("Skipping MongoDB archival - collection unavailable")

# Convert logs to DataFrame for PostgreSQL processing
df = pd.DataFrame(logs)
logger.info(f"Created DataFrame with {len(df)} records")

# Insert into PostgreSQL with error handling
inserted = 0
failed = 0

logger.info("Inserting standardized data into PostgreSQL")

try:
    for idx, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO raw_transaction_logs (event_id, order_id, payment_id, amount_usd, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['event']['id'], row['entity']['order']['id'], row['entity']['payment']['id'], 
                  row['payload']['Amount'], row['payload']['status'], row['event']['ts']))
            inserted += 1
        except Exception as e:
            failed += 1
            logger.warning(f"Row {idx}: Failed to insert - {e}")
    
    conn.commit()
    logger.info(f"PostgreSQL insertion complete: {inserted} inserted, {failed} failed")
    
except Exception as e:
    logger.error(f"PostgreSQL operation failed: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
    if mongo_client:
        mongo_client.close()
    logger.info("Database connections closed")
