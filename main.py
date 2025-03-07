import pandas as pd
import requests
from typing import Dict, List
import logging
import configparser

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, api_endpoint: str, auth_token: str):
        self.api_endpoint = api_endpoint
        self.headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        
    def get_group(self, group_name: str) -> str:
        """
        Make API request to get group ID based on group name
        """
        try:
            response = requests.post(
                f"{self.api_endpoint}",
                json={
                    "jsonrpc": "2.0",
                    "method": "hostgroup.get",
                    "params": {
                        "output": "extend",        
                        "filter": {
                            "name": group_name
                        }
                    },
                    "id": 1 
                },
                headers=self.headers
            )
            response.raise_for_status()
            result = response.json().get("result", [])
            if result:
                return result[0]["groupid"]  # Return the first group's ID
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting group ID for {group_name}: {str(e)}")
            raise

    def get_hosts(self, group_id: str) -> List[Dict]:
        """
        Retrieve records for a given group ID
        """
        try:
            response = requests.post(
                f"{self.api_endpoint}",
                json={
                    "jsonrpc": "2.0",
                    "method": "host.get",
                    "params": {
                        "groupids": group_id,
                        "output": "name",
                        "selectTags": ["tag", "value"]
                    },
                    "id": 1
                },
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting records for group ID {group_id}: {str(e)}")
            raise

    def update_record(self, record_id: str, updated_tags: Dict) -> None:
        """
        Update record with new tags
        """
        try:
            response = requests.post(
                f"{self.api_endpoint}",
                json={
                    "jsonrpc": "2.0",
                    "method": "host.update",
                    "params": {
                        "hostid": record_id,
                        "tags": updated_tags
                    },
                    "id": 1
                },
                headers=self.headers
            )
            response.raise_for_status()
            logger.info(f"Successfully updated record {record_id}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating record {record_id}: {str(e)}")
            raise

def main():
    logger.info("Starting data processing script")
    
    # Load configuration
    config = configparser.ConfigParser()
    try:
        config.read('config.ini')
        API_ENDPOINT = config['DEFAULT']['api_endpoint']
        AUTH_TOKEN = config['DEFAULT']['auth_token']
        CSV_FILE_PATH = config['DEFAULT']['csv_file_path']
    except Exception as e:
        logger.error(f"Error reading configuration: {str(e)}")
        return

    logger.info(f"Initializing DataProcessor with endpoint: {API_ENDPOINT}")
    processor = DataProcessor(API_ENDPOINT, AUTH_TOKEN)

    # Read CSV file
    logger.info(f"Reading CSV file from: {CSV_FILE_PATH}")
    try:
        df = pd.read_csv(CSV_FILE_PATH, sep=';', encoding='utf-8-sig')
        logger.info(f"Successfully loaded CSV with {len(df)} rows")
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        return

    # Process each row in the CSV
    for index, row in df.iterrows():
        logger.info(f"Processing row {index + 1}/{len(df)} - Group: {row['groupname']}")
        try:
            # Get group ID
            logger.info(f"Fetching group ID for {row['groupname']}")
            group_id = processor.get_group(row['groupname'])
            if not group_id:
                logger.warning(f"No group ID found for {row['groupname']}, skipping...")
                continue

            # Get records for the group
            logger.info(f"Retrieving hosts for group ID: {group_id}")
            records = processor.get_hosts(group_id)
            logger.info(f"Found {len(records)} hosts in group")

            # Define our tag names that we'll update
            update_tag_names = ["COUNTRY", "SITE_NAME", "SITE_ID", "PARKID", "TECHNOLOGY"]

            # Prepare new tags
            new_tags = [
                {"tag": "COUNTRY", "value": row['site_country']},
                {"tag": "SITE_NAME", "value": row['site_name']},
                {"tag": "SITE_ID", "value": row['site_id']},
                {"tag": "PARKID", "value": row['park_id']},
                {"tag": "TECHNOLOGY", "value": row['technology']}
            ]

            # Update each record with new tags
            for record in records.get("result", []):
                # Keep existing tags that aren't in our update list
                existing_tags = [tag for tag in record.get("tags", []) 
                               if tag["tag"] not in update_tag_names]
                
                # Combine existing and new tags
                final_tags = existing_tags + new_tags
                
                logger.info(f"Updating host {record['hostid']}")
                processor.update_record(record['hostid'], final_tags)

        except Exception as e:
            logger.error(f"Error processing row {index}: {str(e)}")
            continue

    logger.info("Data processing script completed")

if __name__ == "__main__":
    main()