# Import necessary modules
import os
import sys
import json
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Retrieve MongoDB URL from environment variables
MONGO_DB_URL = os.getenv("MONGO_DB_URL")

# Print MongoDB URL for verification
print(MONGO_DB_URL)

# Import SSL certificate for MongoDB connection
import certifi
ca = certifi.where()

# Import data manipulation and database interaction libraries
import pandas as pd
import numpy as np
import pymongo

# Import custom exception and logging classes
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

class NetworkDataExtract:
    def __init__(self):
        try:
            # Placeholder for initialization logic
            pass
        except Exception as e:
            # Raise custom exception if initialization fails
            raise NetworkSecurityException(e, sys)

    def csv_to_json_convertor(self, file_path):
        """
        Convert a CSV file to a list of JSON records.

        :param file_path: Path to the CSV file
        :return: List of JSON records
        """
        try:
            # Read CSV file into a pandas DataFrame
            data = pd.read_csv(file_path)
            
            # Reset the index of the DataFrame to ensure consistent indexing
            data.reset_index(drop=True, inplace=True)
            
            # Convert DataFrame to JSON and extract records
            records = list(json.loads(data.T.to_json()).values())
            
            return records
        except Exception as e:
            # Raise custom exception if conversion fails
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        """
        Insert records into a MongoDB collection.

        :param records: List of records to insert
        :param database: Name of the database
        :param collection: Name of the collection
        :return: Number of records inserted
        """
        try:
            # Store input parameters as instance variables
            self.database = database
            self.collection = collection
            self.records = records

            # Create a MongoDB client using the URL from environment variables
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            
            # Select the specified database
            self.database = self.mongo_client[self.database]
            
            # Select the specified collection
            self.collection = self.database[self.collection]
            
            # Insert multiple records into the collection
            self.collection.insert_many(self.records)
            
            # Return the number of records inserted
            return len(self.records)
        except Exception as e:
            # Raise custom exception if insertion fails
            raise NetworkSecurityException(e, sys)
        

# Main execution block

if __name__ == '__main__':
    # Define the path to the CSV file
    FILE_PATH = "Network_Data/phisingData.csv"

    # Define the MongoDB database name
    DATABASE = "AKASHAI"

    # Define the MongoDB collection name
    Collection = "NetworkData"

    # Create an instance of NetworkDataExtract
    networkobj = NetworkDataExtract()

    # Convert CSV to JSON records
    records = networkobj.csv_to_json_convertor(file_path=FILE_PATH)

    # Print the records
    print(records)

    # Insert JSON records into MongoDB and get the number of records inserted
    no_of_records = networkobj.insert_data_mongodb(records, DATABASE, Collection)

    # Print the number of records inserted
    print(no_of_records)






