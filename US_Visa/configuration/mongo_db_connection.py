import sys
import os
import pymongo
import certifi
from US_Visa.exception import USvisaException
from US_Visa.logger import logging
from US_Visa.constants import DATABASE_NAME, MONGODB_URL_KEY

# Ensure the root project folder is in sys.path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Setting up certifi for TLS connection
ca = certifi.where()

# Custom Exception Class
class USvisaException(Exception):
    def __init__(self, message, sys):
        self.message = message
        self.sys = sys
        super().__init__(self.message)

# MongoDB Client Class
class MongoDBClient:
    """
    Class Name: MongoDBClient
    Description: This class handles the connection to the MongoDB database and exports the connection.
    
    Attributes:
        client: MongoDB client connection
        database: The MongoDB database connection object
        database_name: The name of the database being used

    Methods:
        __init__: Initializes the connection to the MongoDB database
    """
    
    client = None

    def __init__(self, database_name=DATABASE_NAME) -> None:
        """
        Initializes the connection to the MongoDB database if not already established.
        
        Parameters:
            database_name (str): Name of the database to connect to. Default is taken from `DATABASE_NAME`.
        """
        try:
            # Establish MongoDB connection if not already created
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)
                if mongo_db_url is None:
                    raise Exception(f"Environment key: {MONGODB_URL_KEY} is not set.")
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name
            logging.info(f"MongoDB connection successful to database {self.database_name}")
        
        except Exception as e:
            logging.error(f"Error while connecting to MongoDB: {str(e)}")
            raise USvisaException(str(e), sys)

# Example usage in your script or app.py
if __name__ == "__main__":
    try:
        # Initialize the MongoDB client
        mongo_client = MongoDBClient()
        # Access the database
        db = mongo_client.database
        print(f"Successfully connected to database: {mongo_client.database_name}")
    
    except USvisaException as e:
        logging.error(f"MongoDB connection failed: {e.message}")
