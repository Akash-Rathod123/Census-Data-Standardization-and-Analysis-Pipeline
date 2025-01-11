import pandas as pd
from pymongo import MongoClient

# Load the dataset
file_path = r"C:\Users\DELL\Downloads\updated_filled_stateUT_dataset.xlsx"
data = pd.read_excel(file_path)

# Step 1: Connect to MongoDB (replace <db_password> with your actual password)
client = MongoClient("mongodb+srv://akashrathod1433333:q7CKWcPEzdOlojzY@cluster12.jegtg.mongodb.net/?retryWrites=true&w=majority&appName=Cluster12")

# Select the database and collection
db = client["census_database"]  # Create or select the database
collection = db["census"]  # Create or select the "census" collection

# Step 2: Convert DataFrame to dictionary format for MongoDB insertion
data_dict = data.to_dict("records")

# Step 3: Insert data into MongoDB collection
collection.insert_many(data_dict)

print("Data has been successfully inserted into the MongoDB 'census' collection.")
