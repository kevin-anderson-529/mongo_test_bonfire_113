#imports
import pandas as pd
from pymongo import MongoClient

def load_and_clean_data():
    student_mat = pd.read_csv(r"C:\Users\kevin\OneDrive\Desktop\Coding Temple\Week 6 (Tableau) + Pipeline\MongoDB\student-mat.csv", delimiter=";")
    student_por = pd.read_csv(r"C:\Users\kevin\OneDrive\Desktop\Coding Temple\Week 6 (Tableau) + Pipeline\MongoDB\student-por.csv", delimiter=";")

    # Merge the datasets
    student_data = pd.concat([student_mat, student_por], axis=0, ignore_index=True)

    # Display headers
    print(student_data.head())

    return student_data

# Call the function to load and display the data
student_data = load_and_clean_data()

# Clean data

# Remove duplicate entries
student_data.drop_duplicates(inplace=True)

print(student_data)

# Connect to MongoDB

def insert_data_to_mongodb(data, connection_string):
    client = MongoClient(connection_string)

    # Database and collection
    db = client["student_performance_db"]
    collection = db["student_data"]

    # Convert to dictionary
    data_dict = data.to_dict("records")

    # Insert into MongoDB
    collection.insert_many(data_dict)

    # Close the MongoDB client
    client.close()

# Call to insert data to Mongo

mongodb_connection_string = "mongodb+srv://kevinanderson0529:$gi7b!8a4AjYM.V@cluster0.ntavb27.mongodb.net/?retryWrites=true&w=majority"

insert_data_to_mongodb(student_data, mongodb_connection_string)
