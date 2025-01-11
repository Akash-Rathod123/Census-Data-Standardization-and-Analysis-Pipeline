import pymongo
import math
import mysql.connector
from pymongo import MongoClient
from multiprocessing import Process, Pipe


def fetch_from_mongodb(database_name, collection_name):
    try:
        # Establish MongoDB connection
        client = MongoClient(
            "mongodb+srv://<Userid>:<Password>@cluster12.jegtg.mongodb.net/?retryWrites=true&w=majority&appName=Cluster12"
        )
        db = client[database_name]
        collection = db[collection_name]

        # Fetch all documents and exclude the _id field
        documents = list(collection.find({}, {"_id": 0}))

        client.close()
        return documents
    except Exception as e:
        print(f"Error while fetching data from MongoDB: {e}")
        return []


def connect_to_sql_server():
    try:
        # MySQL connection
        mysql_conn = mysql.connector.connect(
            host="localhost",  # Change to your MySQL host
            user="root",  # Your MySQL username
            password="XYZ",  # Your MySQL password
            database="Databasename",  # Your MySQL database name
        )
        return mysql_conn
    except mysql.connector.Error as err:
        print(f"Error while connecting to MySQL: {err}")
        return None


def create_table(conn):
    try:
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS census (
            District_code INT,
            State_UT VARCHAR(255),
            District VARCHAR(255),
            Population INT,
            Male INT,
            Female INT,
            Literate INT,
            Literate_Male INT,
            Literate_Female INT,
            SC INT,
            Male_SC INT,
            Female_SC INT,
            ST INT,
            Male_ST INT,
            Female_ST INT,
            Workers INT,
            Male_Workers INT,
            Female_Workers INT,
            Main_Workers INT,
            Marginal_Workers INT,
            Non_Workers INT,
            Cultivator_Workers INT,
            Agricultural_Workers INT,
            Household_Workers INT,
            Other_Workers INT,
            Hindus INT,
            Muslims INT,
            Christians INT,
            Sikhs INT,
            Buddhists INT,
            Jains INT,
            Others_Religions INT,
            Religion_Not_Stated INT,
            LPG_or_PNG_Households INT,
            Households_with_Electric_Lighting INT,
            Households_with_Internet INT,
            Households_with_Computer INT,
            Households_Rural INT,
            Households_Urban INT,
            Households INT,
            Below_Primary_Education INT,
            Primary_Education INT,
            Middle_Education INT,
            Secondary_Education INT,
            Higher_Education INT,
            Graduate_Education INT,
            Other_Education INT,
            Literate_Education INT,
            Illiterate_Education INT,
            Total_Education INT,
            Young_and_Adult INT,
            Middle_Aged INT,
            Senior_Citizen INT,
            Age_Not_Stated INT,
            Households_with_Bicycle INT,
            Households_with_Car_Jeep_Van INT,
            Households_with_Radio_Transistor INT,
            Households_with_Scooter_Motorcycle_Moped INT,
            Households_with_Telephone_Mobile_Phone_Landline_only INT,
            Households_with_Telephone_Mobile_Phone_Mobile_only INT,
            Households_with_TV_Computer_Laptop_Telephone_mobile_phone_an INT,
            Households_with_Television INT,
            Households_with_Telephone_Mobile_Phone INT,
            Households_with_Telephone_Mobile_Phone_Both INT,
            Condition_of_occupied_census_houses_Dilapidated_Households INT,
            Households_with_separate_kitchen_Cooking_inside_house INT,
            Having_bathing_facility_Total_Households INT,
            Having_latrine_facility_within_the_premises_Total_Households INT,
            Ownership_Owned_Households INT,
            Ownership_Rented_Households INT,
            Type_of_bathing_facility_Enclosure_without_roof_Households INT,
            Type_of_fuel_used_for_cooking_Any_other_Households INT,
            Type_of_latrine_facility_Pit_latrine_Households INT,
            Type_of_latrine_facility_Other_latrine_Households INT,
            Type_of_latrine_facility_Night_soil_disposed_into_open_drain INT,
            Type_of_latrine_facility_Flush_pour_flush_latrine_connected INT,
            Not_having_bathing_facility_within_the_premises_Total_Househ INT,
            Not_having_latrine_facility_within_the_premises_Alternative INT,
            Main_source_of_drinking_water_Un_covered_well_Households INT,
            Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Hou INT,
            Main_source_of_drinking_water_Spring_Households INT,
            Main_source_of_drinking_water_River_Canal_Households INT,
            Main_source_of_drinking_water_Other_sources_Households INT,
            Main_source_of_drinking_water_Other_sources_Spring_River_Can INT,
            Location_of_drinking_water_source_Near_the_premises_Househol INT,
            Location_of_drinking_water_source_Within_the_premises_Househ INT,
            Main_source_of_drinking_water_Tank_Pond_Lake_Households INT,
            Main_source_of_drinking_water_Tapwater_Households INT,
            Main_source_of_drinking_water_Tubewell_Borehole_Households INT,
            Household_size_1_person_Households INT,
            Household_size_2_persons_Households INT,
            Household_size_1_to_2_persons_Households INT,
            Household_size_3_persons_Households INT,
            Household_size_3_to_5_persons_Households INT,
            Household_size_4_persons_Households INT,
            Household_size_5_persons_Households INT,
            Household_size_6_8_persons_Households INT,
            Household_size_9_persons_and_above_Households INT,
            Location_of_drinking_water_source_Away_Households INT,
            Married_couples_1_Households INT,
            Married_couples_2_Households INT,
            Married_couples_3_Households INT,
            Married_couples_3_or_more_Households INT,
            Married_couples_4_Households INT,
            Married_couples_5__Households INT,
            Married_couples_None_Households INT,
            Power_Parity_Less_than_Rs_45000 INT,
            Power_Parity_Rs_45000_90000 INT,
            Power_Parity_Rs_90000_150000 INT,
            Power_Parity_Rs_45000_150000 INT,
            Power_Parity_Rs_150000_240000 INT,
            Power_Parity_Rs_240000_330000 INT,
            Power_Parity_Rs_150000_330000 INT,
            Power_Parity_Rs_330000_425000 INT,
            Power_Parity_Rs_425000_545000 INT,
            Power_Parity_Rs_330000_545000 INT,
            Power_Parity_Rs_545000 INT,
            Total_Power_Parity INT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Error while creating table: {err}")


def insert_data_into_table(conn, documents):
    try:
        cursor = conn.cursor()

        for document in documents:
            # Replace None or NaN with suitable default values
            data_values = (
                # Mapping each field from MongoDB document
                get_safe_value(document.get('District_code')),
                get_safe_value(document.get('State_UT')),
                get_safe_value(document.get('District')),
                get_safe_value(document.get('Population')),
                get_safe_value(document.get('Male')),
                get_safe_value(document.get('Female')),
                get_safe_value(document.get('Literate')),
                get_safe_value(document.get('Literate_Male')),
                get_safe_value(document.get('Literate_Female')),
                get_safe_value(document.get('SC')),
                get_safe_value(document.get('Male_SC')),
                get_safe_value(document.get('Female_SC')),
                get_safe_value(document.get('ST')),
                get_safe_value(document.get('Male_ST')),
                get_safe_value(document.get('Female_ST')),
                get_safe_value(document.get('Workers')),
                get_safe_value(document.get('Male_Workers')),
                get_safe_value(document.get('Female_Workers')),
                get_safe_value(document.get('Main_Workers')),
                get_safe_value(document.get('Marginal_Workers')),
                get_safe_value(document.get('Non_Workers')),
                get_safe_value(document.get('Cultivator_Workers')),
                get_safe_value(document.get('Agricultural_Workers')),
                get_safe_value(document.get('Household_Workers')),
                get_safe_value(document.get('Other_Workers')),
                get_safe_value(document.get('Hindus')),
                get_safe_value(document.get('Muslims')),
                get_safe_value(document.get('Christians')),
                get_safe_value(document.get('Sikhs')),
                get_safe_value(document.get('Buddhists')),
                get_safe_value(document.get('Jains')),
                get_safe_value(document.get('Others_Religions')),
                get_safe_value(document.get('Religion_Not_Stated')),
                get_safe_value(document.get('LPG_or_PNG_Households')),
                get_safe_value(document.get('Households_with_Electric_Lighting')),
                get_safe_value(document.get('Households_with_Internet')),
                get_safe_value(document.get('Households_with_Computer')),
                get_safe_value(document.get('Households_Rural')),
                get_safe_value(document.get('Households_Urban')),
                get_safe_value(document.get('Households')),
                get_safe_value(document.get('Below_Primary_Education')),
                get_safe_value(document.get('Primary_Education')),
                get_safe_value(document.get('Middle_Education')),
                get_safe_value(document.get('Secondary_Education')),
                get_safe_value(document.get('Higher_Education')),
                get_safe_value(document.get('Graduate_Education')),
                get_safe_value(document.get('Other_Education')),
                get_safe_value(document.get('Literate_Education')),
                get_safe_value(document.get('Illiterate_Education')),
                get_safe_value(document.get('Total_Education')),
                get_safe_value(document.get('Young_and_Adult')),
                get_safe_value(document.get('Middle_Aged')),
                get_safe_value(document.get('Senior_Citizen')),
                get_safe_value(document.get('Age_Not_Stated')),
                get_safe_value(document.get('Households_with_Bicycle')),
                get_safe_value(document.get('Households_with_Car_Jeep_Van')),
                get_safe_value(document.get('Households_with_Radio_Transistor')),
                get_safe_value(document.get('Households_with_Scooter_Motorcycle_Moped')),
                get_safe_value(document.get('Households_with_Telephone_Mobile_Phone_Landline_only')),
                get_safe_value(document.get('Households_with_Telephone_Mobile_Phone_Mobile_only')),
                get_safe_value(document.get('Households_with_TV_Computer_Laptop_Telephone_mobile_phone_an')),
                get_safe_value(document.get('Households_with_Television')),
                get_safe_value(document.get('Households_with_Telephone_Mobile_Phone')),
                get_safe_value(document.get('Households_with_Telephone_Mobile_Phone_Both')),
                get_safe_value(document.get('Condition_of_occupied_census_houses_Dilapidated_Households')),
                get_safe_value(document.get('Households_with_separate_kitchen_Cooking_inside_house')),
                get_safe_value(document.get('Having_bathing_facility_Total_Households')),
                get_safe_value(document.get('Having_latrine_facility_within_the_premises_Total_Households')),
                get_safe_value(document.get('Ownership_Owned_Households')),
                get_safe_value(document.get('Ownership_Rented_Households')),
                get_safe_value(document.get('Type_of_bathing_facility_Enclosure_without_roof_Households')),
                get_safe_value(document.get('Type_of_fuel_used_for_cooking_Any_other_Households')),
                get_safe_value(document.get('Type_of_latrine_facility_Pit_latrine_Households')),
                get_safe_value(document.get('Type_of_latrine_facility_Other_latrine_Households')),
                get_safe_value(document.get('Type_of_latrine_facility_Night_soil_disposed_into_open_drain')),
                get_safe_value(document.get('Type_of_latrine_facility_Flush_pour_flush_latrine_connected')),
                get_safe_value(document.get('Not_having_bathing_facility_within_the_premises_Total_Househ')),
                get_safe_value(document.get('Not_having_latrine_facility_within_the_premises_Alternative')),
                get_safe_value(document.get('Main_source_of_drinking_water_Un_covered_well_Households')),
                get_safe_value(document.get('Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Hou')),
                get_safe_value(document.get('Main_source_of_drinking_water_Spring_Households')),
                get_safe_value(document.get('Main_source_of_drinking_water_River_Canal_Households')),
                get_safe_value(document.get('Main_source_of_drinking_water_Other_sources_Households')),
                get_safe_value(document.get('Main_source_of_drinking_water_Other_sources_Spring_River_Can')),
                get_safe_value(document.get('Location_of_drinking_water_source_Near_the_premises_Househol')),
                get_safe_value(document.get('Location_of_drinking_water_source_Within_the_premises_Househ')),
                get_safe_value(document.get('Main_source_of_drinking_water_Tank_Pond_Lake_Households')),
                get_safe_value(document.get('Main_source_of_drinking_water_Tapwater_Households')),
                get_safe_value(document.get('Main_source_of_drinking_water_Tubewell_Borehole_Households')),
                get_safe_value(document.get('Household_size_1_person_Households')),
                get_safe_value(document.get('Household_size_2_persons_Households')),
                get_safe_value(document.get('Household_size_1_to_2_persons_Households')),
                get_safe_value(document.get('Household_size_3_persons_Households')),
                get_safe_value(document.get('Household_size_3_to_5_persons_Households')),
                get_safe_value(document.get('Household_size_4_persons_Households')),
                get_safe_value(document.get('Household_size_5_persons_Households')),
                get_safe_value(document.get('Household_size_6_8_persons_Households')),
                get_safe_value(document.get('Household_size_9_persons_and_above_Households')),
                get_safe_value(document.get('Location_of_drinking_water_source_Away_Households')),
                get_safe_value(document.get('Married_couples_1_Households')),
                get_safe_value(document.get('Married_couples_2_Households')),
                get_safe_value(document.get('Married_couples_3_Households')),
                get_safe_value(document.get('Married_couples_3_or_more_Households')),
                get_safe_value(document.get('Married_couples_4_Households')),
                get_safe_value(document.get('Married_couples_5__Households')),
                get_safe_value(document.get('Married_couples_None_Households')),
                get_safe_value(document.get('Power_Parity_Less_than_Rs_45000')),
                get_safe_value(document.get('Power_Parity_Rs_45000_90000')),
                get_safe_value(document.get('Power_Parity_Rs_90000_150000')),
                get_safe_value(document.get('Power_Parity_Rs_45000_150000')),
                get_safe_value(document.get('Power_Parity_Rs_150000_240000')),
                get_safe_value(document.get('Power_Parity_Rs_240000_330000')),
                get_safe_value(document.get('Power_Parity_Rs_150000_330000')),
                get_safe_value(document.get('Power_Parity_Rs_330000_425000')),
                get_safe_value(document.get('Power_Parity_Rs_425000_545000')),
                get_safe_value(document.get('Power_Parity_Rs_330000_545000')),
                get_safe_value(document.get('Power_Parity_Rs_545000')),
                get_safe_value(document.get('Total_Power_Parity'))
            )

            insert_query = "INSERT INTO census VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            
            cursor.execute(insert_query, data_values)

        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        
def get_safe_value(value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None  # For MySQL, None will be converted to NULL
    return value
        


def main():
    # Fetch documents from MongoDB
    documents = fetch_from_mongodb("census_database", "census")

    if documents:
        # Connect to MySQL
        mysql_conn = connect_to_sql_server()
        if mysql_conn:
            # Create table and insert data
            create_table(mysql_conn)
            insert_data_into_table(mysql_conn, documents)
            print("Data transfer completed successfully.")
            mysql_conn.close()


if __name__ == "__main__":
    main()
