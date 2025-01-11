import streamlit_app as st
import mysql.connector
import pandas as pd

# Set up database connection
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="PAssword",
        database="DBname"
    )

# Function to execute the query
def run_query(query):
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    cursor.close()
    connection.close()
    return pd.DataFrame(rows, columns=columns)

# List of queries to display in Streamlit
queries ={ 
    "Total Population of Each District": """
        SELECT District, SUM(Population) AS Total_Population
        FROM census
        GROUP BY District;
    """,
    "Literate Males and Females in Each District": """
        SELECT District, SUM(Literate_Male) AS Literate_Males, SUM(Literate_Female) AS Literate_Females
        FROM census
        GROUP BY District;
    """,
    "Percentage of Workers (Male and Female) in Each District": """
        SELECT District, 
               SUM(Workers) AS Total_Workers, 
               (SUM(Male_Workers) / SUM(Population)) * 100 AS Male_Worker_Percentage, 
               (SUM(Female_Workers) / SUM(Population)) * 100 AS Female_Worker_Percentage
        FROM census
        GROUP BY District;
    """,
    "Households with Access to LPG or PNG in Each District": """
        SELECT District, 
               SUM(LPG_or_PNG_Households) AS Households_with_LPG_or_PNG
        FROM census
        GROUP BY District;
    """,
    "Religious Composition of Each District": """
        SELECT District, 
               SUM(Hindus) AS Hindus, 
               SUM(Muslims) AS Muslims, 
               SUM(Christians) AS Christians, 
               SUM(Sikhs) AS Sikhs, 
               SUM(Buddhists) AS Buddhists, 
               SUM(Jains) AS Jains, 
               SUM(Others_Religions) AS Others, 
               SUM(Religion_Not_Stated) AS Religion_Not_Stated
        FROM census
        GROUP BY District;
    """,
    "Households with Internet Access in Each District": """
        SELECT District, 
               SUM(Households_with_Internet) AS Households_with_Internet_Access
        FROM census
        GROUP BY District;
    """,
    "Educational Attainment Distribution in Each District": """
        SELECT District_code, District,
               SUM(Below_Primary_Education) AS Below_Primary,
               SUM(Primary_Education) AS Primary,
               SUM(Middle_Education) AS Middle,
               SUM(Secondary_Education) AS Secondary,
               SUM(Higher_Education) AS Higher,
               SUM(Graduate_Education) AS Graduate,
               SUM(Other_Education) AS Other,
               SUM(Literate_Education) AS Literate,
               SUM(Illiterate_Education) AS Illiterate
        FROM census
        GROUP BY District_code, District
        ORDER BY District_code;
    """,
    "Households with Access to Various Modes of Transportation in Each District": """
        SELECT District_code, District,
               SUM(Households_with_Bicycle) AS Bicycle,
               SUM(Households_with_Car_Jeep_Van) AS Car_Jeep_Van,
               SUM(Households_with_Radio_Transistor) AS Radio_Transistor,
               SUM(Households_with_Television) AS Television,
               SUM(Households_with_Scooter_Motorcycle_Moped) AS Scooter_Motorcycle_Moped
        FROM census
        GROUP BY District_code, District
        ORDER BY District_code;
    """,
    "Occupied Census Houses in Each District": """
        SELECT District_code, District,
               SUM(Condition_of_occupied_census_houses_Dilapidated_Households) AS Dilapidated,
               SUM(Households_with_separate_kitchen_Cooking_inside_house) AS With_Separate_Kitchen,
               SUM(Having_bathing_facility_Total_Households) AS With_Bathing_Facility,
               SUM(Having_latrine_facility_within_the_premises_Total_Households) AS With_Latrine_Facility
        FROM census
        GROUP BY District_code, District
        ORDER BY District_code;
    """,
    "Total Number of Households in Each State": """
        SELECT State_UT,
               SUM(Households) AS Total_Households
        FROM census
        GROUP BY State_UT
        ORDER BY State_UT;
    """,
    "Households with a Latrine Facility in Each State": """
        SELECT State_UT,
               SUM(Having_latrine_facility_within_the_premises_Total_Households) AS Households_With_Latrine
        FROM census
        GROUP BY State_UT
        ORDER BY State_UT;
    """,
	"average household size in each state":"""
		SELECT
			State_UT,
			AVG(Households / NULLIF(Population, 0)) AS Avg_Household_Size
		FROM
			census
		GROUP BY
			State_UT
		ORDER BY
			State_UT;
	""",
	"ouseholds are owned versus rented in each state":"""
		SELECT
			State_UT,
			SUM(Ownership_Owned_Households) AS Owned,
			SUM(Ownership_Rented_Households) AS Rented
		FROM
			census
		GROUP BY
			State_UT
		ORDER BY
			State_UT;
	""",
	"different types of latrine facilities":"""
		SELECT
			State_UT,
			SUM(Type_of_latrine_facility_Pit_latrine_Households) AS Pit_Latrine,
			SUM(Type_of_latrine_facility_Flush_pour_flush_latrine_connected_) AS Flush_Latrine,
			SUM(Type_of_latrine_facility_Other_latrine_Households) AS Other_Latrine
		FROM
			Akashdb.census
		GROUP BY
			State_UT
		ORDER BY
			State_UT;
	""",
	"households have access to drinking water sources near the premises in each state":"""
		SELECT
			State_UT,
			SUM(Location_of_drinking_water_source_Near_the_premises_Househol) AS Water_Near_Premises
		FROM
			Akashdb.census
		GROUP BY
			State_UT
		ORDER BY
			State_UT;
	""",
	"average household income distribution in each state":"""
		SELECT
			State_UT,
			AVG(Power_Parity_Less_than_Rs_45000) AS Less_Than_45000,
			AVG(Power_Parity_Rs_45000_90000) AS Between_45000_and_90000,
			AVG(Power_Parity_Rs_90000_150000) AS Between_90000_and_150000,
			AVG(Power_Parity_Rs_150000_240000) AS Between_150000_and_240000,
			AVG(Power_Parity_Rs_240000_330000) AS Between_240000_and_330000,
			AVG(Power_Parity_Rs_330000_545000) AS Between_330000_and_545000,
			AVG(Power_Parity_Above_Rs_545000) AS More_Than_545000
		FROM
			Akashdb.census
		GROUP BY
			State_UT
		ORDER BY
			State_UT;
	""",
	"percentage of married couples":"""
		SELECT
			State_UT,
			SUM(Married_couples_1_Households) * 100.0 / SUM(Households) AS One_Couple_Percentage,
			SUM(Married_couples_2_Households) * 100.0 / SUM(Households) AS Two_Couples_Percentage,
			SUM(Married_couples_3_or_more_Households) * 100.0 / SUM(Households) AS Three_or_More_Couples_Percentage
		FROM
			Akashdb.census
		GROUP BY
			State_UT
		ORDER BY
			State_UT;
	""",
	"households fall below the poverty line":"""
		SELECT
			State_UT,
			SUM(Power_Parity_Less_than_Rs_45000) AS Below_Poverty_Line_Households
		FROM
			Akashdb.census
		GROUP BY
			State_UT
		ORDER BY
			State_UT;
	""",
	"overall literacy rate":"""
		SELECT
			State_UT,
			SUM(Literate) * 100.0 / SUM(Population) AS Literacy_Rate_Percentage
		FROM
			Akashdb.census
		GROUP BY
			State_UT
		ORDER BY
			State_UT;
	"""	
}

# Streamlit UI
st.title("Census Data Queries")

# Dropdown menu to select a query
query_name = st.selectbox("Select a Query", list(queries.keys()))

# Run the selected query
if st.button("Run Query"):
    query = queries[query_name]
    result_df = run_query(query)
    st.write(result_df)
