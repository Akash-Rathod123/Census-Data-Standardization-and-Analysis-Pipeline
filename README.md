Census Data Standardization and Analysis Pipeline

### Description:
## Problem Statement
This project is aimed at building an end-to-end data pipeline for cleaning, processing, and analyzing datasets.
 It uses various tools and technologies like Python, Pandas, MongoDB, and Streamlit for data manipulation and visualization.
 The objective is to ensure that data is clean, reliable, and efficiently processed for generating insights.

## Tools and Technologies Used
- **Python**: Main programming language used.
- **Pandas**: For data cleaning and processing.
- **MongoDB**: For data storage and retrieval.
- **MySQL**: Used as a relational database for storing structured data.
- **Streamlit**: For building the web application to visualize the data.


## Approach
1. **Data Cleaning**: Used scripts like `rename_column.py` and `Find_process_missingData.py` to standardize and clean datasets.
2. **Data Transformation**: The `rename_stateUT.py` and `NEW_stateUT.py` scripts were used to modify and transform datasets.
3. **Data Transfer**: Transferred data between MongoDB and MySQL using `DataTransfer_to_MongoDB.py` and `Mongo_Mysql.py`.
4. **Data Visualization**: Built an interactive web interface using `streamlit_app.py` to visualize and analyze the processed data.


## Installation

1. **Clone the repository**:
    ```bash
    git clone https://github.com/Akash-Rathod123/Census-Data-Standardization-and-Analysis-Pipeline.git
    ```
2. **Install dependencies**:
    ```bash
    #pip install  --You can Have To install Below Packages 
pandas==1.5.3          # For data manipulation and analysis
pymongo==4.4.0         # For MongoDB interactions
mysql-connector-python==8.0.32  # For MySQL interactions
streamlit==1.27.0      # For building the web app

    ```
3. **Set up databases**:
    - Install MongoDB and MySQL .
    - Ensure the databases are running and properly configured.
    
4. **Run the project**:
    ```bash
    python streamlit_app.py
    ```

5. **Access the app**: Open `http://localhost:8501/` in your browser to access the Streamlit web app.

## Project Structure

```bash
.
├── rename_column.py
├── rename_stateUT.py
├── NEW_stateUT.py
├── Find_process_missingData.py
├── DataTransfer_to_MongoDB.py
├── Mongo_Mysql.py
├── streamlit_app.py
├── README.md
└── requirements.txt

