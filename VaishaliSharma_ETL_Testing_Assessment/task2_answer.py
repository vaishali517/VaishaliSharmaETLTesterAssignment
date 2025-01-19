import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Date, MetaData
import logging

#Configure logging
logging.basicConfig(
    filename = " task2_log.log",
    level = logging.DEBUG,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

#Database configuration for PostgreSQL
DATABASE_URI = 'postgresql://username:password@localhost:1234/my_database'

def extract_data(file_path):
    """
    This method extracts data from the flat file.
    
    param file_path : Path to the CSV file
    return : A pandas dataframe containing data from the CSV file
    """
    try:
        logging.info("Extracting data from the file : {}.".format(file_path))
        data = pd.read_csv(file_path)
        logging.info("Extracting data successful!")
        return data
    except Exception as e:
        logging.error("Error encountered during execution : %s", e)

def transform_data(data):
    """
    This method performs the following data transformation activities - 
        1. Clean column names by removing leading/trailing spaces
        2. Convert 'date_of_birth' column to datetime format , setting invalid entries to NaT
        3. Ensure 'salary' column is converted to a numeric type, setting invalid entries to NaN
    
    param data : Data from the flat file
    return : A pandas dataframe with the transformed data
    """
    try:
        logging.info("Starting data transformation.")
        
        #Executing activity 1 from docstring
        data.columns = data.columns.strip()
        
        #Executing activity 2 from docstring
        data['date_of_birth'] = pd.to_datetime(data[date_of_birth], errors = 'coerce')
        
        #Executing activity 3 from docstring
        data['salary'] = pd.to_numeric(data['salary'], errors = 'coerce')
        
        logging.info("Data transformations completed successfully!")
        return data
    except Exception as e:
        logging.error("Error encounteredd while performing data transformations : %s", e)
        
def load_data(engine, data):
    """
    This method loads the data into the database tables Employees and Departments by performing the below tasks - 
        1. Defining the schema of both tables
        2. Creating both tables if they don't exist
        3. Loading data into the tables
        
    param engine : 
    param data : Data from the flat file
    """
    try:
        logging.info("Starting data load into database.")
        
        #Define metadata object to manage schema information
        metadata = MetaData()
        
        #Define the employees table schema
        employees = Table(
            'employees', metadata,
            Column('id', Integer, primary_key = True),
            Column('name', String),
            Column('date_of_birth', Date),
            Column('salary', Float),
            Column('department_id', Integer)
        )
        
        #Define the departments table schema
        departments = Table(
            'departments', metadata,
            Column('department_id', Integer, primary_key = True),
            Column('department_name', String)
        )
        
        #Create the tables in the database (if they don't exist)
        metadata.create_all(engine)
        
        #Extract unique Department IDs and assign a default name
        departments = data[['department_id']].drop_duplicates().assign(
            department_name = lambda df: 'Dept_' + df['department_id'].astype(str)
        )
        
        #Load the departments data into the database
        departments.to_sql('departments', con=engine, if_exists='replace', index=False)
        
        #Load the Employees data into the database
        data[['id', 'name', 'date_of_birth', 'salary', 'department_id']].to_sql(
            'employees', con=engine, if_exists='replace', index=False
        )
        
        logging.info("Data load completed successfully!")
        
    except Exception as e:
        logging.error("Error encountered during data load : %s", e)
        
if __name__ == '__main__':
    
    file_path = 'flat_file.csv'
    
    try:
        logger.info("ETL Process Started.")
        
        #Extract
        raw_data = extract_data(file_path)
        
        #Transform
        transformed_data = transform_data(raw_data)
        
        #Load
        engine = create_engine(DATABASE_URI)
        load_data(engine, transformed_data)
        
        logging.info("ETL Process Completed Successfully!")
    except Exception as e:
        logging.error("ETL Process Failed : %s", e)