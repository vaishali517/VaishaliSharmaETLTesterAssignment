import csv
from collections import Counter, defaultdict
from datetime import datetime
import logging

#Configure logging
logging.basicConfig(
    filename = "task1_log.log",
    level = logging.DEBUG,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

def extract_unique_breeds(file_path):
    """
    This method does the following tasks -
        1. Reads the CSV file
        2. Normalizes the breed names
        3. Extracts unique breed names from the data
        
    param file_path : Path to the CSV file
    return : List of unique, normalized breeds
    """
    breeds = set()
    logging.info("Extracting unique, normalized breeds from the file: %s", file_path)
    
    try:
        with open(file_path, "r") as file:
            records = csv.DictReader(file)
            for row in records:
                breed = row['Breed'].strip().replace(" ", "").lower()
                breeds.add(breed)
        login.info("Successfully extracted {} unique breeds!".format(len(breeds)))
    except Exception as e:
        logging.error("Error encountered while extracting unique breeds : %s", e)
        
    return list(breeds)
    
def license_count_by_type(file_path):
    """
    This method does the following tasks - 
        1. Reads the CSV file
        2. Creates a dictionary, mapping each breed to the number of license by LicenseType
        
    param file_path : Path to the CSV file
    return : Dictionary with breeds as keys and a nested dictionary with LicenseType as keys and count of licenses as values    
    """
    
    breed_license_count = defaultdict(lambda : defaultdict(int))
    logging.info("Counting licenses by LicenseType for each breed from the file: %s", file_path)
    
    try:
        with open(file_path, "r") as file:
            records = csv.DictReader(file)
            for row in records:
                breed = row['Breed'].strip().replace(" ", "").lower()
                license_type = row['LicenseType'].strip()
                breed_license_count[breed][license_type] += 1
        logging.info("Successfully counted licenses for {} breed!".format(len(breed_license_count)))
    except Exception as e:
        logging.error("Error encountered while counting licenses by Breed and LicenseType : %s", e)
        
    return dict(breed_license_count)
    
def top_dog_names(file_path):
    """
    This method does the following tasks - 
        1. Reads the CSV file
        2. Identifies the top 5 dog names with their counts
        
    param file_path : Path to the CSV file
    return : List of tuples containing dog names and their counts
    """
    dog_name_counter = Counter()
    logging.info("Finding the top 5 most popular dog names from the file: %s", file_path)
    
    try:
        with open(file_path, "r") as file:
            records = csv.DictReader(file)
            for row in records:
                dog_name = row['DogName'].strip().lower()
                dog_name_counter[dog_name] += 1
        logging.info("Successfully found the top 5 dog names!")
    except Exception as e:
        logging.error("Error encountered while finding the top 5 dog names : %s", e)
        
    return dog_name_counter.most_common(5)
    
def licenses_within_date_range(file_path, start_date, end_date):
    """
    This method does the following tasks - 
        1. Reads the CSV file
        2. Filters licenses issued within the given date range
        
    param file_path : Path to the CSV file
    param start_date : Start date in the format 'YYYY-MM-DD'
    param end_date : End date in the format 'YYYY-MM-DD'
    return : List of license details within the date range
    """
    
    results = []
    logging.info("Filtering licenses issued between %s and %s from the file: %s", start_date, end_date, file_path)
    
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        with open(file_path, "r") as file:
            records = csv.DictReader(file)
            for row in records:
                valid_date = datetime.strptime(row['ValidDate'], '%Y-%m-%d')
                if start_date <= valid_date <= end_date:
                    results.append(row)
        logging.info("Successfully filtered %d licenses within the date range!", len(results))
    except Exception as e:
        logging.error("Error encountered while filtering licenses : %s", e)
        
    return results
    
if __name__ == "__main__":
    
    file_path = '\path\to\2017.csv'
    
    #Q. No. 1 : Extract unique breeds
    unique_breeds = extract_unique_breeds(file_path)
    print("Unique Breeds : ", unique_breeds)
    
    #Q. No. 2 : License count by Breed and LicenseType
    license_count = license_count_by_type(file_path)
    print("\nLicenses by Breed and LicenseType : ", license_count)
    
    #Q. No. 3 : Top 5 popular dog names
    dog_count = top_dog_names(file_path)
    print("\nTop 5 dog names : ", dog_count)
    
    #Bonus Q. : Filter licenses within date range
    start_date = '2017-07-01'
    end_date = '2017-12-31'
    licenses_in_range = licenses_within_date_range(file_path, start_date, end_date)
    print("\nLicenses issued withing date range : ", licenses_in_range