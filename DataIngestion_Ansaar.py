DataIngestion_Ansaar

#First i am importing all lib:::
#Import sqllite3 to make a connection to the daatabase
#imported request ans json to read data from the API
import sqlite3
from sqlite3 import Error

import json
import requests
from urllib.request import Request, urlopen
import os
from datetime import datetime
import concurrent.futures
from pathlib import Path


#Making database connection
def make_connection(db_file):
    """ create a database connection to the SQLite database
        by the db_file
    :param db_file: database file
    :return: the Connection object if exist or None
    """
    connection = None
    try:
        connection = sqlite3.connect(db_file, check_same_thread = False)
        return connection
    except Error as e:
        print(e)

    return connection


# Filtering the API Data for the respective county
def data_county(county_name, data):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    final = []
    for row in data:
        if row[9] == county_name:
            final.append((row[8], row[10], row[11], row[12], row[13], dt_string))
    
    return final

#main function::
def main():
    
    my_file = Path("pythonsqlite.db")
    if my_file.is_file():
        os.remove("pythonsqlite.db")

    database = r"pythonsqlite.db"
    
    # create a database connection
    connection = make_connection(database)
    
    # Getting the Data from the API
    request = Request("https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD")
    response = urlopen(request)
    response = response.read()
    data = json.loads(response)
    DF = data["data"]
    nested_lst_of_tuples = [tuple(l) for l in DF]
    counties = []
    for row in nested_lst_of_tuples:
        modified = row[9].replace(" ", "")
        modified = modified.replace(".", "")
        if modified not in counties:
            counties.append(modified)

    # Creating and Inserting data into county tables:
    '''
    Current Implementation :
        1) So basically when i run cron jobs i am making sure that the tables dont get copied again and again 
        basically avoiding duplicates, hence i am deleting the old county tables
        2) Reinserting the entire data
        
    Better Logic:
    
        1) Once the county tables are created initially then,
        2) then insert records only for that particular day, by checking the last row in the old dataset
        3) Try need to keep dropping all the tables daily
        '''
    if connection is not None:
        for county in counties:
            try:
                con = connection.cursor()
                con.execute("DROP TABLE IF EXISTS %s;" % (county))
                con.execute("CREATE TABLE IF NOT EXISTS %s (test_date text, new_positives integer \
                          , cumal_positives integer, total_tests integer, cumal_tests integer \
                          , load_date text);" % (county))
                
                sqlite_insert_query = "INSERT INTO %s \
                                  (test_date, new_positives, cumal_positives, total_tests, cumal_tests, load_date) \
                                  VALUES (?, ?, ?, ?, ?, ?);" % (county)

                recordList = data_county(county, nested_lst_of_tuples)
                con.executemany(sqlite_insert_query, recordList)
                connection.commit()
            except Error as e:
                print(e)
    else:
        print("Error! Couldn't create the database connection.")


if __name__ == '__main__':
    main()
    
    
# Trying out Mulithreading Approach

# def createAndInsertDataIntoTables(county):
#     try:
#         con = connection.cursor()
#         con.execute("DROP TABLE IF EXISTS %s;" % (county))
#         con.execute("CREATE TABLE IF NOT EXISTS %s (test_date text, new_positives integer \
#                   , cumal_positives integer, total_tests integer, cumal_tests integer \
#                   , load_date text);" % (county))

#         sqlite_insert_query = "INSERT INTO %s \
#                           (test_date, new_positives, cumal_positives, total_tests, cumal_tests, load_date) \
#                           VALUES (?, ?, ?, ?, ?, ?);" % (county)
        
#         print(sqlite_insert_query)

#         recordList = data_for_county(county, nested_lst_of_tuples)
#         print(recordList)
#         con.executemany(sqlite_insert_query, recordList)
#         connection.commit()
#     except Error as e:
#         print(e)


# Implementing a Multithreading Approach
    '''
    if connection is not None:   
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(createAndInsertDataIntoTables, counties)
    else:
        print("Error! cannot create the database connection.")
    '''
# '''

# Checks- 

# 1) From the API data and data inserted into the tables we can check if the number of records are the same
# 2) We can check if the count of new positives from the table and data from API match at a daily level
# 3) We can check if the Cumulative Number of Positives count from the table and data from API match at a daily level
# 4) Check if the number of counties are the same in the database and from the API data
# 5) Make sure the date entered into the tables is correct and updated correctly everyday

# '''
