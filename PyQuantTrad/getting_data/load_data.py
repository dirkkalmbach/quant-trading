#%%
import os
import sys
import pandas as pd
import psycopg2
import os
import matplotlib
from sqlalchemy import create_engine
from tqdm import tqdm_notebook


def csv_to_close(csv_filepath, field_names):

    """Reads in data from a csv file and produces a DataFrame with close data.
    
    Parameters
    ----------
    csv_filepath : str
        The name of the csv file to read
    field_names : list of str
        The field names of the field in the csv file

    Returns
    -------
    close : DataFrame
        Close prices for each ticker and date
    """
    
    data = pd.read_csv(csv_filepath, names=field_names)
    data = data.pivot(index='date', columns='ticker', values='close')
    
    return data


# POSTGRES DATABASE STUFF
# =======================
#%%
def connect_to_db(host="localhost", database="trading", user="dirkkalmbach"):
    
    try:
    # Create a database connection
        user = os.environ.get("db_user")
        password = os.environ.get("db_password")
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password="")

        #Creating a cursor object using the cursor() method
        cursor = conn.cursor()
        cursor.execute("select version()")
        data = cursor.fetchone()
        return cursor, data
    except:
        print("Could not connect to database. 😞")


def get_tables():
    """Print all tables in postgres database >trading<.
    
    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    cursor = connect_to_db()[0]
    cursor.execute("select current_database()")
    db_name = cursor.fetchone()[0]

    cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    print("TABLES IN DATABSE {}:".format(db_name))
    for table in cursor.fetchall():
        print(table)
