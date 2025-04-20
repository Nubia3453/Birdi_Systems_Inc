# importing requred libraries

import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
from datetime import datetime


# file preparation
location = r'E:\project_stuff\birdi_systems_icn\raw_datasets'
dataset = r"\payment_detail.csv"

# defining logging details
log_dir = r'E:\project_stuff\birdi_systems_icn\logs'
#os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%d-%m-%y-%H-%M")
log_file = os.path.join(log_dir,f'ETL_payment_details_{timestamp}.txt')
with open(log_file, 'w') as file:
    file.write(f"Process started at {timestamp}\n")

# define dataset name:
payments = location+dataset

# defining db attributes
host = '127.0.0.1'
user = 'root'
password = 'newpass2024'
database = 'payments'

# defining insert query
insert_query = '''
    insert into payment_details(payment_id,
                       order_id,
                       card_type,	
                       card_number,	
                       status,
                       payment_date)
                values(%s, %s, %s, %s, %s, %s) '''

# functions to handle tranformations:

# creating and loading dataframe
def open_dataset(payments):
    df_payments  = pd.read_csv(payments)
    return df_payments

# masking sensitive data
def mask_cc_number(card_number):
    card_number = str(card_number).replace('-','').replace(' ','')
    if len(card_number) == 16:
        masked_number = '*' * 12 + card_number[-4:]
    else:
        masked_number = card_number
    return masked_number

# apply masking
def apply_masking(df_payments):
    df_payments['card_number'] = df_payments['card_number'].apply(mask_cc_number)
    df_payments_mask = df_payments
    return df_payments_mask

# converting date columns
def to_datetime_convertion(df_payments_mask):
    df_payments_mask['payment_date'] = pd.to_datetime(df_payments_mask['payment_date'])
    df_payments_final = df_payments_mask
    return df_payments_final


# functions to handle sql connection:
def check_connection(host,user,password,database):
    sql_connection = None
    #cursor = None
    try:  # first try to make connection with db
        sql_connection = mysql.connector.connect(
            host = host,
            user = user,
            password = password,
            database = database
        )
        return_value = 'Connection Successful' 

    except Error as e: # if faces issues, return error and none value
        with open(log_file, 'a') as file:
            file.write(f'Error while connecting to db {e}\n')
        return_value = None

    finally:
        if sql_connection and sql_connection.is_connected():
            sql_connection.close()
    return return_value

# function to insert data
def insert_data(df_payments_final):
    try:
    # define sql connection
        sql_connection = mysql.connector.connect(
            host = host,
            user = user,
            password = password,
            database = database
        )
        #check connection
        if sql_connection.is_connected():
            with open(log_file, 'a') as file:
                file.write(str(timestamp) + " - Now connected to database\n")
        cursor = sql_connection.cursor()   # define cursor
        required_cols = ['payment_id', 'order_id', 'card_type', 'card_number', 'status', 'payment_date']
        insert_data = [tuple(row) for row in df_payments_final[required_cols].itertuples(index=False, name=None)]
        with open(log_file, 'a') as file:
            file.write(str(timestamp) + " - Now inserting Data\n")

        #insert data to db
        cursor.executemany(insert_query,insert_data)
        sql_connection.commit()
        with open(log_file, 'a') as file:
            file.write(str(timestamp) + ' - ' + str(cursor.rowcount) + " rows inserted\n")
        #print(f'{cursor.rowcount} rows inserted')
    except mysql.connector.Error as e:
        with open(log_file, 'a') as file:
            file.write(f'Error occurred: {e}\n')
    
    # closing curosr in both successful insert or failuer to avoid losses
    if cursor:
        cursor.close()
    if sql_connection and sql_connection.is_connected():
        sql_connection.close()
    return None 

# loading data:
if os.path.exists(location):
    with open(log_file, 'a') as file:
        file.write('\n')
        file.write(f'path specified= {location}\n')
        
    try:
        # if path exist check file avaliabity:
        #  and load data into dataframe
        df_payments = open_dataset(payments)
        # on sucessful file reading the tranformation start below
        df_payments_mask = apply_masking(df_payments)
        df_payments_final = to_datetime_convertion(df_payments_mask)  

        # checking on connection status before loading data
        if check_connection(host,user,password,database) is not None:
            with open(log_file, 'a') as file:
                file.write(str(timestamp) + ' - Checking connection status\n')
                file.write(str(timestamp) + ' - status - ok\n')
            insert_data(df_payments_final)
        with open(log_file, 'a') as file:
            file.write(f"Process completed at {timestamp}\n")

    # various exceptoin handling
    except PermissionError:
        with open(log_file, 'a') as file:
            file.write("can not access input file\n")
    except pd.errors.EmptyDataError:
        with open(log_file, 'a') as file:
            file.write("Input File is empty\n")
    except pd.errors.ParserError:
        with open(log_file, 'a') as file:
            file.write("invalid input file format\n")
    except Exception as e:
        with open(log_file, 'a') as file:
            file.write("Unexpected error occured: {}".format(e))
else:
    with open(log_file, 'a') as file:
        file.write("Unexpected error occured: {}".format(dataset))    