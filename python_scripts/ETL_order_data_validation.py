# etl script to load order items data

# importing requred libraries
import pandas as pd
import numpy as np
import os
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime


# defining logging details
log_dir = r'E:\project_stuff\birdi_systems_icn\logs'
#os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%d-%m-%y-%H-%M")
log_file = os.path.join(log_dir,f'ETL_order_data_validation_{timestamp}.txt')
with open(log_file, 'w') as file:
    file.write(f"Process started at {timestamp}\n")


# file preparation
location = r'E:\project_stuff\birdi_systems_icn\raw_datasets'
dataset = r"\order_items.csv"


# define dataset name:
order_items= location+dataset
orders = r'E:\project_stuff\birdi_systems_icn\raw_datasets\orders.csv'
df_orders = pd.read_csv(orders)


# defining db attributes
host = '127.0.0.1'
user = 'root'
password = 'newpass2024'
database = 'order_data_validation'


# logging database details
with open(log_file, 'a') as file:
    file.write(f'''below parametres passed.
        host = {host}
        user = {user}
        password = {password}
        database = {database}
        ''')

# defining insert query
insert_query = '''
    insert into orders(order_id,
                       product_id,
                       quantity)
                values(%s, %s, %s) '''


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
    return return_value


# functions to handle tranformations:
# load data
def open_dataset(order_items):
    df_order_items  = pd.read_csv(order_items)
    return df_order_items

# check and handle null values:
def check_and_handle_nulls(df_order_items):
    # check if dataset has null values, if yes prceed to handle 
    if df_order_items.isnull().values.any():
        with open(log_file, 'a') as file:
            file.write('null found performing null removal\n')
        # spaerate out nulls and put into dataset then delete nulls from main dataset
        df_null_records = df_order_items[df_order_items.isnull().any(axis=1)]
        df_null_records.to_csv(r'E:\project_stuff\birdi_systems_icn\errors\null_records.csv', index=False)
        with open(log_file, 'a') as file:
            file.write('null records written to error file\n')
        df_null_check=df_order_items.dropna()
        return df_null_check
    
    else:
        # no nulls found return dataset
        #print('no nulls returning dataset')
        df_null_check = df_order_items
        return df_null_check

# check duplicates
def check_and_handle_dup(df_null_check):
    if df_null_check.duplicated().values.any():
        with open(log_file, 'a') as file:
            file.write('dup found performing dup removal\n')
        df_dup_check=df_null_check.drop_duplicates(subset=['order_id','product_id','quantity'], keep='first')
        dup_count = len(df_null_check)-len(df_dup_check)
        with open(log_file, 'a') as file:
            #file.write('\n')
            file.write(f'duplicates removed = {str(dup_count)}\n')
        return df_dup_check
    else:
        with open(log_file, 'a') as file:
            file.write('no dup found returning main dataset\n')
        df_dup_check = df_null_check 
        return df_dup_check 

# check invalid records:
# includes - negative order qauntity, non zero negative integers in any of the columns
def data_validation(df_dup_check):
    if (df_dup_check['quantity'] < 0).any():
        with open(log_file, 'a') as file:
            file.write('invalid data found\n')
        df_valid = df_dup_check.copy()
        df_valid = df_valid[
            (df_valid['order_id'] > 0) &
            (df_valid['product_id'] > 0) &
            (df_valid['quantity'] > 0) 
        ] 
        # spaerating invalid records to error file
        df_invalid_reords = df_dup_check[~df_dup_check.index.isin(df_valid.index)]
        df_invalid_reords.to_csv(r'E:\project_stuff\birdi_systems_icn\errors\invalid_records.csv', index=False)
        with open(log_file, 'a') as file:
            file.write('invalid records written to error file\n')
        return df_valid
    else:
        with open(log_file, 'a') as file:
            file.write('no invalid records, returning main dataset\n')
        df_valid = df_dup_check 
        return df_valid

# function to handle data load:
def insert_data(df_valid):
    sql_connection = None
    cursor = None
    try:
        sql_connection = mysql.connector.connect(
            host = host,
            user = user,
            password = password,
            database = database
        )
        cursor = sql_connection.cursor()
        insert_data = [tuple(row) for row in df_valid.itertuples(index=False, name=None)]
        with open(log_file, 'a') as file:
            file.write('inserting records\n')
        cursor.executemany(insert_query,insert_data)
        sql_connection.commit()
        with open(log_file, 'a') as file:
            file.write('commited successfuly\n')
        # Log inserted records and count
        with open(log_file, 'a') as file:
            #for row in df_valid.itertuples(index=False, name=None):
                #file.write(str(row) + '\n')
            file.write(f'total records inserted {cursor.rowcount}')
        cursor.close()
        sql_connection.close()
    except mysql.connector.Error as e:
        print(f'error occured: {e}')
        with open(log_file, 'a') as file:
            file.write(f'Error occurred: {e}\n')
    if cursor:
        cursor.close()
    if sql_connection and sql_connection.is_connected():
        sql_connection.close()
    return None 

  
df_order_items  = open_dataset(order_items)
df_null_check = check_and_handle_nulls(df_order_items)
df_dup_check = check_and_handle_dup(df_null_check)
df_valid = data_validation(df_dup_check)

# loading data:
if os.path.exists(location):
    with open(log_file, 'a') as file:
        file.write('\n')
        file.write(f'path specified= {location}\n')
        
    try:
        # if path exist check file avaliabity:
        #  and load data into dataframe
        df_order_items  = open_dataset(order_items)
        # on sucessful file reading the tranformation start below
        df_null_check = check_and_handle_nulls(df_order_items)
        df_dup_check = check_and_handle_dup(df_null_check)
        df_valid = data_validation(df_dup_check)

        if check_connection(host,user,password,database) is not None:
            with open(log_file, 'a') as file:
                file.write('Connection Status= Success\n')
                file.write('Loading data\n')
            orders = r'E:\project_stuff\birdi_systems_icn\raw_datasets\orders.csv'
            df_orders = pd.read_csv(orders)
            df_valid['order_id']=df_valid['order_id'].astype('int64')
            df_valid = df_valid[df_valid['order_id'].isin(df_orders['order_id'])]
            insert_data(df_valid)
    except PermissionError:
        print("can not access file")
    except pd.errors.EmptyDataError:
        print('Input File is empty')
    except pd.errors.ParserError:
        print('invalid input file format')
    except Exception as e:
        print('Unexpected error occured: {}'.format(e))
else:
    print('"{}" specified file not found'.format(dataset))