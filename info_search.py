#!/user/bin/env python
# encoding: utf-8
"""
"""
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import pika
import sqlite3
from sqlite3 import Error
import pandas as pd
import json as json

#run in terminal before:rabbitmq-server
DB_FILE = "/Users/hagaimanor/Downloads/chinook.db"
message_result =""

csv_file_query1 = "/Users/hagaimanor/Downloads/query1_to_csv.csv"
csv_file_query2 = "/Users/hagaimanor/Downloads/query2_to_csv.csv"
json_file_query3 = "/Users/hagaimanor/Downloads/query3_to_json.json"
xml_file_query4 = "/Users/hagaimanor/Downloads/query4_to_xml.xml"




query_1 = "SELECT country,count(invoiceid) AS purchased_number FROM customers c " \
          " inner join invoices i on c.customerid = i.customerid Group by 1"
query_2 = "SELECT country,count(*) AS purchased_items_number FROM customers c  " \
          " inner join invoices i on c.customerid = i.customerid " \
          " inner join invoice_items ii on i.invoiceid = ii.invoiceid Group by 1"
query_3 = "SELECT country,title FROM customers c" \
          " inner join invoices i on c.customerid = i.customerid " \
          " inner join invoice_items ii on i.invoiceid = ii.invoiceid" \
          " inner join tracks t on t.trackid = ii.trackid"\
          " inner join albums a on a.albumid = t.albumid" \
          " Group by 1,2"
query_4 = "SELECT title,country,SUBSTR(invoicedate,1,4) AS year,count(i.invoiceid) AS Sales_Amount FROM customers c" \
          " inner join invoices i on c.customerid = i.customerid " \
          " inner join invoice_items ii on i.invoiceid = ii.invoiceid" \
          " inner join tracks t on t.trackid = ii.trackid"\
          " inner join albums a on a.albumid = t.albumid" \
          " inner join genres g on g.genreid = t.genreid" \
          " Where g.name = 'Rock' and SUBSTR(invoicedate,1,4) >= <YEAR> and country = <COUNTRY> " \
          " Group by 1,2" \
          " order by 1,2" \





def write_dict_to_json(dict,file_path):
    json_data = json.dumps(dict)
    f = open(file_path, "w")
    f.write(json_data)
    f.close()


def send_message(db_path,country,year):
    credentials = pika.PlainCredentials(username='guest', password='guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='RABBITMQ')
    channel.basic_publish(exchange='',
                          routing_key='RABBITMQ',
                          body=db_path+','+country+','+str(year))
    print(" [x] Sent 'RABBITMQ queue'")
    connection.close()



def get_message():
    credentials = pika.PlainCredentials(username='guest', password='guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='RABBITMQ')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        global message_result
        message_result = body
        # create a string variable to hold the whole rabbit message body
        string_body = str(body)
        # split the string created above based on the message seporator enclosed in '' below
        message_vars = string_body.split(',')
        # define names for the variables which are seporated from the string_body in the step above and assign then to an array
        mv1, mv2, mv3, = message_vars[0], message_vars[1], message_vars[2]
        if body == message_result:
            channel.stop_consuming()

    channel.basic_consume(
        queue='RABBITMQ', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    print("Starts consuming.")
    channel.start_consuming()
    print("I'm done.")


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return None




def select_all_tasks(conn,query,operation,operation_type,write_to_file_path,year_value = None,country_value = None,table = None):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """

    def upload_data_to_sqlite(table, df):

        tbl_name = table
        print(tbl_name)
        wildcards = ','.join(['?'] * len(df.columns))
        data = [tuple(x) for x in df.values]

        cur.execute("drop table if exists %s" % tbl_name)

        col_str = '"' + '","'.join(df.columns) + '"'
        cur.execute("create table %s (%s)" % (tbl_name, col_str))

        cur.executemany("insert into %s values(%s)" % (tbl_name, wildcards), data)

    if operation == 4:
        if year_value:
            query = query.replace('<YEAR>', "'"+year_value+"'")
        if country_value:
            query = query.replace('<COUNTRY>', "'"+country_value+"'")
        else:
            query = query.replace('<COUNTRY>', """country""")
    cur = conn.cursor()
    cur.execute(query)
    df = pd.read_sql_query(query, conn)
    if operation == 1:
        if operation_type == 'write_file':
            df.to_csv(write_to_file_path,index=False)

        if operation_type == 'insert_into_db':
            upload_data_to_sqlite(table, df)

    elif operation == 2:
        if operation_type == 'write_file':
            df.to_csv(write_to_file_path,index=False)
        if operation_type == 'insert_into_db':
            upload_data_to_sqlite(table, df)
    elif operation == 3:
        if operation_type == 'write_file':
            country_list = df.Country.unique()
            dict = {}
            for row_country in country_list:
                if row_country not in dict:
                    dict[row_country] = []
                for index_title, row_title in df.iterrows():
                    if row_country == row_title["Country"]:
                        dict[row_country].append(row_title["Title"])
            write_dict_to_json(dict, write_to_file_path)
    elif operation == 4:
        if operation_type == 'write_file':
            def to_xml(df, filename=None, mode='w'):
                def row_to_xml(row):
                    sales_amount = row['Sales_Amount']
                    title = row['Title']
                    country = row['Country']
                    year = row['year']
                    xml = '<event album_name="{0}" country="{1}" year="{2}" sales_amount="{3}"></event>'.format(title, country ,year ,sales_amount)
                    # xml = '<albums_info1111>'+xml+'</albums_info1111>'
                    return xml
                res = '<albums_info>'+' '.join(df.apply(row_to_xml, axis=1))+'</albums_info>'
                if filename is None:
                    return res
                with open(filename, mode) as f:
                    f.write(res)

            to_xml(df, write_to_file_path, mode='w')
        if operation_type == 'insert_into_db':
            upload_data_to_sqlite(table, df)
        rows = cur.fetchall()
    rows = cur.fetchall()
    # for row in rows:
    #     print(row)



def test_inserted_tables(conn,query):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(query)


    rows = cur.fetchall()

    for row in rows:
        print(row)

def main():
    dest_table_query1 = "query1_table"
    dest_table_query2 = "query2_table"
    dest_table_query4 = "query4_table"
    print("Hi, Please enter the following info, DB path (example : /Users/hagaimanor/Downloads/chinook.db ), year,country")
    db_path = raw_input('Please enter DB path: ')
    year = int(raw_input('Please enter year: '))
    country = raw_input('Please enter country: ')

    print("""The program will execute the following modules
             "Create RabbitMQ queue with parameters
             "Validate the queue and create files with data from the queries
             "Insert data to tables from the queries" 
             "Extra : validate the data that was inserted to the DB""")

    print(""" 
             Create queue
             ----------------------------------------
            """)
    send_message(DB_FILE,country,year)
    print(""" 
             Check queue
             ----------------------------------------
            """)
    get_message()
    att_list = message_result.split (',')

    db_path = att_list[0]
    country = att_list[1]
    year = att_list[2]
    conn = create_connection(db_path)
    #Write data to files
    print(""" 
             Write data to files
             ----------------------------------------
            """)
    select_all_tasks(conn, query_1, 1, 'write_file', csv_file_query1)
    select_all_tasks(conn, query_2, 2, 'write_file', csv_file_query2)
    select_all_tasks(conn,query_3,3,'write_file',json_file_query3)
    select_all_tasks(conn,query_4,4,'write_file',xml_file_query4,year,country)

    #insert into database the data
    print(""" 
             Write data to DB
             ----------------------------------------
            """)
    select_all_tasks(conn,query_1,1,'insert_into_db',csv_file_query1,table = dest_table_query1)
    select_all_tasks(conn,query_2,2,'insert_into_db',csv_file_query2,table = dest_table_query2)
    select_all_tasks(conn,query_4,4,'insert_into_db',xml_file_query4,year,country,table = dest_table_query4)

    query1 = "SELECT * FROM query1_table"
    query2 = "SELECT * FROM query2_table"
    query4 = "SELECT * FROM query4_table"
    print(""" 
             Test tables within the DB
             ----------------------------------------
            """)
    test_inserted_tables(conn, query1)
    print("------------------------")
    test_inserted_tables(conn, query2)
    print("------------------------")
    test_inserted_tables(conn, query4)

#XML validator linkhttps://codebeautify.org/xmlvalidator


if __name__ == "__main__":
    main()

