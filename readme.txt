Python version 2.7
----------------------------------------

Installation :
----------------------------------------
1.raabitMQ (MacOS: Homebrew)
1.pika
2.sqlite3
4.pandas
5.json

RabbitMQ info:
----------------------------------------
hardcoded in the script
username='guest'
password='guest'
host='localhost'

Execution:
----------------------------------------
Run from CMD your file location:
change files path:
	log_file = "/Users/hagaimanor/Documents/rabbitmq_log.txt"
	csv_file_query1 = "/Users/hagaimanor/Downloads/query1_to_csv.csv"
	csv_file_query2 = "/Users/hagaimanor/Downloads/query2_to_csv.csv"
	json_file_query3 = "/Users/hagaimanor/Downloads/query3_to_json.json"
	xml_file_query4 = "/Users/hagaimanor/Downloads/query4_to_xml.xml"


send to queue : python /Users/hagaimanor/Documents/PythonETLs/info_search_m1.py
get queue: python /Users/hagaimanor/Documents/PythonETLs/info_search_m1.py

Parameters:
The program is simple and works automatically after inserting the following parameters:
DB path :  /Users/hagaimanor/Downloads/chinook.db
Year : 2013
Country : Portugal

After the parameters are inserted the following steps are executed:
1.create queue
2.check queue and write files
3.Upload data into the database
4.Extra mile: check the data that was inserted into the DB.