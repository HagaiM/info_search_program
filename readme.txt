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
python /Users/hagaimanor/Documents/PythonETLs/info_search.py

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