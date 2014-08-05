import time
from SixdeskDB import *
import MySQLdb
from warnings import filterwarnings
from config import *

args = sys.argv[:1]
a = SixDeskDB(argv[0])
filterwarnings('ignore', category = Warning)
	# conn = connect(args,user,password)
	# sql = "create database if not exists %s"
	# conn.cursor().execute(sql%(a[0]))
	conn = connect(host,user,password,db)
	conn1.text_factory=str
except Error as err:
	print "error {}".format(err)
	exit(1)	
print "Opened database successfully";
while True:
	a.st_boinc(conn)
	time.sleep(600)