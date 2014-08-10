#!/usr/bin/python

# python implementation of Sixdesk synchronisation between
# local user DB and centralized DB
# done by Moonis Javed (monis.javed@gmail.com)
# This synchronizes the local database with centralized database 
# Below are indicated thing that need to be edited by hand. 
# You have to use it from main like
# python mysql.py <host> <user> <pass> <db> <study> 
# python mysql.py <host> <user> <pass> <db> <study> bo
# bo switch is to store boinc fort.10 files directly to central DB
# 
# NOTA: please use python version >=2.6 

<<<<<<< HEAD
from sqltable import SQLTable
from MySQLdb import connect,Error
=======
from sqltable import *
from tables import *
from MySQLdb import *
>>>>>>> 35ae74d6ec7f2e965c0319c59e750499eec189aa
import sqlite3
import tables
from warnings import filterwarnings
import sys
import os
# from contextlib import closing

try:
  args = sys.argv[1:]
  # a = os.environ['LOGNAME']
  host = args[0]
  user = args[1]
  password = args[2]
  db = args[3]
  if len(args) > 5:
    if args[5] == 'bo':
      bo = True
  filterwarnings('ignore', category = Warning)
  # conn = connect(args,user,password)
  # sql = "create database if not exists %s"
  # conn.cursor().execute(sql%(a[0]))
  conn = connect(host,user,password,db)
  if not args[4].endswith('.db'):
    args[4] += '.db'
  if not os.path.isfile(args[4]):
    print 'db not found'
    exit(0)
  conn1 = sqlite3.connect(args[4])
  conn1.text_factory=str
  conn.autocommit(False)
  # cur = conn.cursor()
  cur1 = conn1.cursor()
except Error as err:
  print "error {}".format(err)
  exit(1) 
print "Opened database successfully"

try:
<<<<<<< HEAD
  cur = conn.cursor()
  cur.execute("drop table if exists env")
  cur.execute("drop table if exists mad6t_run")
  cur.execute("drop table if exists mad6t_run2")
  cur.execute("drop table if exists mad6t_results")
  cur.execute("drop table if exists six_beta")
  cur.execute("drop table if exists six_input")
  cur.execute("drop table if exists six_results")
  cur.execute("drop table if exists files")

  cols=SQLTable.cols_from_fields(tables.Env.fields)
  cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
  cols = ['study VARCHAR(128)'] + cols
  tab = SQLTable(conn,'env',cols) 
  sql = "select a.value,b.* from env as a,env as b where a.keyname = 'LHCDescrip'"  
  a = [list(i) for i in list(cur1.execute(sql))]
  cur.executemany("insert into env values(%s,%s,%s)",a)
  conn.commit()

  cols=SQLTable.cols_from_fields(tables.Mad_Run.fields)
  cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
  cols = ['study VARCHAR(128)'] + cols
  tab = SQLTable(conn,'mad6t_run',cols) 
  sql = "select a.value,b.* from env as a,mad6t_run as b where a.keyname = 'LHCDescrip'"  
  a = [list(i) for i in list(cur1.execute(sql))]
  sql = "insert into mad6t_run values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
  # print len(a[0])
  cur.executemany(sql,a)
  conn.commit()

  # cols=SQLTable.cols_from_fields(tables.Mad_Run2.fields)
  # cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
  # cols = ['study VARCHAR(128)'] + cols
  # tab = SQLTable(conn,'mad6t_run2',cols)  
  # sql = "select a.value,b.* from env as a,mad6t_run2 as b where a.keyname = 'LHCDescrip'" 
  # a = [list(i) for i in list(cur1.execute(sql))]
  # sql = "insert into mad6t_run2 values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
  # cur.executemany(sql,a)
  # conn.commit()

  cols=SQLTable.cols_from_fields(tables.Mad_Res.fields)
  cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
  cols = ['study VARCHAR(128)'] + cols
  tab = SQLTable(conn,'mad6t_results',cols) 
  sql = "select a.value,b.* from env as a,mad6t_results as b where a.keyname = 'LHCDescrip'"  
  a = [list(i) for i in list(cur1.execute(sql))]
  sql = "insert into mad6t_results values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
  cur.executemany(sql,a)
  conn.commit()

  cols=SQLTable.cols_from_fields(tables.Six_Be.fields)
  cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
  cols = ['study VARCHAR(128)'] + cols
  tab = SQLTable(conn,'six_beta',cols)  
  sql = "select a.value,b.* from env as a,six_beta as b where a.keyname = 'LHCDescrip'" 
  a = [list(i) for i in list(cur1.execute(sql))]
  sql = "insert into six_beta values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
  cur.executemany(sql,a)
  conn.commit()

  cols=SQLTable.cols_from_fields(tables.Six_In.fields)
  cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
  cols = ['study VARCHAR(128)'] + cols
  tab = SQLTable(conn,'six_input',cols) 
  sql = "select a.value,b.* from env as a,six_input as b where a.keyname = 'LHCDescrip'"  
  a = [list(i) for i in list(cur1.execute(sql))]
  sql = "insert into six_input values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
  cur.executemany(sql,a)
  conn.commit()

  cols=SQLTable.cols_from_fields(tables.Six_Res.fields)
  cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
  cols = ['study VARCHAR(128)'] + cols
  tab = SQLTable(conn,'six_results',cols) 
  sql = "select a.value,b.* from env as a,six_results as b where a.keyname = 'LHCDescrip'"  
  a = [list(i) for i in list(cur1.execute(sql))]
  # cur.execute("show variables like 'max_%'")
  cur = conn.cursor()
  cur.execute("set global max_allowed_packet=209715200;")
  cur.execute("set global wait_timeout=120;")
  cur.execute("set global net_write_timeout=120;")
  cur.execute("set global net_read_timeout=120;")
  # print list(cur)
  # print len(a),(sys.getsizeof(a)/(1024.0))
  sql = "insert into six_results values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
  for _ in xrange(len(a)/150000):
    cur.executemany(sql,a[:150000])
    a = a[150000:]
    conn.commit()
  cur.executemany(sql,a)
  conn.commit()

  cols=SQLTable.cols_from_fields(tables.Files.fields)
  cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
  cols = ['study VARCHAR(128)'] + cols
  tab = SQLTable(conn,'files',cols) 
  sql = "select a.value,b.* from env as a,files as b where a.keyname = 'LHCDescrip'"  
  a = [list(i) for i in list(cur1.execute(sql))]
  sql = "insert into files values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
  cur.executemany(sql,a)
  conn.commit()
  if bo:
    cmd = "python boinc.py %s"%(args[4])
    os.spawnl(os.P_NOWAIT, cmd)
=======
	cur = conn.cursor()
	cur.execute("drop table if exists env")
	cur.execute("drop table if exists mad6t_run")
	cur.execute("drop table if exists mad6t_run2")
	cur.execute("drop table if exists mad6t_results")
	cur.execute("drop table if exists six_beta")
	cur.execute("drop table if exists six_input")
	cur.execute("drop table if exists six_results")
	cur.execute("drop table if exists files")

	cols=SQLTable.cols_from_fields(testtables.Env.fields)
	cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
	cols = ['study VARCHAR(128)'] + cols
	tab = SQLTable(conn,'env',cols)	
	sql = "select a.value,b.* from env as a,env as b where a.keyname = 'LHCDescrip'"	
	a = [list(i) for i in list(cur1.execute(sql))]
	cur.executemany("insert into env values(%s,%s,%s)",a)
	conn.commit()

	cols=SQLTable.cols_from_fields(testtables.Mad_Run.fields)
	cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
	cols = ['study VARCHAR(128)'] + cols
	tab = SQLTable(conn,'mad6t_run',cols)	
	sql = "select a.value,b.* from env as a,mad6t_run as b where a.keyname = 'LHCDescrip'"	
	a = [list(i) for i in list(cur1.execute(sql))]
	sql = "insert into mad6t_run values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
	# print len(a[0])
	cur.executemany(sql,a)
	conn.commit()

	# cols=SQLTable.cols_from_fields(testtables.Mad_Run2.fields)
	# cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
	# cols = ['study VARCHAR(128)'] + cols
	# tab = SQLTable(conn,'mad6t_run2',cols)	
	# sql = "select a.value,b.* from env as a,mad6t_run2 as b where a.keyname = 'LHCDescrip'"	
	# a = [list(i) for i in list(cur1.execute(sql))]
	# sql = "insert into mad6t_run2 values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
	# cur.executemany(sql,a)
	# conn.commit()

	cols=SQLTable.cols_from_fields(testtables.Mad_Res.fields)
	cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
	cols = ['study VARCHAR(128)'] + cols
	tab = SQLTable(conn,'mad6t_results',cols)	
	sql = "select a.value,b.* from env as a,mad6t_results as b where a.keyname = 'LHCDescrip'"	
	a = [list(i) for i in list(cur1.execute(sql))]
	sql = "insert into mad6t_results values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
	cur.executemany(sql,a)
	conn.commit()

	cols=SQLTable.cols_from_fields(testtables.Six_Be.fields)
	cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
	cols = ['study VARCHAR(128)'] + cols
	tab = SQLTable(conn,'six_beta',cols)	
	sql = "select a.value,b.* from env as a,six_beta as b where a.keyname = 'LHCDescrip'"	
	a = [list(i) for i in list(cur1.execute(sql))]
	sql = "insert into six_beta values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
	cur.executemany(sql,a)
	conn.commit()

	cols=SQLTable.cols_from_fields(testtables.Six_In.fields)
	cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
	cols = ['study VARCHAR(128)'] + cols
	tab = SQLTable(conn,'six_input',cols)	
	sql = "select a.value,b.* from env as a,six_input as b where a.keyname = 'LHCDescrip'"	
	a = [list(i) for i in list(cur1.execute(sql))]
	sql = "insert into six_input values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
	cur.executemany(sql,a)
	conn.commit()

	cols=SQLTable.cols_from_fields(testtables.Six_Res.fields)
	cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
	cols = ['study VARCHAR(128)'] + cols
	tab = SQLTable(conn,'six_results',cols)	
	sql = "select a.value,b.* from env as a,six_results as b where a.keyname = 'LHCDescrip'"	
	a = [list(i) for i in list(cur1.execute(sql))]
	# cur.execute("show variables like 'max_%'")
	cur = conn.cursor()
	cur.execute("set global max_allowed_packet=209715200;")
	cur.execute("set global wait_timeout=120;")
	cur.execute("set global net_write_timeout=120;")
	cur.execute("set global net_read_timeout=120;")
	# print list(cur)
	# print len(a),(sys.getsizeof(a)/(1024.0))
	sql = "insert into six_results values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
	for _ in xrange(len(a)/150000):
		cur.executemany(sql,a[:150000])
		a = a[150000:]
		conn.commit()
	cur.executemany(sql,a)
	conn.commit()

	cols=SQLTable.cols_from_fields(testtables.Files.fields)
	cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
	cols = ['study VARCHAR(128)'] + cols
	tab = SQLTable(conn,'files',cols)	
	sql = "select a.value,b.* from env as a,files as b where a.keyname = 'LHCDescrip'"	
	a = [list(i) for i in list(cur1.execute(sql))]
	sql = "insert into files values (%s)"%(','.join("%s " for _ in xrange(len(cols))))
	cur.executemany(sql,a)
	conn.commit()
	if bo:
		cmd = "python boinc.py %s"%(args[4])
		os.spawnl(os.P_NOWAIT, cmd)
>>>>>>> 35ae74d6ec7f2e965c0319c59e750499eec189aa
except Error as err:
  print("Something went wrong: {}".format(err))
conn.close()