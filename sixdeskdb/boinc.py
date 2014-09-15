#!/usr/bin/env python

# python daemon for storing boinc files directly to central Db in background
# done by Moonis Javed (monis.javed@gmail.com)
# This stores the boinc files in central DB in background
# DO NOT USE THIS DIRECTLY
# 
# NOTA: please use python version >=2.6 import time

from deskdb import SixDeskDB
import MySQLdb
from warnings import filterwarnings
from config import *
import sys
import time

try:
  args = sys.argv[:1]
  a = SixDeskDB(argv[0])
  filterwarnings('ignore', category = Warning)
    # conn = connect(args,user,password)
    # sql = "create database if not exists %s"
    # conn.cursor().execute(sql%(a[0]))
  conn = MySQLdb.connect(host,user,password,db)
  # conn.text_factory=str
  print "Opened database successfully"
  while True:
    a.st_boinc(conn)
    time.sleep(600)
except MySQLdb.Error as err:
  print "error {}".format(err)
  exit(1) 
