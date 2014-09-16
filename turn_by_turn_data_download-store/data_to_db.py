# --------------------------------------------------------------------------------------------------------------
# INSERT TRACKING DATA INTO DB
# GIOVANNA CAMPOGIANI
# LAST MODFIIED: 16/09/2014
# This script stores the turn_by_turn tracking data downloaded from CASTOR into an SQL table called tracking
# contained in the db called 	
# dbname=studio%seedinit-seedend%ampi-ampend%numberofangles
# It also contains utilities to further manipulate the table with the turn_by_turn data
# --------------------------------------------------------------------------------------------------------------

import numpy as np
import sqlite3

def create_db(tbt_data,si,sf,ai,af,angles):
	'''creates the SQL db and inserts in it the corresponding turn by turn data
	'''
	table=[]
	seed_name=str(si)+'-'+str(sf)
	amp_nam=str(ai)+'-'+str(af)
	ang_name=str(len(angles))
	
	dbname=studio+'%'+seed_name+'%'+amp_name+'%'+ang_name
	
	tbt=sqlite3.connect('%s.db' %dbname)
	cur=tbt.cursor()

	cur.execute('''
	CREATE TABLE tracking_data(
		seed INTEGER, 
		partID INTEGER, 
		turnID INTEGER, 
		pdist REAL,
		x REAL,
		xp REAL,
		y REAL,
		yp REAL, 
		sig REAL,
		delta REAL,
		energy REAL);''')
	tbt.commit()
	
	tbt.text_factory=str

	for sed in sorted(tbt_data.keys()):
		for pID in sorted(tbt_data[sed].keys()):
			for tID in range(len(tbt_data[sed][pID])):
				pdist=tbt_data[sed][pID][tID][0]
				x=tbt_data[sed][pID][tID][1]
				xp=tbt_data[sed][pID][tID][2]
				y=tbt_data[sed][pID][tID][3]
				yp=tbt_data[sed][pID][tID][4]
				sig=tbt_data[sed][pID][tID][5]
				delta=tbt_data[sed][pID][tID][6]
				energy=tbt_data[sed][pID][tID][7]
				single_entry=(sed,pID,tID,pdist,x,xp,y,yp,sig,delta,energy)
				table.append(single_entry)

	cur.executemany('INSERT INTO tracking_data VALUES(?,?,?,?,?,?,?,?,?,?,?);',table)
	tbt.commit()
	print ('Data stored in the database %s, table: tracking_data' %dbname)
	
	cur.execute('SELECT * FROM tracking_data;')
	print cur.fetchall()
	
	cur.close()
	tbt.close()
	
	return dbname
	
