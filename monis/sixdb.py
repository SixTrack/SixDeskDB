import sqlite3,time
import psutil, os, re, gzip
from sys import platform as _platform
import sys
import sixdeskdir

if _platform == "linux" or _platform == "linux2":
    	os.nice(-20)
elif _platform == "win32":
	#set high priority
	p = psutil.Process(os.getpid())
	p.set_nice(psutil.HIGH_PRIORITY_CLASS)

#time initialisation
#start = time.time()
def col_name(cur,table):
	sql = 'pragma table_info(%s)'%(table)
	cur.execute(sql)
	name = ""
	for row in cur.fetchall():
		if not 'AUTO_INCREMENT' in row[2]:
			name += str(row[1])+","
	return name[:-1]

def col_count(cur,table):
	sql = 'pragma table_info(%s)'%(table)
	cur.execute(sql)
	return len(cur.fetchall())

def dict_to_list(dict):
	lst = []
	for i in sorted(dict.keys()):
		for j in dict[i]:
			if isinstance(j,list):
				lst.append(j)
			else:
				lst.append(dict[i])
				break
	return lst

def store_dict(cur,colName,table,data):
	#cur = db.cursor()
	cur.execute("select max(%s) from %s"% (colName,table))
	temp = cur.fetchone()[0]
	if temp is not None:
		newid = int(temp) + 1
	else:	
		newid = 1
	lst = []
	for head in data:
	  lst.append([newid,head,data[head]])
	sql = "INSERT into %s values(?,?,?)"%(table)
	cur.executemany(sql,lst)
	return newid

def load_dict(db,table,idnum,idcol):
	cur = db.cursor()
	sql='SELECT key,value from %s WHERE %s=%d'%(table,idcol,idnum)
	return dict(*zip(*list(cur.execute(sql))))

#opening connection
def sixdb_createdb(studyName,studyDir):
	try:
		db = studyName+".db"
		conn = sqlite3.connect(db,isolation_level="IMMEDIATE")
		cur = conn.cursor()
	except sqlite3.Error:
		print "error"
		return 0
	print "Opened database successfully"
	#creating table
	sql = "CREATE table if not exists env(env_id int, key string, value string)"
	cur.execute(sql)
	sql = """create table if not exists mad6t_run(env_id int, run_id string, 
		seed int , mad_in blob, mad_out blob, mad_lsf blob)"""
	cur.execute(sql)
	sql = """create table if not exists mad6t_run2(env_id int, 
		fort3aux blob, fort3mad blob, fort3mother1 blob, 
		fort3mother2 blob)"""
	cur.execute(sql)
	sql = """create table if not exists mad6t_results(env_id int, seed int,
		 fort2 blob, fort8 blob, fort16 blob)"""
	cur.execute(sql)
	sql = """create table if not exists six_beta(env_id int, seed int, 
		tunex double, tuney double, beta11 double, beta12 double, beta22 double,
		beta21 double, qx double,qy double,dqx double, dqy double, x double,
		xp double,y double, yp double, sigma double, delta double, emitn double, 
		gamma double, deltap double, qx1 double, qy1 double, qx2 double,
		qy2 double)"""
	cur.execute(sql)
	sql = """create table if not exists six_input(
		id integer primary key,env_id int, seed int, simul string,
		tunex double, tuney double, amp1 int, amp2 int, angle int, turns string,
		fort3 blob)"""
	cur.execute(sql)
	sql = """create table if not exists six_results(
		six_input_id int ,row_num int,
		""" + ','.join('o'+str(i)+' double' for i in xrange(1,61)) + """, 
		primary key(six_input_id,row_num))"""
	cur.execute(sql)

	#sql = """SELECT name FROM sqlite_master WHERE type='table' 
	#	AND name='outputs'"""
	#cur.execute(sql)
	#for row in cur:
	#	print row

	#PRAGMA settings
	cur.execute("PRAGMA synchronous = OFF")
	cur.execute("PRAGMA journal_mode = MEMORY")
	cur.execute("PRAGMA auto_vacuum = FULL")
	cur.execute("PRAGMA temp_store = MEMORY")
	cur.execute("PRAGMA count_changes = OFF")	
	cur.execute("PRAGMA mmap_size=2335345345") 
	
	print ' storing environment variables'
	env_var = {}
	flag = 0
	for dirName, subdirList, fileList in os.walk(studyDir+'/studies'):
  	    for files in fileList:
	        if 'sixdeskenv' in files or 'sysenv' in files:
	        	env_var = sixdeskdir.parse_env(dirName)
	        	newid = store_dict(cur,"env_id","env",env_var)
	        	conn.commit()
	        	break

	
	print 'storing mad6t_run'
	rows = {}
	#print newid
	col = col_count(cur,'mad6t_run2')
	for dirName, subdirList, fileList in os.walk(studyDir+'/sixtrack_input'):
		if 'mad.dorun' in dirName:
			for files in fileList:
				if not ( files.endswith('.mask') or 'out' in files 
						or files.endswith('log') or files.endswith('lsf') ):
					seed = files.split('.')[-1]
					run_id = dirName.split('/')[-1]
					mad_in = sqlite3.Binary(open(
												os.path.join(dirName,files),'r'
												).read()
											)

					out_file = files.replace('.','.out.')
					mad_out = sqlite3.Binary(open(
												os.path.join(dirName,out_file),'r'
												).read()
											)
					lsf_file = 'mad6t_'+seed+'.lsf'
					mad_lsf = sqlite3.Binary(open(
												os.path.join(dirName,lsf_file),'r'
												).read()
											)
					rows[seed] = []
					rows[seed].append([newid,seed,mad_in,mad_out,mad_lsf])
					#print files,out_file,lsf_file
			if rows:
				lst = dict_to_list(rows)
				sql = "INSERT into mad6t_results values(" 
				sql	+= ','.join("?"*col) + ")"
				cur.executemany(sql,lst)
				conn.commit()
				rows = {}

	
	print 'storing mad6t_run2'
	fort3 = {}
	#print newid
	col = col_count(cur,'mad6t_run2')
	for dirName, subdirList, fileList in os.walk(studyDir+'/sixtrack_input'):
		for files in fileList:
			if 'fort.3' in files and not files.endswith('.tmp'):
				fort3[files.replace('fort.3.','')] = sqlite3.Binary(open(
											os.path.join(dirName,files),'r'
																	).read()
																)
		if fort3 and len(fort3.keys()) == 4:
			sql = "INSERT into mad6t_run2 values(" + ','.join("?"*col) + ")"
			cur.execute(sql,[newid,fort3['aux'],fort3['mad'],
							fort3['mother1'],fort3['mother2']
							]
						)
			conn.commit()
			fort3 = {}
	
	
	print 'storing mad6t_results'
	rows = {}
	col = col_count(cur,'mad6t_results')
	for dirName, subdirList, fileList in os.walk(studyDir+'/sixtrack_input'):
		for files in fileList:
			if 'fort' in files and files.endswith('.gz'):
				head = int(files.split('_')[1].replace('.gz',''))
				if head not in rows.keys():
					rows[head] = [newid,head]
				if '2' in files:
					rows[head].insert(2,sqlite3.Binary(open(
											os.path.join(dirName,files),'r'
													).read()
												))
				elif '8' in files:
					rows[head].insert(3,sqlite3.Binary(open(
											os.path.join(dirName,files),'r'
													).read()
												))
				else:
					rows[head].extend([sqlite3.Binary(open(
											os.path.join(dirName,files),'r'
													).read()
												)])
		if rows:
			lst = dict_to_list(rows)
			sql = "INSERT into mad6t_results values(" + ','.join("?"*col) + ")"
			cur.executemany(sql,lst)
			conn.commit()
			rows = {}


	print 'storing six_beta values'
	rows = {}
	col = col_count(cur,'six_beta')
	beta = six = gen = []
	for dirName, subdirList, fileList in os.walk(studyDir+'/track'):
		for files in fileList:
			if 'general_input' in files:
				with open(os.path.join(dirName,files),'r') as FileObj:
					for lines in FileObj:
						gen = lines.split()
			if 'betavalues' in files or 'sixdesktunes' in files:
				dirn = dirName.replace('./files/track/','')
				dirn = dirn.split('/')
				seed = int(dirn[0])
				tunex,tuney = [i for i in dirn[2].split('_')]
				if not seed in rows.keys():
					rows[seed] = []
					#print rows.keys()
				temp = [newid,seed,tunex,tuney]
				if 'betavalues' in files:
					f = open(os.path.join(dirName,files),'r')
					beta = [float(i) for i in f.read().split()]
				if 'sixdesktunes' in files:
					f = open(os.path.join(dirName,files),'r')
					six = [float(i) for i in f.read().split()]
				f.close()
			if beta and temp and six:
				rows[seed].append(temp+beta+gen+six)
				beta = temp = six = []
		
		if rows:
			lst = dict_to_list(rows)
			#print lst
			sql = "INSERT into six_beta values(" + ','.join("?"*col) + ")"
			cur.executemany(sql,lst)
			conn.commit()
			rows = {}


	print 'storing six_input values'
	rows = []
	cur.execute('SELECT id from six_input order by id DESC limit 1')
	count = cur.fetchone()
	if count is not None:
		count += 1
	else:
		count = 1
	col = col_count(cur,'six_input')
	for dirName, subdirList, fileList in os.walk(studyDir+'/track'):
		for files in fileList:
			if 'fort.3' in files:
				dirn = dirName.replace('./files/track/','')
				dirn = re.split('/|_|-',dirn)
				dirn = [count,newid] + dirn
				#print dirn
				dirn.extend([sqlite3.Binary(gzip.open(
											os.path.join(dirName,files),'r'
												).read()
											)])
				rows.append(dirn)				
				dirn=[]
				count += 1
				if len(rows) % 5000 == 0:
					#print len(rows)
					sql = "INSERT into six_input values (" 
					sql += ','.join('?'*col) + ")"
					#print sql
					cur.executemany(sql,rows)
					conn.commit()
					rows = []
	if rows:
		#print len(rows)
		sql = "INSERT into six_input values (" + ','.join('?'*col) + ")"
		cur.executemany(sql,rows)
		conn.commit()
		rows = []


	#six_results
	print 'storing six_results values'
	rows = []
	col = col_count(cur,'six_results')
	max = 0
	for dirName, subdirList, fileList in os.walk(studyDir+'/track'):
		for files in fileList:
			if 'fort.10' in files:
				dirn = dirName.replace('./files/track/','')
				dirn = re.split('/|_|-',dirn)
				#print len(dirn),dirn
				sql = """SELECT id from six_input where env_id=? and seed=? and 
					simul=? and tunex=? and tuney=? and amp1=? and amp2=? and 
					angle=? and turns=?"""
				cur.execute(sql,[newid]+dirn)
				six_id = cur.fetchone()
				if six_id is None:
					cur.execute("SELECT max(id) from six_input")
					six_id = cur.fetchone()[0] + 1
					if not max:
						max = six_id
					else:
						max += 1
						six_id = max
					sql = "INSERT into six_input values(" 
					sql	+= ','.join('?'*col_count(cur,'six_input')) + ")"
					cur.execute(sql,[max,newid]+dirn+[''])
					conn.commit()
					#print 'added',tuple(dirn)
				else:
					six_id = six_id[0]
				#print six_id
				#print six_id
				with gzip.open(os.path.join(dirName,files),"r") as FileObj:
					count = 1
					for lines in FileObj:
						rows.append([six_id,count]+lines.split())
						count += 1
				if len(rows) > 149999:
					#print len(rows)
					sql = "INSERT into six_results values (" 
					sql += ','.join('?'*col) + ")"
					#print sql
					#start = time.time()
					cur.executemany(sql,rows)
					conn.commit()
					#end = time.time()
					#print end-start
					rows = []
	if rows:
		#print len(rows)
		sql = "INSERT into six_results values (" + ','.join('?'*col) + ")"
		cur.executemany(sql,rows)
		conn.commit()
		rows = []
		

if __name__ == '__main__':

	if len(sys.argv) == 3:
		sixdb_createdb(sys.argv[1],sys.argv[2])
	else:
		print sys.argv
		sixdb_createdb('monis','./files')