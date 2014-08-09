#!/usr/bin/python

# python implementation of Sixdesk storage using local database and creation of
# study using database
# done by Moonis Javed (monis.javed@gmail.com)
# This stores the study to a local database 
# Below are indicated thing that need to be edited by hand. 
# You have to use it from main like
# python main.py loaddir <study location> 
# python main.py loaddb <studyDB> <new location of study>
# 
# NOTA: please use python version >=2.6   

import sqlite3
import time
import os
import re
import gzip
import cStringIO
import StringIO
import sixdeskdir
import lsfqueue
import numpy as np
import matplotlib.pyplot as pl
import scipy.signal
import tables
from sqltable import *  
import copy

def load_dict(cur,table):
  sql='SELECT keyname,value from %s'%(table)
  cur.execute(sql)
  a = cur.fetchall()
  dict = {}
  for row in a:
    dict[str(row[0])] = str(row[1]) 
  return dict 

def compressBuf(file):
  '''file compression for storing in DB'''
  buf = open(file,'r').read()
  zbuf = cStringIO.StringIO()
  zfile = gzip.GzipFile(mode = 'wb',  fileobj = zbuf, compresslevel = 9)
  zfile.write(buf)
  zfile.close()
  return zbuf.getvalue()

def decompressBuf(buf):
  '''file decompression to retrieve from DB'''
  zbuf = StringIO.StringIO(buf)
  f = gzip.GzipFile(fileobj=zbuf)
  return f.read()

def is_number(s):
  try:
    float(s)
    return True
  except ValueError:
    pass

def col_count(cur, table):
  sql = 'pragma table_info(%s)' % (table)
  cur.execute(sql)
  return len(cur.fetchall())


def dict_to_list(dict):
  '''convert dictionary to list for DB insert'''
  lst = []
  for i in sorted(dict.keys()):
    for j in dict[i]:
      if isinstance(j, list):
        lst.append(j)
      else:
        lst.append(dict[i])
        break
  return lst

def store_dict(cur, colName, table, data):
  cur.execute("select max(%s) from %s" % (colName, table))
  temp = cur.fetchone()[0]
  if temp is not None:
    newid = int(temp) + 1
  else:
    newid = 1
  lst = []
  for head in data:
    lst.append([newid, head, data[head]])
  sql = "INSERT into %s values(?,?,?)" % (table)
  cur.executemany(sql, lst)
  return newid

def guess_range(l):
  '''find start,stop and setp value for list provided'''
  l=sorted(set(l))
  start=l[0]
  if len(l)>1:
    step=np.diff(l).min()
  else:
    step=None
  stop=l[-1]
  return start,stop,step

class SixDeskDB(object):
  @staticmethod
  def st_env(conn,env_var,studyDir):
    '''store environment variables to DB'''
    extra_files = []
    cols=SQLTable.cols_from_fields(tables.Env.fields)
    tab = SQLTable(conn,'env',cols,tables.Env.key)
    cols=SQLTable.cols_from_fields(tables.Files.fields)
    tab1 = SQLTable(conn,'files',cols,tables.Files.key)
    cur = conn.cursor()
    env_var['env_timestamp']=str(time.time())
    env_var1 = [[i,env_var[i]] for i in env_var.keys()]
    tab.insertl(env_var1)
    path = os.path.join(studyDir, 'sixdeskenv')
    content = sqlite3.Binary(compressBuf(path))
    path = path.replace(env_var['basedir']+'/','')
    extra_files.append([path, content])
    path = os.path.join(studyDir, 'sysenv')
    content = sqlite3.Binary(compressBuf(path))
    path = path.replace(env_var['basedir']+'/','')
    extra_files.append([path, content])
    tab1.insertl(extra_files) 

  @classmethod
  def from_dir(cls,studyDir,basedir='.',verbose=False,dryrun=False):
    '''create local Database for storing study'''
    cls = None
    if not (os.path.exists(studyDir+'/sixdeskenv') and \
      os.path.exists(studyDir+'/sysenv')):
      print "sixdeskenv and sysenv should both be present"
      exit(0)
    env_var = sixdeskdir.parse_env(studyDir)
    for key in env_var.keys():
      if key not in tables.acc_var:
        del env_var[key]
    env_var['env_timestamp']=str(time.time())
    db = env_var['LHCDescrip'] + ".db"
    conn = sqlite3.connect(db, isolation_level="IMMEDIATE")
    cur = conn.cursor()
    cur.execute("PRAGMA synchronous = OFF")
    cur.execute("PRAGMA journal_mode = MEMORY")
    cur.execute("PRAGMA auto_vacuum = FULL")
    cur.execute("PRAGMA temp_store = MEMORY")
    cur.execute("PRAGMA count_changes = OFF")
    cur.execute("PRAGMA mmap_size=2335345345")
    conn.text_factory=str
    cols=SQLTable.cols_from_fields(tables.Env.fields)
    tab = SQLTable(conn,'env',cols,tables.Env.key)
    temp = tab.select('count(*)')[0][0]
    if temp > 0:
      print "study found updating..."
      lst = tab.select("keyname,value")
      cls = SixDeskDB(env_var['LHCDescrip'],basedir,verbose,dryrun)
      cls.set_variable(lst)  
    else:
      print "study not found inserting..."
    SixDeskDB.st_env(conn,env_var,studyDir)
    if cls is None:
      cls = SixDeskDB(env_var['LHCDescrip'],basedir,verbose,dryrun)
    cls.st_mad6t_run()
    cls.st_mad6t_run2(env_var)
    cls.st_mad6t_results(env_var)
    cls.st_six_beta(env_var)
    cls.st_six_input(env_var)
    cls.st_six_results(env_var)
    return cls

  def __init__(self,studyName,basedir='.',verbose=False,dryrun=False):
    '''initialise variables and location for study creation 
        or databse creation, usage listed in main.py'''
    self.verbose = verbose
    self.dryrun = dryrun
    self.studyName = studyName
    self.basedir = basedir
    db = studyName
    if not studyName.endswith('.db'):
      db = studyName+".db"
    if '/' in studyName:
      self.studyName = studyName.split('/')[-1]
    self.studyName = self.studyName.replace(".db","")
    if not os.path.isfile(db):
      print "file %s does'nt exist "%(db)
      print "see if you have typed the name correctly"
      exit(0)
    try:
      conn = sqlite3.connect(db,isolation_level="IMMEDIATE")
    except sqlite3.Error:
      print 'error'
      return
    print "Opened database successfully"
    self.conn = conn
    self.load_env_var()
    env_var = self.env_var
    self.orig_env_var = copy.copy(env_var)
    if self.basedir == '.':
      self.basedir = os.path.realpath(__file__).replace("SixdeskDB.py","")
      self.basedir = self.basedir.replace("SixdeskDB.pyc","")
    if not self.basedir.endswith('/'):
      self.basedir += '/'
    if env_var['basedir'] != self.basedir:
      for i in env_var.keys():
        if env_var['basedir'] in self.env_var[i]:
          self.env_var[i] = self.env_var[i].replace(
            env_var['basedir']+'/',self.basedir)

  def set_variable(self,lst):
    '''set additional variables besides predefined environment variables
        in sixdeskenv and sysenv'''
    conn = self.conn
    env_var = self.orig_env_var
    cols=SQLTable.cols_from_fields(tables.Env.fields)
    tab = SQLTable(conn,'env',cols,tables.Env.key)
    flag = 0
    upflag = 0
    for i in lst:
      if not ('env_timestamp' in str(i[0]) or str(i[0] in tables.def_var)):
        if str(i[0]) in env_var.keys():
          if str(i[1]) != env_var[str(i[0])]:
            if is_number(str(i[1])) and is_number(env_var[str(i[0])]):
              if float(str(i[1])) != float(env_var[str(i[0])]):
                upflag = 1
            else:
              upflag = 1
            if upflag == 1:
              print 'variable',str(i[0]),'already present updating value from',
              print env_var[str(i[0])],'to',str(i[1])
              flag = 1
        else:
          print 'variable',str(i[0]),'not present adding'
          flag = 1
        env_var[str(i[0])] = str(i[1])
        upflag = 0
    if flag == 1:
      env_var['env_timestamp']=str(time.time())
    env_var = [[i,env_var[i]] for i in env_var.keys()]
    tab.insertl(env_var)

  def info(self): 
    ''' provide info of study'''    
    var = ['LHCDescrip', 'platform', 'madlsfq', 'lsfq', 'runtype', 'e0',
    'gamma', 'beam', 'dpini', 'istamad', 'iendmad', 'ns1l', 'ns2l', 'nsincl', 
    'sixdeskpairs', 'turnsl', 'turnsle', 'writebinl', 'kstep', 'kendl', 'kmaxl',
    'trackdir', 'sixtrack_input']
    env_var = self.orig_env_var
    for keys in var:
      print '%s=%s'%(keys,env_var[keys])

  def st_mad6t_run(self):
    ''' store mad run files'''
    conn = self.conn
    env_var = self.orig_env_var
    cur = conn.cursor()
    cols = SQLTable.cols_from_fields(tables.Mad_Run.fields)
    tab = SQLTable(conn,'mad6t_run',cols,tables.Mad_Run.key)
    cols = SQLTable.cols_from_fields(tables.Files.fields)
    tab1 = SQLTable(conn,'files',cols,tables.Files.key)
    rows = {}
    extra_files = []
    a = []
    workdir = env_var['sixtrack_input']
    a = tab.select('distinct run_id')
    if a:
      a = [str(i[0]) for i in a]
    col = col_count(cur, 'mad6t_run')
    for dirName, subdirList, fileList in os.walk(workdir):
      if 'mad.dorun' in dirName and not (dirName.split('/')[-1] in a):
        print 'found new mad run',dirName.split('/')[-1]
        for files in fileList:
          if not (files.endswith('.mask') or 'out' in files
              or files.endswith('log') or files.endswith('lsf')):
            seed = files.split('.')[-1]
            run_id = dirName.split('/')[-1]
            mad_in = sqlite3.Binary(
              compressBuf(os.path.join(dirName, files))
              )
            out_file = files.replace('.', '.out.')
            mad_out = sqlite3.Binary(
              compressBuf(os.path.join(dirName, out_file))
              )
            lsf_file = 'mad6t_' + seed + '.lsf'
            mad_lsf = sqlite3.Binary(
              compressBuf(os.path.join(dirName, lsf_file))
              )
            log_file = files.replace('.','_mad6t_')+'.log'
            mad_log = sqlite3.Binary(
              compressBuf(os.path.join(dirName, log_file))
              )
            time = os.path.getmtime(
              os.path.join(dirName, log_file)
              )
            rows[seed] = []
            rows[seed].append(
              [run_id, seed, mad_in, mad_out, mad_lsf, 
              mad_log, time]
              )
          if files.endswith('.mask'):  
            path = os.path.join(dirName, files)
            content = sqlite3.Binary(compressBuf(path))
            path = path.replace(
              env_var['basedir']+'/','')
            extra_files.append([path, content])
      if rows:
        lst = dict_to_list(rows)
        tab.insertl(lst)
        rows = {}
    if extra_files:
      tab1.insertl(extra_files)

  def st_mad6t_run2(self,env_var):
    ''' store fort.3 and tmp files'''
    conn = self.conn
    cur = conn.cursor()
    env_var = self.orig_env_var
    cols = SQLTable.cols_from_fields(tables.Files.fields)
    tab1 = SQLTable(conn,'files',cols,tables.Files.key)
    workdir = env_var['sixtrack_input']
    extra_files = []
    col = col_count(cur, 'mad6t_run2')
    for dirName, subdirList, fileList in os.walk(workdir):
      for files in fileList:
        if 'fort.3' in files or files.endswith('.tmp'):
          path = os.path.join(dirName, files)
          content = sqlite3.Binary(compressBuf(path))
          path = path.replace(
            env_var['basedir']+'/','')
          extra_files.append([path, content])
    if extra_files:
      tab1.insertl(extra_files)

  def st_mad6t_results(self,env_var):
    ''' store fort.2, fort.8, fort.16 files'''
    conn = self.conn
    cur = conn.cursor()
    env_var = self.orig_env_var
    cols = SQLTable.cols_from_fields(tables.Mad_Res.fields)
    tab = SQLTable(conn,'mad6t_results',cols,tables.Mad_Res.key)
    workdir = env_var['sixtrack_input']
    res = []
    rows = []
    ista = int(env_var['ista'])
    iend = int(env_var['iend'])
    flag = 0
    cmd = "find %s -name 'fort.%s*.gz'"
    rows = []
    a = os.popen(cmd%(env_var['sixtrack_input'],'2')).read().split('\n')[:-1]
    b = os.popen(cmd%(env_var['sixtrack_input'],'8')).read().split('\n')[:-1]
    c = os.popen(cmd%(env_var['sixtrack_input'],'16')).read().split('\n')[:-1]
    print 'fort.2 files =',len(a)
    print 'fort.8 files =',len(b)
    print 'fort.16 files =',len(c)
    for i in a:
      seed = i.split('/')[-1].split('_')[1].replace(".gz","")
      row = [seed,sqlite3.Binary(open(i, 'r').read())]
      f8 = i.replace("fort.2","fort.8")
      mtime = os.path.getmtime(i)
      if f8 in b:
        row.extend([sqlite3.Binary(open(f8, 'r').read())])
        del b[b.index(f8)]
      else:
        row.extend([""])
        print 'missing file',f8,'inserting null instead'
      f16 = i.replace("fort.2","fort.16")
      if f16 in c:
        row.extend([sqlite3.Binary(open(f16, 'r').read())])
        del c[c.index(f16)]
      else:
        row.extend([""])
        print 'missing file',f16,'inserting null instead'
      row.extend([mtime])
      rows.append(row)
    for i in b:
      seed = i.split('/')[-1].split('_')[1].replace(".gz","")
      print 'missing file',
      print '%s inserting null instead'%(i.replace('fort.8','fort.2'))
      row = [seed,"",sqlite3.Binary(open(i, 'r').read())]
      mtime = os.path.getmtime(i)
      f16 = i.replace('fort.8','fort.16')
      if f16 in c:
        row.extend([sqlite3.Binary(open(f16, 'r').read())])
        del c[c.index(f16)]
      else:
        row.extend([""])
        print 'missing file',f16,'inserting null instead'
      row.extend([mtime])
      rows.append(row)
    for i in c:
      seed = i.split('/')[-1].split('_')[1].replace(".gz","")
      print 'missing file',
      print '%s inserting null instead'%(i.replace('fort.16','fort.2'))
      print 'missing file',
      print '%s inserting null instead'%(i.replace('fort.16','fort.8'))
      row = [seed,"","",sqlite3.Binary(open(i, 'r').read())]
      mtime = os.path.getmtime(i)
      row.extend([mtime])
      rows.append(row)
    if rows:
      tab.insertl(rows)
      rows = {}

  def st_six_beta(self,env_var):
    ''' store general_input, sixdesktunes, betavalues '''
    conn = self.conn
    cur = conn.cursor()
    env_var = self.orig_env_var
    cols = SQLTable.cols_from_fields(tables.Six_Be.fields)
    tab = SQLTable(conn,'six_beta',cols,tables.Six_Be.key)
    cols = SQLTable.cols_from_fields(tables.Files.fields)
    tab1 = SQLTable(conn,'files',cols,tables.Files.key)
    workdir = os.path.join(env_var['sixdesktrack'],env_var['LHCDescrip'])
    rows = {}
    extra_files = []
    col = col_count(cur, 'six_beta')
    beta = six = gen = []
    cmd = "find %s -name 'general_input'"%(workdir)
    a = os.popen(cmd).read().split('\n')[:-1]
    if not a:
      print 'general_input not found please check and run again'
      exit(0)
    else:
      a = a[0]
      with open(a,'r') as FileObj:
        for lines in FileObj:
          gen = lines.split()
      path = a
      content = sqlite3.Binary(compressBuf(path))
      path = path.replace(
        env_var['basedir']+'/','')
      extra_files.append([path, content])
    cmd = "find %s -name 'betavalues' -o -name 'sixdesktunes'"%(workdir)
    a = os.popen(cmd).read().split('\n')[:-1] 
    if not a:
      print 'betavalues and sixdesktunes files missing'
      exit(0)
    for dirName in a:
      files = dirName.split('/')[-1]
      dirName = dirName.replace('/'+files,'')
      dirn = dirName.replace(workdir+'/','').split('/')
      seed = int(dirn[0])
      tunex, tuney = dirn[2].split('_')
      if not (seed in rows.keys()):
        rows[seed] = []
      temp = [seed, tunex, tuney]
      if 'betavalues' in files:
        f = open(os.path.join(dirName, files), 'r')
        beta = [float(i) for i in f.read().split()]
      if 'sixdesktunes' in files:
        f = open(os.path.join(dirName, files), 'r')
        six = [float(i) for i in f.read().split()]
      f.close()
      if beta and temp and six:
        rows[seed].append(temp + beta + gen + six)
        beta = temp = six = []
    if rows:
      lst = dict_to_list(rows)
      tab.insertl(lst)
    if extra_files:
      tab1.insertl(extra_files)

  def st_six_input(self,env_var):
    ''' store input values (seed,tunes,amps,etc) along with fort.3 file'''
    conn = self.conn
    cur = conn.cursor()
    env_var = self.orig_env_var
    cols = SQLTable.cols_from_fields(tables.Six_In.fields)
    tab = SQLTable(conn,'six_input',cols,tables.Six_In.key)
    cols = SQLTable.cols_from_fields(tables.Files.fields)
    tab1 = SQLTable(conn,'files',cols,tables.Files.key)
    workdir = os.path.join(env_var['sixdesktrack'],env_var['LHCDescrip'])
    extra_files = []
    rows = []
    six_id = 1
    cmd = """find %s -name 'fort.3.gz'"""%(workdir)
    a = os.popen(cmd).read().split('\n')[:-1]
    print 'fort.3 files =',len(a)
    for dirName in a:
      files = dirName.split('/')[-1]
      dirName = dirName.replace('/'+files,'')
      if not ('-' in dirName):
        dirn = dirName.replace(workdir + '/', '')
        dirn = re.split('/|_', dirn)
        dirn = [six_id] + dirn
        dirn.extend([sqlite3.Binary(open(
          os.path.join(dirName, files), 'r'
        ).read()
        )])
        rows.append(dirn)
        dirn = []
        six_id += 1
    if rows:
      tab.insertl(rows)
      rows = []
    if rows:
      tab.insertl(rows)

  def st_six_results(self,env_var):
    '''store fort.10 values'''
    conn = self.conn
    cur = conn.cursor()
    env_var = self.orig_env_var
    cols = SQLTable.cols_from_fields(tables.Six_In.fields)
    aff_count = 0
    tab = SQLTable(conn,'six_input',cols,tables.Six_In.key)
    workdir = os.path.join(env_var['sixdesktrack'],env_var['LHCDescrip'])
    rows = []
    col = col_count(cur,'six_results')
    inp = tab.select("""distinct id,seed,simul,tunex,tuney,amp1,amp2,turns,
        angle""")
    inp = [[str(i) for i in j] for j in inp]
    cols = SQLTable.cols_from_fields(tables.Six_Res.fields)
    tab = SQLTable(conn,'six_results',cols,tables.Six_Res.key)
    maxtime = tab.select("max(mtime)")[0][0]
    if not maxtime:
      maxtime = 0
    cmd = "find %s -name 'fort.10.gz'"%(workdir)
    a = [i for i in os.popen(cmd).read().split('\n')[:-1] if not '-' in i]
    print 'fort.10 files =',len(a)
    for dirName in a:
      files = dirName.split('/')[-1]
      dirName = dirName.replace('/fort.10.gz','')
      if 'fort.10' in files and (not '-' in dirName) \
        and (os.path.getmtime(dirName) > maxtime):
        mtime = os.path.getmtime(dirName)
        dirn = dirName.replace(workdir+'/','')
        dirn = re.split('/|_',dirn)
        for i in [2,3,4,5,7]:
          if not ('.' in str(dirn[i])): 
            dirn[i] += '.0'
        for i in xrange(len(inp)+1):
          if i == len(inp):
            print 'fort.3 file missing for',
            print dirName.replace(env_var['sixdesktrack']+'/','')
            print 'create file and run again'
            print dirn
            exit(0)
          if dirn == inp[i][1:]:
            six_id = inp[i][0]
            break
        FileObj = gzip.open(
          os.path.join(dirName,files),"r").read().split("\n")[:-1]
        count = 1
        for lines in FileObj:
          rows.append([six_id,count]+lines.split()+[mtime])
          count += 1
          aff_count += 1
        if len(rows) > 180000:
          temp = tab.insertl(rows)
          rows = []
    if rows:
      temp = tab.insertl(rows)
    print "no of fort.10 updated =",aff_count/30

  def execute(self,sql):
    cur= self.conn.cursor()
    cur.execute(sql)
    self.conn.commit()
    return list(cur)

  def load_env_var(self):
    ''' long environment variables from DB'''
    conn = self.conn
    cur = conn.cursor()
    sql = """SELECT count(*) from env where keyname='LHCDescrip' 
    and value='%s'"""%(self.studyName)
    cur.execute(sql)
    temp = int(list(cur)[0][0])
    if temp == 0:
      print 'studyname not found'
      return
    self.env_var = load_dict(cur,"env")

  def load_extra(self):
    ''' load extra files from DB '''
    verbose = self.verbose
    dryrun = self.dryrun
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    basedir = self.basedir
    cur.execute("begin IMMEDIATE transaction")
    sql = """SELECT path,content from files"""
    cur.execute(sql)
    files = cur.fetchall()
    #print len(files)
    for file in files:
      path = os.path.join(basedir,str(file[0]))
      path1 = path.replace(path.split('/')[-1],"")
      if not os.path.exists(path1):
        if not dryrun:
          os.makedirs(path1)  
        if verbose:
          print 'creating directory',path1
      if verbose:
        print 'creating file',path.split('/')[-1]
      if not dryrun:
        f = open(path,'w')
        if '.gz' in path:
          f.write(str(file[1]))
        else:
          f.write(decompressBuf(str(file[1])))
        f.close()

  def load_mad6t_run(self):
    ''' load mad runs from DB '''
    verbose = self.verbose
    dryrun = self.dryrun
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    sql = """SELECT * from mad6t_run"""
    cur.execute(sql)
    files = cur.fetchall()
    for file in files:
      path = os.path.join(env_var['sixtrack_input'],str(file[0]))
      if not os.path.exists(path):
        if not dryrun:
          os.makedirs(path)
        if verbose:
          print 'creating directory',path
          print 'creating mad_in, mad_out, lsf and log files'
      mad_in,mad_out,mad_lsf,mad_log = [str(file[i]) for i in range(2,6)]
      if not dryrun:
        f = open(
          path+'/'+env_var['LHCDescrip']+'.'+str(file[1]),'w')
        f.write(decompressBuf(mad_in))
        f = open(
          path+'/'+env_var['LHCDescrip']+'.out.'+str(file[1]),'w')
        f.write(decompressBuf(mad_out))
        f = open(
          path+'/mad6t_'+str(file[1])+'.lsf','w')
        f.write(decompressBuf(mad_lsf))
        f = open(
          path+'/'+env_var['LHCDescrip']+'_mad_'+str(str(file[1])+'.log'),'w')
        f.write(decompressBuf(mad_in))
        # f = open(path+'/'+env_var)
        f.close()

  # def load_mad6t_run2(self):
  #     conn = self.conn
  #     cur = conn.cursor()
  #     env_var = self.env_var
  #     id = self.id
  #     sql = """SELECT * from mad6t_run2 where env_id = ?"""
  #     cur.execute(sql,[id])
  #     fort3 = cur.fetchone()
  #     aux,mad,m1,m2 = [str(fort3[i]) for i in range(1,5)]
  #     path = env_var['sixtrack_input']
  #     f = open(path+'/fort.3.aux','w')
  #     f.write(aux)
  #     f = open(path+'/fort.3.mad','w')
  #     f.write(mad)
  #     f = open(path+'/fort.3.mother1','w')
  #     f.write(m1)
  #     f = open(path+'/fort.3.mother2','w')
  #     f.write(m2)
  #     f.close()

  def load_mad6t_results(self):
    ''' load fort.2,fort.8,fort.16 from DB '''
    verbose = self.verbose
    dryrun = self.dryrun
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    cur.execute("begin IMMEDIATE transaction")
    sql = """SELECT * from mad6t_results"""
    cur.execute(sql)
    forts = cur.fetchall()
    path = env_var['sixtrack_input']
    for fort in forts:
      seed,f2,f8,f16 = [str(fort[i]) for i in range(0,4)]
      if verbose:
        print 'creating fort.2_%s.gz at %s'%(seed,path)
      if not dryrun:
        if f2:
          f = open(path+'/fort.2_'+seed+'.gz','w')
          f.write(f2)
        else:
          print 'fort.2_%s.gz was not created at %s',(seed,path)
      if verbose:
        print 'creating fort.8_%s.gz at %s'%(seed,path)
      if not dryrun:
        if f8:
          f = open(path+'/fort.8_'+seed+'.gz','w')
          f.write(f8)
        else:
          print 'fort.8_%s.gz was not created at %s',(seed,path)
      if verbose:
        print 'creating fort.16_%s.gz at %s'%(seed,path)
      if not dryrun:
        if f16:
          f = open(path+'/fort.16_'+seed+'.gz','w')
          f.write(f16)
          f.close()
        else:
          print 'fort.16_%s.gz was not created at %s',(seed,path)

  def load_six_beta(self):
    '''load general_input,betavalues and sixdesktunes from DB'''
    verbose = self.verbose
    dryrun = self.dryrun
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    path = os.path.join(env_var['sixdesktrack'],env_var['LHCDescrip'])
    cur.execute("begin IMMEDIATE transaction")
    sql = """SELECT * from six_beta"""
    cur.execute(sql)
    beta = cur.fetchall()
    for row in beta:
      sql = """SELECT simul from six_input where seed=? and 
          tunex=? and tuney=?"""
      cur.execute(sql,row[0:3])
      simul = cur.fetchone()[0]
      path1 = os.path.join(
        path,str(row[0]),simul,str(row[1])+'_'+str(row[2])
        )
      if not os.path.exists(path1):
        if not dryrun:
          os.makedirs(path1)
        if verbose:
          print 'creating directory',path1
      stri = ' '.join([str(row[i]) for i in range(3,17)])
      if verbose:
        print 'creating betavalues at %s'%(path1)
      if not dryrun:
        f = open(path1+'/betavalues','w')
        f.write(stri)
      stri = str(row[9])+' '+str(row[10])
      if verbose:
        print 'creating mychrom at %s'%(path1)
      if not dryrun:
        f = open(path1+'/mychrom','w')
        f.write(stri)
      stri = str(row[19])+'\n'+str(row[20])+' '+str(row[21])+'\n'
      stri += str(row[22])+' '+str(row[23])
      if verbose:
        print 'creating sixdesktunes at %s'%(path1)
      if not dryrun:
        f = open(path1+'/sixdesktunes','w')
        f.write(stri)
        f.close()

  def load_six_input_results(self):
    '''load fort.3 and fort.10 files from DB '''
    verbose = self.verbose
    dryrun = self.dryrun
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    path = os.path.join(env_var['sixdesktrack'],env_var['LHCDescrip'])
    cur.execute("begin IMMEDIATE transaction")
    sql = """SELECT * from six_input"""
    cur.execute(sql)
    six = cur.fetchall()
    for row in six:
      path1 = os.path.join(
        path,str(row[1]),str(row[2]),str(row[3])+'_'+str(row[4]),
        str(int(float(row[5])))+'_'+str(int(float(row[6]))),
        str(row[7]),str(int(float(row[8])))
        )
      if not os.path.exists(path1):
        if not dryrun:
          os.makedirs(path1)
        if verbose:
          print 'creating directory',path1
      if verbose:
        print 'creating fort.3.gz at %s'%(path1)
      if not dryrun:
        f = open(path1+'/fort.3.gz','w')
        f.write(str(row[9]))
      sql = """SELECT * from six_results where six_input_id=?"""
      cur.execute(sql,[row[0]])
      fort = cur.fetchall()
      stri = ""
      for col in fort:
        str1 = ""
        for i in xrange(60):
          str1 += str(col[i+2]) + ' '
        stri += str1 + '\n'
      if verbose:
        print 'creating fort.10.gz at %s'%(path1)
      if not dryrun:
        f = gzip.open(path1+'/fort.10.gz','w')
        f.write(stri)
        f.close()

  def get_missing_fort10(self):
    '''get input values for which fort.10 is not present '''
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    # newid = self.newid
    sql = """select seed,tunex,tuney,amp1,amp2,turns,angle from six_input
    where not exists(select 1 from six_results where id=six_input_id)"""
    a = self.execute(sql)
    if a:
      for rows in a:
        print 'fort.10 missing at','/'.join([str(i) for i in rows])
        print 
      return 1
    else:
      return 0

  def get_incomplete_fort10(self):
    '''get input values for which fort.10 is incomplete '''
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    # newid = self.newid
    sql = """select seed,tunex,tuney,amp1,amp2,turns,angle from six_input
    where not exists(select 1 from six_results where id=six_input_id and 
    row_num=30)"""
    a = self.execute(sql)
    if a:
      for rows in a:
        print 'fort.10 incomplete at','/'.join([str(i) for i in rows])
        print 
      return 1
    else:
      return 0

  def join10(self):
    '''re-implementation of run_join10 '''
    verbose = self.verbose
    dryrun = self.dryrun
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    ista = int(env_var['ista'])
    iend = int(env_var['iend'])
    tuney = float(env_var['tuney'])
    tunex = float(env_var['tunex'])
    tune = str(tunex)+'_'+str(tuney)
    print env_var['short'],env_var['long']
    if env_var['short'] == 1:
      amp1 = int(env_var['ns1s'])
      amp2 = int(env_var['ns2s'])
      ampd = int(env_var['nss'])
    else:
      amp1 = int(env_var['ns1l'])
      amp2 = int(env_var['ns2l'])
      ampd = int(env_var['nsincl'])
    sql = """SELECT distinct turns,angle from six_input"""
    cur.execute(sql)
    val = cur.fetchall()
    for seed in range(ista,iend+1):
      workdir = os.path.join(
        env_var['sixdesktrack'],env_var['LHCDescrip'],str(seed),'simul',tune
        )
      join = os.path.join(workdir,str(amp1)+'-'+str(amp2))
      #print join
      for amp in range(amp1,amp2,ampd):
        sql = """SELECT * from six_input,six_results 
        where six_input_id=id and seed=? and amp1=? and amp2=?"""
        cur.execute(sql,[seed,amp,amp+2])
        data = cur.fetchall()
        while data:
          path = os.path.join(join,str(data[0][7]),str(data[0][8]))
          if not os.path.exists(path):
            if not dryrun:
              os.makedirs(path)
            if verbose:
              print 'creating directory',path1
          if amp == amp1:
            if not dryrun:
              f = gzip.open(os.path.join(path,'fort.10.gz'),'w')
            if verbose:
              print 'creating joined fort.10 file at',path
            #print os.path.join(path,'fort.10.gz')
          else:
            if not dryrun:
              f = gzip.open(os.path.join(path,'fort.10.gz'),'a')
          if not dryrun:
            for j in xrange(30):
              str1 = '\t'.join(
                [str(data[0][i]) for i in range(12,72)]
                )
              str1 += '\n'
              f.write(str1)
              del data[0]
            f.close()

  def st_boinc(self,conn):
    '''store fort.10 files from boinc directory to local DB '''
    env_var = self.env_var
    study = env_var['LHCDescrip']
    cols=SQLTable.cols_from_fields(tables.Six_Res.fields)
    cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
    cols = ['study VARCHAR(128)'] + cols
    tab = SQLTable(conn,'six_results',cols)
    maxtime = tab.select("max(mtime)")[0][0]
    if not maxtime:
      maxtime = 0
    cur = conn.cursor()
    cur.execute("set global max_allowed_packet=209715200;")
    cur.execute("set global wait_timeout=120;")
    cur.execute("set global net_write_timeout=120;")
    cur.execute("set global net_read_timeout=120;")
    sql = "insert into six_results values (%s)"
    sql = sql%(','.join("%s " for _ in xrange(len(cols))))
    rows = []
    boincdir = os.path.join(env_var['sixdeskboincdir'],'results')
    cmd = "find %s -name '*boinc*'"%(boincdir)
    a = os.popen(cmd).read().split("\n")[:-1]
    for dirName in a and (os.path.getmtime(dirName) > maxtime):
      mtime = os.path.getmtime(dirName)
      dirn = dirName.replace(boincdir,'')
      dirn = dirn.replace(env_var['sixdeskboincdirname']+"__","")
      inp = re.split('_*',dirn)[:-3]
      if inp[1] == 's':
        inp[1] = 'simul'
      inp[-2] = 'e' + inp[-2]
      sql = """SELECT id from six_input where seed=? and simul=? and tunex=? 
            and tuney=? and amp1=? and amp2=? and turns=? and angle=?"""
      six_id = []
      cur.execute(sql,inp)
      six_id = list(cur)
      if six_id:
        six_id = six_id[0][0]
      else:
        print 'fort.3 missing for','/'.join(inp)
        exit(0)
      count = 1
      FileObj = open(dirName).read().split("\n")[:-1]
      for lines in FileObj:
        rows.append([study,six_id,count]+lines.split()+[mtime])
        count += 1
      if len(rows) > 150000:
        cur.executemany(sql,rows)
        conn.commit()
        rows = []
    if rows:
      cur.executemany(sql,rows)
      conn.commit()

  def inspect_results(self):
    ''' inspect input (seed, tunes, amps, turn and angle) values '''
    names='seed,tunex,tuney,amp1,amp2,turns,angle'
    for name in names.split(','):
      sql="SELECT DISTINCT %s FROM six_input"%name
      print sql
      data=[d[0] for d in self.execute(sql)]
      print name, guess_range(data)

  def iter_job_params(self):
    ''' get jobparams from DB '''
    names="""b.value,a.seed,a.tunex,a.tuney,a.amp1,a.amp2,a.turns,a.angle,
        c.row_num"""
    sql="""SELECT DISTINCT %s FROM six_input as a,env as b,six_results as c
        where a.id=c.six_input_id and b.keyname='LHCDescrip'"""%names
    return self.conn.cursor().execute(sql)
    
  def iter_job_params_comp(self):
    names='seed,tunex,tuney,amp1,amp2,turns,angle'
    sql='SELECT DISTINCT %s FROM six_input'%names
    return self.conn.cursor().execute(sql)
  
  def get_num_results(self):
    ''' get results count from DB '''
    return self.execute('SELECT count(*) from six_results')[0][0]/30

  def get_seeds(self):
    ''' get seeds from DB'''
    env_var = self.env_var
    ista = int(env_var['ista'])
    iend = int(env_var['iend'])
    return range(ista,iend+1)

  def get_angles(self):
    ''' get angles from DB '''
    env_var = self.env_var
    kmaxl = int(env_var['kmaxl'])
    kinil = int(env_var['kinil'])
    kendl = int(env_var['kendl'])
    kstep = int(env_var['kstep'])
    s=90./(kmaxl+1)
    return np.arange(kinil,kendl+1,kstep)*s

  def get_amplitudes(self):
    ''' get_amplitudes from DB '''
    env_var = self.env_var
    nsincl = float(env_var['nsincl'])
    ns1l = float(env_var['ns1l'])
    ns2l = float(env_var['ns2l'])
    return [(a,a+nsincl) for a in np.arange(ns1l,ns2l,nsincl)]

  def iter_tunes(self):
    ''' get tunes from DB '''
    env_var = self.env_var
    qx = float(env_var['tunex'])
    qy = float(env_var['tuney'])
    while qx <= float(env_var['tunex1']) and qy <= float(env_var['tuney1']):
      yield qx,qy
      qx += float(env_var['deltax'])
      qy += float(env_var['deltay'])

  def get_tunes(self):
    return list(self.iter_tunes())

  def gen_job_params(self):
    ''' generate jobparams based on values '''
    turnsl = '%E'%(float(self.env_var['turnsl']))
    turnsl = 'e'+str(int(turnsl.split('+')[1]))
    for seed in self.get_seeds():
      for tunex,tuney in self.get_tunes():
        for amp1,amp2 in self.get_amplitudes():
          for angle in self.get_angles():
            yield (seed,tunex,tuney,amp1,amp2,turnsl,angle)

  def get_missing_jobs(self):
    '''get missing jobs '''
    cur = self.conn.cursor()
    turnsl = '%E'%(float(self.env_var['turnsl']))
    turnsl = 'e'+str(int(turnsl.split('+')[1]))
    a = self.execute("""SELECT seed,tunex,tuney,amp1,amp2,turns,angle from 
      six_input where turns='%s'"""%(turnsl))
    print a[0]
    for rows in self.gen_job_params():
      if rows not in a:
        print 'missing job',rows

  def inspect_jobparams(self):
    data=list(self.iter_job_params())
    names='study,seed,tunex,tuney,amp1,amp2,turns,angle,row'.split(',')
    data=dict(zip(names,zip(*data)))
  #    for name in names:
  #      print name, guess_range(data[name])
  #    print 'results found',len(data['seed'])
    turns=list(set(data['turns']))
    p={}
    p['ista'],p['iend'],p['istep']=guess_range(data['seed'])
    p['tunex'],p['tunex1'],p['deltax']=guess_range(data['tunex'])
    p['tuney'],p['tuney1'],p['deltay']=guess_range(data['tuney'])
    if p['deltax'] is None:
      p['deltax']=0.0001
    if p['deltay'] is None:
      p['deltay']=0.0001
    a1,a2,ast=guess_range(data['angle'])
    p['kmaxl']=90/ast-1
    p['kinil']=a1/ast
    p['kendl']=a2/ast
    p['kstep']=1
    p['ns1l'],p['ns2l'],p['nsincl']=guess_range(data['amp1']+data['amp2'])
    p['turnsl']=max(data['turns'])
    p['sixdeskpairs']=max(data['row'])+1
    p['LHCDescrip']=str(data['study'][0])
    return p
    
  def get_survival_turns(self,seed):
    '''get survival turns from DB '''
    cmd="""SELECT angle,amp1+(amp2-amp1)*row_num/30,
        CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
        FROM six_results,six_input WHERE seed=%s and id=six_input_id
        ORDER BY angle,sigx1"""
    # cmd="""SELECT angle,sqrt(sigx1*2+sigy1*2),
    #         CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
    #         FROM six_results,six_input WHERE seed=%s and id=six_input_id 
    #         ORDER BY angle,sigx1"""

    cur=self.conn.cursor().execute(cmd%seed)
    ftype=[('angle',float),('amp',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    angles=len(set(data['angle']))
    return data.reshape(angles,-1)
    
  def plot_survival_2d(self,seed,smooth=None):
    '''plot survival turns graph '''
    data=self.get_survival_turns(seed)
    a,s,t=data['angle'],data['amp'],data['surv']
    self._plot_survival_2d(a,s,t,smooth=smooth)
    pl.title('Seed %d survived turns'%seed)
    
  def plot_survival_2d_avg(self,smooth=None):
    ''' plot avg survival turns graph '''
    cmd=""" SELECT seed,angle,amp1+(amp2-amp1)*row_num/30,
        CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
        FROM six_results,six_input WHERE id=six_input_id
        ORDER BY angle,sigx1"""
    cur=self.conn.cursor().execute(cmd)
    ftype=[('seed',float),('angle',float),('amp',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    angles=len(set(data['angle']))
    seeds=len(set(data['seed']))
    data=data.reshape(seeds,angles,-1)
    a,s,t=data['angle'],data['amp'],data['surv']
    a=a.mean(axis=0); s=s.mean(axis=0); t=t.mean(axis=0);
    self._plot_survival_2d(a,s,t,smooth=smooth)
    pl.title('Survived turns')
    
  def _plot_survival_2d(self,a,s,t,smooth=None):
    rad=np.pi*a/180
    x=s*np.cos(rad)
    y=s*np.sin(rad)
    #t=np.log10(t)
    if smooth is not None:
      if type(smooth)==int:
        smooth=np.ones((1.,smooth))/smooth
      elif type(smooth)==tuple:
        smooth=np.ones(smooth)/float(smooth[0]*smooth[1])
        t=scipy.signal.fftconvolve(smooth,t,mode='full')
    pl.pcolormesh(x,y,t,antialiased=True)
    pl.xlabel(r'$\sigma_x$')
    pl.ylabel(r'$\sigma_y$')
    pl.colorbar()
    
  def plot_survival_avg(self,seed):
    data=self.get_survival_turns(seed)
    a,s,t=data['angle'],data['amp'],data['surv']
    rad=np.pi*a/180
    drad=rad[0,0]
    slab='Seed %d'%seed
    #savg2=s**4*np.sin(2*rad)*drad
    pl.plot(s.mean(axis=0),t.min(axis=0) ,label='min')
    pl.plot(s.mean(axis=0),t.mean(axis=0),label='avg')
    pl.plot(s.mean(axis=0),t.max(axis=0) ,label='max')
    pl.ylim(0,pl.ylim()[1]*1.1)
    pl.xlabel(r'$\sigma$')
    pl.ylabel(r'survived turns')
    pl.legend(loc='lower left')
    return data
    
  def plot_survival_avg2(self,seed):
    def mk_tuple(a,s,t,nlim):
      region=[]
      for ia,ts in enumerate(t):
        cond=np.where(ts<=nlim)[0]
        #print ts[cond]
        if len(cond)>0:
          it=cond[0]
        else:
          it=-1
        region.append((ia,it))
        #print ts[it],s[ia,it]
      region=zip(*region)
      #print t[region]
      tmin=t[region].min()
      tavg=t[region].mean()
      tmax=t[region].max()
      savg=s[region].mean()
      rad=np.pi*a[region]/180
      drad=rad[0]
      savg2=np.sum(s[region]**4*np.sin(2*rad)*drad)**.25
      return nlim,tmin,tavg,tmax,savg,savg2
    data=self.get_survival_turns(seed)
    a,s,t=data['angle'],data['amp'],data['surv']
    table=np.array([mk_tuple(a,s,t,nlim) for nlim \
      in np.arange(0,1.1e6,1e4)]).T
    nlim,tmin,tavg,tmax,savg,savg2=table
    pl.plot(savg,tmin,label='min')
    pl.plot(savg,tavg,label='avg')
    pl.plot(savg,tmax,label='max')
    return table


if __name__ == '__main__':
  SixDeskDB.from_dir('/home/monis/Desktop/GSOC/files/w7/sixjobs/')
  


