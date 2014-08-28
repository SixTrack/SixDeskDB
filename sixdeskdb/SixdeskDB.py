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

import sqlite3, time, os, re, gzip, sys, glob
from cStringIO import StringIO
import copy

try:
  import numpy as np
  import matplotlib.pyplot as pl
  import scipy.signal
except ImportError:
  print "No module found: numpy matplotlib and scipy modules should be present to run sixdb"
  raise ImportError

import tables
import sixdeskdir
from sqltable import SQLTable

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
  zbuf = StringIO()
  zfile = gzip.GzipFile(mode = 'wb',  fileobj = zbuf, compresslevel = 9)
  zfile.write(buf)
  zfile.close()
  return zbuf.getvalue()

def decompressBuf(buf):
  '''file decompression to retrieve from DB'''
  zbuf = StringIO(buf)
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

def split_fort10fn(fn):
  ll=fn.split('/')
  data=ll[-8:-1]
  study,seed,simul,tunes,rng,turns,angle=data
  if '-' in rng:
    join='-'
  else:
    join='_'
  seed=int(seed)
  tunex,tuney=map(float,tunes.split('_'))
  amp1,amp2=map(float,rng.split(join))
  angle=float(angle)
  turns=10**int(turns[1])
  return study,seed,tunex,tuney,amp1,amp2,join,turns,angle

def check_mad_out(fhs):
  closest2=[]
  closest1=[]
  closest0=[]
  kqs={}
  kqt={}
  for fh in fhs:
    for l in fh:
      l=l.strip()
      if l.startswith('closest'):
        if l.startswith('closest2 =  '):
          closest2.append(float(l.split('=')[1].split(';')[0]))
        elif l.startswith('closest1 =  '):
          closest1.append(float(l.split('=')[1].split(';')[0]))
        elif l.startswith('closest0 =  '):
          closest0.append(float(l.split('=')[1].split(';')[0]))
      elif 'kmqsmax*100' in l:
        name,val=extract_kmax(l)
        kqs.setdefault(name,[]).append(val)
      elif 'kmqtmax*100' in l:
        name,val=extract_kmax(l)
        kqt.setdefault(name,[]).append(val)
  print "Mad6tOut clo0: %s"%minmaxavg(closest0)
  print "Mad6tOut clo1: %s"%minmaxavg(closest1)
  print "Mad6tOut clo2: %s"%minmaxavg(closest2)
  kqsmax=[max(abs(m) for m in l) for l in zip(*kqs.values())]
  kqtmax=[max(abs(m) for m in l) for l in zip(*kqt.values())]
  print "Mad6tOut kqt : %s"%minmaxavg(kqtmax)
  print "Mad6tOut kqs : %s"%minmaxavg(kqsmax)

def extract_kmax(l):
  name,val=l.split('=')
  name=name.split('/')[0]
  val=float(val.split(';')[0])
  return name,val

def minmaxavg(l,fmt="%13e"):
  if len(l)>0:
      l=np.array(l)
      mi=l.min()
      ma=l.max()
      av=l.mean()
      tmp="min %s avg %s max %s"%(fmt,fmt,fmt)
      return tmp%(mi,av,ma)
  else:
      return "no data to find min and max"





class SixDeskDB(object):
  @staticmethod
  def st_env(conn,env_var,studyDir):
    '''store environment variables to DB'''
    extra_files = []
    cols=SQLTable.cols_from_fields(tables.Env.fields)
    tab = SQLTable(conn,'env',cols,tables.Env.key)
    cols=SQLTable.cols_from_fields(tables.Files.fields)
    tab1 = SQLTable(conn,'files',cols,tables.Files.key)
    env_var['env_timestamp']=str(time.time())
    mtime = os.path.getmtime(studyDir+"/sixdeskenv")
    env_var1 = [[i,env_var[i],mtime] for i in env_var.keys()]
    tab.insertl(env_var1)
    path = os.path.join(studyDir, 'sixdeskenv')
    content = sqlite3.Binary(compressBuf(path))
    path = 'sixdeskenv'
    #path = path.replace(env_var['basedir']+'/','')
    extra_files.append([path, content])
    path = os.path.join(studyDir, 'sysenv')
    content = sqlite3.Binary(compressBuf(path))
    path = 'sysenv'
    #path = path.replace(env_var['basedir']+'/','')
    extra_files.append([path, content])
    tab1.insertl(extra_files) 

  @classmethod
  def from_dir(cls,studyDir,basedir='.',verbose=False,dryrun=False):
    '''create local Database for storing study'''
    cls = None
    if not (os.path.exists(studyDir+'/sixdeskenv') and \
      os.path.exists(studyDir+'/sysenv')):
      print "Error in loaddir from %s:"%studyDir,
      print "`sixdeskenv` and `sysenv` files should both be present"
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
      print "Updating %s"%db
      lst = tab.select("keyname,value")
      cls = SixDeskDB(env_var['LHCDescrip'],basedir,verbose,dryrun)
      mtime = os.path.getmtime(studyDir+"/sixdeskenv")
      cls.set_variable(lst,mtime)
    else:
      print "Creating %s"%db
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
        or database creation, usage listed in main.py'''
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
      print "file %s doesn't exist "%(db)
      print "see if you have typed the name correctly"
      raise IOError
    try:
      conn = sqlite3.connect(db,isolation_level="IMMEDIATE")
    except sqlite3.Error:
      print 'error'
      return
    print "Opened %s successfully"%db
    self.conn = conn
    self.load_env_var()
    env_var = self.env_var
    self.orig_env_var = copy.copy(env_var)
    if not self.basedir.endswith('/'):
      self.basedir += '/'
      # print self.basedir
    #print self.basedir
    if env_var['basedir'] != self.basedir:
      for i in env_var.keys():
        if env_var['basedir'] in self.env_var[i]:
          self.env_var[i] = self.env_var[i].replace(
            env_var['basedir']+'/',self.basedir)
          #print self.env_var[i]

  def print_table_info(self):
      out=self.execute("SELECT name FROM sqlite_master WHERE type='table';")
      tables=[str(i[0]) for i in out]
      for tab in tables:
          rows=self.execute("SELECT count(*) FROM %s"%tab)[0][0]
          columms=[i[1] for i in self.execute("PRAGMA table_info(%s)"%tab)]
          print "%s(%d):\n  %s"%(tab,rows,', '.join(columms))

  def mad_out(db):
      mad_runs=db.execute('SELECT DISTINCT  run_id FROM mad6t_run')
      for run in mad_runs:
          print "Checking %s"%run
          sql="SELECT mad_out FROM mad6t_run WHERE run_id=='%s'"%run
          bufs=db.execute(sql)
          bufs=[StringIO(decompressBuf(buf[0])) for buf in bufs]
          check_mad_out(bufs)

  def set_variable(self,lst,mtime):
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
    env_var = [[i,env_var[i],mtime] for i in env_var.keys()]
    tab.insertl(env_var)

  def info(self):
    ''' provide info of study'''
    var = ['LHCDescrip', 'platform', 'madlsfq', 'lsfq', 'runtype', 'e0',
    'gamma', 'beam', 'dpini', 'istamad', 'iendmad', 'ns1l', 'ns2l', 'nsincl', 
    'sixdeskpairs', 'turnsl', 'turnsle', 'writebinl',
    'kstep', 'kendl', 'kmaxl',
    'trackdir', 'sixtrack_input']
    env_var = self.orig_env_var
    for keys in var:
      print '%s=%s'%(keys,env_var[keys])

  def st_mad6t_run(self):
    ''' store mad run files'''
    conn = self.conn
    env_var = self.orig_env_var
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
    print "Looking for fort.2, fort.8, fort.16 in\n %s"%workdir
    for dirName, _, fileList in os.walk(workdir):
      if 'mad.dorun' in dirName and not (dirName.split('/')[-1] in a):
        print 'found new mad run',dirName.split('/')[-1]
        for files in fileList:
          if not (files.endswith('.mask') or 'out' in files
              or files.endswith('log') or files.endswith('lsf')):
            fnroot,seed=os.path.splitext(files)
            seed=int(seed[1:])
            run_id = dirName.split('/')[-1]
            mad_in = sqlite3.Binary(
              compressBuf(os.path.join(dirName, files))
              )
            out_file=os.path.join(dirName,fnroot+'.out.%d'%seed)
            log_file=os.path.join(dirName,fnroot+'_mad6t_%d.log'%seed)
            lsf_file=os.path.join(dirName,'mad6t_%d.lsf'%seed)
            mad_out = sqlite3.Binary(compressBuf(out_file))
            mad_lsf = sqlite3.Binary(compressBuf(lsf_file))
            mad_log = sqlite3.Binary(compressBuf(log_file))
            time = os.path.getmtime( log_file)
            rows[seed] = []
            rows[seed].append(
              [run_id, seed, mad_in, mad_out, mad_lsf, 
              mad_log, time]
              )
          if files.endswith('.mask'):
            path = os.path.join(dirName, files)
            content = sqlite3.Binary(compressBuf(path))
            path = path.replace(
              env_var['scratchdir']+'/','')
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
    env_var = self.orig_env_var
    cols = SQLTable.cols_from_fields(tables.Files.fields)
    tab1 = SQLTable(conn,'files',cols,tables.Files.key)
    workdir = env_var['sixtrack_input']
    extra_files = []
    for dirName, _, fileList in os.walk(workdir):
      for files in fileList:
        if 'fort.3' in files or files.endswith('.tmp'):
          path = os.path.join(dirName, files)
          content = sqlite3.Binary(compressBuf(path))
          path = path.replace(
            env_var['scratchdir']+'/','')
          extra_files.append([path, content])
    if extra_files:
      tab1.insertl(extra_files)

  def st_mad6t_results(self,env_var):
    ''' store fort.2, fort.8, fort.16 files'''
    conn = self.conn
    env_var = self.orig_env_var
    cols = SQLTable.cols_from_fields(tables.Mad_Res.fields)
    tab = SQLTable(conn,'mad6t_results',cols,tables.Mad_Res.key)
    maxtime = tab.select("max(fort_mtime)")[0][0]
    if not maxtime:
      maxtime = 0
    rows = []
    cmd = "find %s -type f -name 'fort.%s*.gz'"
    rows = []
    a = os.popen(cmd%(env_var['sixtrack_input'],'2')).read().split('\n')[:-1]
    b = os.popen(cmd%(env_var['sixtrack_input'],'8')).read().split('\n')[:-1]
    c = os.popen(cmd%(env_var['sixtrack_input'],'16')).read().split('\n')[:-1]
    f_a=len(a);f_b=len(b);f_c=len(c);
    up_a = up_b = up_c = 0
    for i in a:
      if os.path.getmtime(i) > maxtime:
        seed = i.split('/')[-1].split('_')[1].replace(".gz","")
        row = [seed,sqlite3.Binary(open(i, 'r').read())]
        f8 = i.replace("fort.2","fort.8")
        mtime = os.path.getmtime(i)
        if f8 in b:
          row.extend([sqlite3.Binary(open(f8, 'r').read())])
          del b[b.index(f8)]
          up_b += 1
        else:
          row.extend([""])
          print 'missing file',f8,'inserting null instead'
        f16 = i.replace("fort.2","fort.16")
        if f16 in c:
          row.extend([sqlite3.Binary(open(f16, 'r').read())])
          del c[c.index(f16)]
          up_c += 1
        else:
          row.extend([""])
          print 'missing file',f16,'inserting null instead'
        row.extend([mtime])
        rows.append(row)
        up_a += 1
    for i in b:
      if os.path.getmtime(i) > maxtime:
        seed = i.split('/')[-1].split('_')[1].replace(".gz","")
        print 'missing file',
        print '%s inserting null instead'%(i.replace('fort.8','fort.2'))
        row = [seed,"",sqlite3.Binary(open(i, 'r').read())]
        mtime = os.path.getmtime(i)
        f16 = i.replace('fort.8','fort.16')
        if f16 in c:
          row.extend([sqlite3.Binary(open(f16, 'r').read())])
          del c[c.index(f16)]
          up_c += 1
        else:
          row.extend([""])
          print 'missing file',f16,'inserting null instead'
        row.extend([mtime])
        rows.append(row)
        up_b += 1
    for i in c:
      if os.path.getmtime(i) > maxtime:
        seed = i.split('/')[-1].split('_')[1].replace(".gz","")
        print 'missing file',
        print '%s inserting null instead'%(i.replace('fort.16','fort.2'))
        print 'missing file',
        print '%s inserting null instead'%(i.replace('fort.16','fort.8'))
        row = [seed,"","",sqlite3.Binary(open(i, 'r').read())]
        mtime = os.path.getmtime(i)
        row.extend([mtime])
        rows.append(row)
        up_c += 1
    if rows:
      tab.insertl(rows)
      rows = {}
    print 'no of fort.2 updated/found: %d/%d'%(up_a,f_a)
    print 'no of fort.8 updated/found: %d/%d'%(up_b,f_b)
    print 'no of fort.16 updated/found: %d/%d'%(up_c,f_c)

  def st_six_beta(self,env_var):
    ''' store sixdesktunes, betavalues '''
    conn = self.conn
    env_var = self.orig_env_var
    cols = SQLTable.cols_from_fields(tables.Six_Be.fields)
    tab = SQLTable(conn,'six_beta',cols,tables.Six_Be.key)
    cols = SQLTable.cols_from_fields(tables.Files.fields)
    tab1 = SQLTable(conn,'files',cols,tables.Files.key)
    workdir = os.path.join(env_var['sixdesktrack'],env_var['LHCDescrip'])
    rows = {}
    extra_files = []
    beta = six = gen = []
    print "Looking for sixdesktunes, betavalues in\n %s"%workdir
    gen_input=os.path.join(workdir,'general_input')
    if not os.path.exists(gen_input):
      print "Warning: %s not found"%gen_input
      gen=[0,0]
    else:
      content = sqlite3.Binary(compressBuf(gen_input))
      gen=[float(i) for i in open(gen_input).read().split()]
      extra_files.append(['general_input', content])
    a=glob.glob('%s/*/simul/*/betavalues'%workdir)
    a+=glob.glob('%s/*/simul/*/sixdesktunes'%workdir)
    a+=glob.glob('%s/*/simul/*/mychrom'%workdir)
    print "no of files found: %d"%len(a)
    if not a:
      print 'Warning betavalues and sixdesktunes files missing'
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
      if 'mychrom' in files:
        f = open(os.path.join(dirName, files), 'r')
        chrom = [float(i) for i in f.read().split()]
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
    #tab = SQLTable(conn,'mad6t_results',cols,tables.Mad_Res.key)
    maxtime = tab.select("max(mtime)")[0][0]
    count = 0
    if not maxtime:
      maxtime = 0
    cols = SQLTable.cols_from_fields(tables.Files.fields)
    tab1 = SQLTable(conn,'files',cols,tables.Files.key)
    workdir = os.path.join(env_var['sixdesktrack'],env_var['LHCDescrip'])
    extra_files = []
    rows = []
    six_id = 1
    print "Looking for fort.3.gz files in\n %s"%workdir
    cmd = """find %s -type f -name 'fort.3.gz'"""%(workdir)
    #a = os.popen(cmd).read().split('\n')[:-1]
    #print 'fort.3 files =',len(a)
    file_count=0
    for dirName in os.popen(cmd):
      dirName,files=os.path.split(dirName.strip())
      ranges= dirName.split('/')[-3]
      if '_' in ranges:
        file_count+=1
        if file_count%100==0:
            sys.stdout.write('.')
            sys.stdout.flush()
        mtime = os.path.getmtime(dirName)
        if mtime>maxtime:
            dirn = dirName.replace(workdir + '/', '')
            dirn = re.split('/|_', dirn)
            dirn = [six_id] + dirn
            dirn.extend([sqlite3.Binary(open(
              os.path.join(dirName, files), 'r'
            ).read()
            ),mtime])
            rows.append(dirn)
            dirn = []
            six_id += 1
            count += 1
    if rows:
      tab.insertl(rows)
      rows = []
    print '\n no of fort.3 updated/found: %d/%d'%(count,file_count)

  def st_six_results(self,env_var):
    '''store fort.10 values'''
    conn = self.conn
    env_var = self.orig_env_var
    cols = SQLTable.cols_from_fields(tables.Six_In.fields)
    aff_count = 0
    tab = SQLTable(conn,'six_input',cols,tables.Six_In.key)
    workdir = os.path.join(env_var['sixdesktrack'],env_var['LHCDescrip'])
    rows = []
    inp = tab.select("""distinct id,seed,simul,tunex,tuney,amp1,amp2,turns,
        angle""")
    inp = [[str(i) for i in j] for j in inp]
    cols = SQLTable.cols_from_fields(tables.Six_Res.fields)
    tab = SQLTable(conn,'six_results',cols,tables.Six_Res.key)
    maxtime = tab.select("max(mtime)")[0][0]
    if not maxtime:
      maxtime = 0
    print "Looking for fort.10.gz files in\n %s"%workdir
    cmd = "find %s -type f -name 'fort.10.gz'"%(workdir)
    #a = [i for i in os.popen(cmd).read().split('\n')[:-1] if not '-' in i]
    #fort10=[i for i in os.popen(cmd)]
    #print 'fort.10 files =',len(fort10)
    file_count=0
    for dirName in os.popen(cmd):
      dirName,files=os.path.split(dirName.strip())
      ranges=dirName.split('/')[-3]
      if '_' in ranges:
        file_count+=1
        if file_count%100==0:
            sys.stdout.write('.')
            sys.stdout.flush()
        if os.path.getmtime(dirName) > maxtime:
            mtime = os.path.getmtime(dirName)
            print dirn
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
              tab.insertl(rows)
              rows = []
    if rows:
      tab.insertl(rows)
    print "\n no of fort.10 updated/found: %d/%d"%(aff_count/30,file_count)

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
    env_var = self.env_var
    conn = self.conn
    cur = conn.cursor()
    # env_var = self.env_var
    basedir = self.basedir
    cur.execute("begin IMMEDIATE transaction")
    sql = """SELECT path,content from files"""
    cur.execute(sql)
    files = cur.fetchall()
    #print len(files)
    for file in files:
      if 'sixdeskenv' or 'sysenv' in str(file[0]):
        path = os.path.join(env_var['scratchdir'],str(file[0]))
      elif 'mad.dorun' or 'fort.3' in str(file[0]):
        path = os.path.join(env_var['scratchdir'],str(file[0]))
      else:
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
        if os.path.exists(path):
          print 'file already exists please remove it and try again'
          exit(0)
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
        temp = path+'/'+env_var['LHCDescrip']+'.'+str(file[1])
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        f = open(temp,'w')
        f.write(decompressBuf(mad_in))
        temp = path+'/'+env_var['LHCDescrip']+'.out.'+str(file[1])
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        f = open(temp,'w')
        f.write(decompressBuf(mad_out))
        temp = path+'/mad6t_'+str(file[1])+'.lsf'
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        f = open(temp,'w')
        f.write(decompressBuf(mad_lsf))
        temp = path+'/'+env_var['LHCDescrip']+'_mad_'+str(str(file[1])+'.log')
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        f = open(temp,'w')
        f.write(decompressBuf(mad_log))
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
        temp = path+'/fort.2_'+seed+'.gz'
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        if f2:
          f = open(temp,'w')
          f.write(f2)
        else:
          print 'fort.2_%s.gz was not created at %s',(seed,path)
      if verbose:
        print 'creating fort.8_%s.gz at %s'%(seed,path)
      if not dryrun:
        temp = path+'/fort.8_'+seed+'.gz'
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        if f8:
          f = open(temp,'w')
          f.write(f8)
        else:
          print 'fort.8_%s.gz was not created at %s',(seed,path)
      if verbose:
        print 'creating fort.16_%s.gz at %s'%(seed,path)
      if not dryrun:
        temp = path+'/fort.16_'+seed+'.gz'
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        if f16:
          f = open(temp,'w')
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
        temp = path1+'/betavalues'
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        f = open(temp,'w')
        f.write(stri)
      stri = str(row[9])+' '+str(row[10])
      if verbose:
        print 'creating mychrom at %s'%(path1)
      if not dryrun:
        temp = path1+'/mychrom'
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        f = open(temp,'w')
        f.write(stri)
      stri = str(row[19])+'\n'+str(row[20])+' '+str(row[21])+'\n'
      stri += str(row[22])+' '+str(row[23])
      if verbose:
        print 'creating sixdesktunes at %s'%(path1)
      if not dryrun:
        temp = path1+'/sixdesktunes'
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        f = open(temp,'w')
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
      temp = path1+'/fort.3.gz'
      if os.path.exists(temp):
        print 'file already exists please remove it and try again'
        exit(0)
      if not dryrun:
        f = open(temp,'w')
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
      temp = path1+'/fort.10.gz'
      if os.path.exists(temp):
        print 'file already exists please remove it and try again'
        exit(0)
      if not dryrun:
        f = gzip.open(temp,'w')
        f.write(stri)
        f.close()

  def get_missing_fort10(self):
    '''get input values for which fort.10 is not present '''
    # conn = self.conn
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
    # conn = self.conn
    # cur = conn.cursor()
    # env_var = self.env_var
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
              print 'creating directory',path
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

  def get_surv(self,seed):
    #change for new db version
    emit=float(self.env_var['emit'])
    gamma=float(self.env_var['gamma'])
    cmd="""SELECT angle,emitx+emity,
         CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
         FROM six_results,six_input WHERE seed=%s and id=six_input_id
         ORDER BY angle,emitx+emity"""
    cur=self.conn.cursor().execute(cmd%seed)
    ftype=[('angle',float),('sigma',float),('sturn',float)]
    data=np.fromiter(cur,dtype=ftype)
    data['sigma']=np.sqrt(data['sigma']/(emit/gamma))
    angles=len(set(data['angle']))
    return data.reshape(angles,-1)


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
    a=a.mean(axis=0); s=s.mean(axis=0); t=t.mean(axis=0)
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

  def read10b(self):
    database= '%s.db'%(self.studyName)
    fntxt='DA_%s.txt'%self.studyName
    fhtxt = open(fntxt, 'w')

    rectype=[('seed','int'),('betx'    ,'float'),('bety'    ,'float'),('sigx1'   ,'float'),('sigy1'   ,'float'),('emitx'   ,'float'),('emity'   ,'float'),
            ('sigxavgnld' ,'float') ,('sigyavgnld' ,'float'),('betx2'   ,'float'),('bety2'   ,'float'),('distp'   ,'float'),('dist'    ,'float'),
            ('sturns1' ,'int')   ,('sturns2' ,'int')  ,('turn_max','int')  ,('amp1'    ,'float'),('amp2'    ,'float'),('angle'   ,'float')]
    names='seed,betx,bety,sigx1,sigy1,emitx,emity,sigxavgnld,sigyavgnld,betx2,bety2,distp,dist,sturns1,sturns2,turn_max,amp1,amp2,angle'
    outtype=[('study','S100'),('seed','int'),('angle','float'),('achaos','float'),('achaos1','float'),('alost1','float'),('alost2','float'),('Amin','float'),('Amax','float')]

    LHCDesName=self.env_var['LHCDesName']
    turnse=self.env_var['turnse']
    sixdesktunes=self.env_var['tunex']+"_"+self.env_var['tuney']
    ns1l=self.env_var['ns1l']
    ns2l=self.env_var['ns2l']

    tmp=np.array(self.execute('SELECT DISTINCT %s FROM six_results,six_input where id=six_input_id'%names),dtype=rectype)
    Elhc,Einj = self.execute('SELECT emitn,gamma from six_beta LIMIT 1')[0]
    anumber=1
    angles=np.unique(tmp['angle'])
    seeds=np.unique(tmp['seed'])
    for angle in angles:
        fndot='DAres.%s.%s.%s.%d'%(LHCDesName,sixdesktunes,turnse,anumber)
        fhdot = open(fndot, 'w')
        for seed in seeds:
            ich1 = 0
            ich2 = 0
            ich3 = 0
            icount = 1.
            itest = 0
            iin  = -999
            iend = -999
            alost1 = 0.
            alost2 = 0.
            achaos = 0
            achaos1 = 0
            mask=[(tmp['betx']>0) & (tmp['emitx']>0) & (tmp['bety']>0) & (tmp['emity']>0) & (tmp['angle']==angle) & (tmp['seed']==seed)]
            inp=tmp[mask]
            if inp.size<2 :
                print 'not enought data for angle = %s' %angle
                break

            zero = 1e-10
            for itest in range(0,inp.size):
                if inp['betx'][itest]>zero and inp['emitx'][itest]>zero : inp['sigx1'][itest] =  np.sqrt(inp['betx'][itest]*inp['emitx'][itest])
                if inp['bety'][itest]>zero and inp['emity'][itest]>zero : inp['sigy1'][itest] =  np.sqrt(inp['bety'][itest]*inp['emity'][itest])
                if inp['betx'][itest]>zero and inp['emitx'][itest]>zero and inp['bety'][itest]>zero and inp['emity'][itest]>zero: itest+=1

            iel=inp.size-1
            rat=0

            if inp['sigx1'][0]>0:
                rat=pow(inp['sigy1'][0],2)*inp['betx'][0]/(pow(inp['sigx1'][0],2)*inp['bety'][0])
            if pow(inp['sigx1'][0],2)*inp['bety'][0]<pow(inp['sigy1'][0],2)*inp['betx'][0]:
                rat=2
            if inp['emity'][0]>inp['emitx'][0]:
                rat=0
                dummy=np.copy(inp['betx'])
                inp['betx']=inp['bety']
                inp['bety']=dummy
                dummy=np.copy(inp['betx2'])
                inp['betx2']=inp['bety2']
                inp['bety2']=dummy
                dummy=np.copy(inp['sigx1'])
                inp['sigx1']=inp['sigy1']
                inp['sigy1']=dummy
                dummy=np.copy(inp['sigxavgnld'])
                inp['sigxavgnld']=inp['sigyavgnld']
                inp['sigyavgnld']=dummy
                dummy=np.copy(inp['emitx'])
                inp['emitx']=inp['emity']
                inp['emity']=dummy

            sigma=np.sqrt(inp['betx'][0]*Elhc/Einj)
            if abs(inp['emity'][0])>0 and abs(inp['sigx1'][0])>0:
                if abs(inp['emitx'][0])<zero :
                    rad=np.sqrt(1+(pow(inp['sigy1'][0],2)*inp['betx'][0])/(pow(inp['sigx1'][0],2)*inp['bety'][0]))/sigma
                else:
                    rad=np.sqrt((abs(inp['emitx'][0])+abs(inp['emity'][0]))/abs(inp['emitx'][0]))/sigma
            else:
                rad=1
            if abs(inp['sigxavgnld'][0])>zero and abs(inp['bety'][0])>zero and sigma > 0:
                if abs(inp['emitx'][0]) < zero :
                    rad1=np.sqrt(1+(pow(inp['sigyavgnld'][0],2)*inp['betx'][0])/(pow(inp['sigxavgnld'][0],2)*inp['bety'][0]))/sigma
                else:
                    rad1=(inp['sigyavgnld'][0]*np.sqrt(inp['betx'][0])-inp['sigxavgnld'][0]*np.sqrt(inp['bety2'][0]))/(inp['sigxavgnld'][0]*np.sqrt(inp['bety'][0])-inp['sigyavgnld'][0]*np.sqrt(inp['betx2'][0]))
                    rad1=np.sqrt(1+rad1*rad1)/sigma
            else:
                rad1 = 1
            for i in range(0,iel+1):
                if ich1 == 0 and (inp['distp'][i] > 2. or inp['distp'][i]<=0.5):
                    ich1 = 1
                    achaos=rad*inp['sigx1'][i]
                    iin=i
                if ich3 == 0 and inp['dist'][i] > 1e-2 :
                    ich3=1
                    iend=i
                    achaos1=rad*inp['sigx1'][i]
                if ich2 == 0 and  (inp['sturns1'][i]<inp['turn_max'][i] or inp['sturns2'][i]<inp['turn_max'][i]):
                    ich2 = 1
                    alost2 = rad*inp['sigx1'][i]
            icount=1.
            if iin != -999 and iend == -999 : iend=iel
            if iin != -999 and iend > iin :
                for i in range(iin,iend+1) :
                    if(abs(rad*inp['sigx1'][i])>zero):
                        alost1 += rad1 * inp['sigxavgnld'][i]/rad/inp['sigx1'][i]
                    if(i!=iend):
                        icount+=1.
                alost1 = alost1/icount
                if alost1 >= 1.1 or alost1 <= 0.9:  alost1= -1. * alost1
            else:
                alost1 = 1.0

            alost1=alost1*alost2
            name2 = "DAres."+self.studyName+"."+sixdesktunes+"."+turnse
            name1= '%s%ss%s%s-%s%s.%d'%(LHCDesName,seed,sixdesktunes,ns1l, ns2l, turnse,anumber)
            if(seed<10):
                name1+=" "
            if(anumber<10):
                name1+=" "
            fmt=' %s  %10.6f  %10.6f  %10.6f  %10.6f  %10.6f  %10.6f\n'
            fhdot.write(fmt%( name1[:39],achaos,achaos1,alost1,alost2,rad*inp['sigx1'][0],rad*inp['sigx1'][iel]))
            fhtxt.write('%s %s %s %s %s %s %s %s %s \n'%( name2, seed,angle,achaos,achaos1,alost1,alost2,rad*inp['sigx1'][0],rad*inp['sigx1'][iel]))
        anumber+=1
        fhdot.close()
        print fndot
    fhtxt.close()
    print fntxt

    fhtxt = open(fntxt, 'r')
    final=np.genfromtxt(fhtxt,dtype=outtype)
    fhtxt.close()

    fnplot='DAres.%s.%s.%s.plot'%(LHCDesName,sixdesktunes,turnse)
    fhplot = open(fnplot, 'w')
    fn=0
    for angle in np.unique(final['angle']):
        fn+=1
        study= final['study'][0]
        idxangle=final['angle']==angle
        idx     =idxangle&(final['alost1']!=0)
        idxneg  =idxangle&(final['alost1']<0)
        mini, smini = np.min(np.abs(final['alost1'][idx])), np.argmin(np.abs(final['alost1'][idx]))
        maxi, smaxi = np.max(np.abs(final['alost1'][idx])), np.argmax(np.abs(final['alost1'][idx]))
        toAvg = np.abs(final['alost1'][idx])
        i = len(toAvg)
        mean = np.mean(toAvg)
        idxneg = (final['angle']==angle)&(final['alost1']<0)
        eqaper = np.where(final['alost2'] == final['Amin'])[0]
        nega = len(final['alost1'][idxneg])
        Amin = np.min(final['Amin'][idxangle])
        Amax = np.max(final['Amax'][idxangle])

        for k in eqaper:
          print "Seed #:  %d Dynamic Aperture below:  %.2f Sigma" %( k, final['Amin'][k])

        if i == 0:
          mini  = -Amax
          maxi  = -Amax
          mean  = -Amax
        else:
          if i < int(self.env_var['iend']):
            maxi = -Amax
          elif len(eqaper)>0:
            mini = -Amin
          print "Minimum:  %.2f  Sigma at Seed #: %d" %(mini, smini)
          print "Maximum:  %.2f  Sigma at Seed #: %d" %(maxi, smaxi)
          print "Average: %.2f Sigma" %(mean)
        print "# of (Aav-A0)/A0 >10%%:  %d"  %nega
        fhplot.write('%s %d %.2f %.2f %.2f %d %.2f %.2f\n'%(name2, fn, mini, mean, maxi, nega, Amin, Amax))
    fhplot.close()

if __name__ == '__main__':
  SixDeskDB.from_dir('/home/monis/Desktop/GSOC/files/w7/sixjobs/')



