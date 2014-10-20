# Store the study to a local database
#
# Moonis Javed <monis.javed@gmail.com>,
# Riccardo De Maria <riccardo.de.maria@cern.ch>
# Xavier Valls Pla  <xavier.valls.pla@cern.ch>
# Danilo Banfi <danilo.banfi@cern.ch>
#
# This software is distributed under the terms of the GNU Lesser General Public
# License version 2.1, copied verbatim in the file ``COPYING''.

# In applying this licence, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as an Intergovernmental Organization or
# submit itself to any jurisdiction.

# NOTA: please use python version >=2.6

import sqlite3, time, os, re, gzip, sys, glob
from cStringIO import StringIO
import copy

try:
  import numpy as np
  import matplotlib
  if 'DISPLAY' not in os.environ:
      matplotlib.use('Agg')
  import matplotlib.pyplot as pl
  import scipy.signal
except ImportError:
  print "No module found: numpy matplotlib and scipy modules should be present to run sixdb"
  raise ImportError

import tables
import lsfqueue
import madout
from sqltable import SQLTable
for t in (np.int8, np.int16, np.int32, np.int64,np.uint8, np.uint16, np.uint32, np.uint64):
  sqlite3.register_adapter(t, long)

def parse_env(studydir):
  tmp="sh -c '. %s/sixdeskenv;. %s/sysenv; python -c %s'"
  cmd=tmp%(studydir,studydir,'"import os;print os.environ"')
  return eval(os.popen(cmd).read())


def compressBuf(filename):
  '''file compression for storing in DB'''
  if os.path.isfile(filename):
      buf = open(filename,'r').read()
      zbuf = StringIO()
      zfile = gzip.GzipFile(mode = 'wb',  fileobj = zbuf, compresslevel = 9)
      zfile.write(buf)
      zfile.close()
      return zbuf.getvalue()
  else:
      print "Warning: %s not found"%filename
      return ''

def decompressBuf(buf):
  '''file decompression to retrieve from DB'''
  zbuf = StringIO(buf)
  f = gzip.GzipFile(fileobj=zbuf)
  return f.read()

def isint(s):
  try:
    int(s)
    return True
  except ValueError:
    pass

def isfloat(s):
  try:
    float(s)
    return True
  except ValueError:
    pass

def obj2num(s):
   try:
     return int(s)
   except ValueError:
     try:
         return float(s)
     except ValueError:
         return s

def tune_dir(tune):
  """converts the list of tuples into the standard directory name, e.g. (62.31, 60.32) -> 62.31_60.32"""
  return str(tune[0])+'_'+str(tune[1])

def col_count(cur, table):
  sql = 'pragma table_info(%s)' % (table)
  cur.execute(sql)
  return len(cur.fetchall())

def mk_dir(dirname):
   if not os.path.isdir(dirname):
     try:
       os.mkdir(dirname)
       print "Make dir %s"%dirname
     except OSError,msg:
       print "Error creating dir %s"%dirname
       print "OSError:", msg
   return dirname

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

def split_job_params(dirname):
  ll=dirname.split('/')
  data=ll[-6:]
  seed,simul,tunes,rng,turns,angle=data
  seed=int(seed)
  tunex,tuney=map(float,tunes.split('_'))
  amp1,amp2=map(float,rng.split('_'))
  angle=float(angle)
  #turns=10**int(turns[1])
  return seed,simul,tunex,tuney,amp1,amp2,turns,angle

def get_job_dirname(seed,simul,tunex,tuney,amp1,amp2,turne,angle):
  t='%s/%s/%s/%s/%s/%s/'
  #turne=np.log10(turns)
  rng="%s_%s"%(amp1,amp2)
  tunes="%s_%s"%(tunex,tuney)
  return t%(seed,simul,tunes,rng,turne,angle)



def check_sixdeskenv(studyDir):
  sixdeskenv=os.path.join(studyDir,'sixdeskenv')
  sysenv=os.path.join(studyDir,'sysenv')
  if not (os.path.exists(sixdeskenv) and os.path.exists(sysenv)):
      msg="Error: sixdeskenv or sysenv not found in %s:"%studyDir
      print "Error: sixdeskenv or sysenv not found in %s:"%studyDir,
      raise ValueError,msg
  return sixdeskenv,sysenv


class SixDeskDB(object):
  @classmethod
  def from_dir(cls,studyDir):
    '''create local Database for storing study'''
    sixdeskenv,sysenv=check_sixdeskenv(studyDir)
    env_var = parse_env(studyDir)
    dbname = env_var['LHCDescrip'] + ".db"
    db=cls(dbname,create=True)
    db.update_sixdeskenv(studyDir)
    db.st_mad6t_run()
    db.st_mad6t_run2()
    db.st_mad6t_results()
    db.st_six_beta()
    db.st_six_input()
    # db.st_six_results()
    return db

  def update_sixdeskenv(self,studyDir):
    sixdeskenv,sysenv=check_sixdeskenv(studyDir)
    self.add_files([['sixdeskenv',sixdeskenv],['sysenv',sysenv]])
    env_var = parse_env(studyDir)
    for key in env_var.keys():
      if key not in tables.acc_var:
        del env_var[key]
    mtime=time.time()
    self.set_variables(env_var.items(),mtime)

  def add_files(self,files):
    """add files in  key,realpath list"""
    cols=SQLTable.cols_from_fields(tables.Files.fields)
    filetab = SQLTable(self.conn,'files',cols,tables.Files.key)
    toinsert=[]
    for key,path in files:
        mtime=os.path.getmtime(path)
        content = sqlite3.Binary(compressBuf(path))
        toinsert.append([key,content,mtime])
    filetab.insertl(toinsert)
  def __init__(self,dbname,create=False):
    '''initialise variables and location for study creation 
        or database creation, usage listed in main.py'''
    if not dbname.endswith('.db'):
        dbname+='.db'
    if create is False and not os.path.exists(dbname):
        raise ValueError,"File %s not found"%dbname
    try:
      conn = sqlite3.connect(dbname,isolation_level="IMMEDIATE")
      cur = conn.cursor()
      cur.execute("PRAGMA synchronous = OFF")
      cur.execute("PRAGMA journal_mode = MEMORY")
      cur.execute("PRAGMA auto_vacuum = FULL")
      cur.execute("PRAGMA temp_store = MEMORY")
      cur.execute("PRAGMA count_changes = OFF")
      cur.execute("PRAGMA mmap_size=2335345345")
    except sqlite3.Error:
      print 'Error creating database %s'%dbname
      sys.exit(1)
    print "Opened %s successfully"%dbname
    self.conn = conn
    conn.text_factory=str
    self.load_env()

  def load_env(self):
    cols=SQLTable.cols_from_fields(tables.Env.fields)
    tab = SQLTable(self.conn,'env',cols,tables.Env.key)
    sql="SELECT keyname,value,mtime FROM env"
    cur=self.conn.cursor()
    cur.execute(sql)
    self.env_var={}
    self.env_mtime={}
    for key,val,mtime in cur.fetchall():
      self.env_var[key]=val
      self.env_mtime[key]=mtime
    self.LHCDescrip=self.env_var.get('LHCDescrip')

  def print_table_info(self):
      out=self.execute("SELECT name FROM sqlite_master WHERE type='table';")
      tables=[str(i[0]) for i in out]
      for tab in tables:
          rows=self.execute("SELECT count(*) FROM %s"%tab)[0][0]
          columms=[i[1] for i in self.execute("PRAGMA table_info(%s)"%tab)]
          print "%s(%d):\n  %s"%(tab,rows,', '.join(columms))

  def mad_out(self):
      mad_runs=self.execute('SELECT DISTINCT run_id FROM mad6t_run')
      if len(mad_runs)==0:
          print "No mad out data"
      for run, in mad_runs:
          print "Checking %s"%run
          sql="SELECT seed,mad_out FROM mad6t_run WHERE run_id=='%s' ORDER BY seed"%run
          data=self.execute(sql)
          data=[(seed,StringIO(decompressBuf(out))) for seed,out in data]
          resname=os.path.join(self.mk_analysis_dir(),run)+'.csv'
          madout.check_mad_out(data,resname)
          print resname


  def set_variables(self,lst,mtime):
    '''set additional variables besides predefined environment variables
        in sixdeskenv and sysenv'''
    conn = self.conn
    env_var = self.env_var
    cols=SQLTable.cols_from_fields(tables.Env.fields)
    tab = SQLTable(conn,'env',cols,tables.Env.key)
    toupdate=[]
    for key,val in sorted(lst):
      val=obj2num(val)
      if key in tables.acc_var:
          if key in self.env_var:
            oldval=self.env_var[key]
            if oldval!=val:
              print "Updating %s from %s to %s"%(key,oldval,val)
              toupdate.append([key,val,mtime])
          else:
             toupdate.append([key,val,mtime])
      else:
        print 'variable %s illegal'%key
    print "Inserting or updating %d variables"%(len(toupdate))
    tab.insertl(toupdate)
    self.load_env()

  def info(self):
    ''' provide info of study'''
    var = ['LHCDescrip', 'platform', 'madlsfq', 'lsfq', 'runtype', 'e0',
    'gamma', 'beam', 'dpini', 'istamad', 'iendmad', 'ns1l', 'ns2l', 'nsincl', 
    'sixdeskpairs', 'turnsl', 'turnsle', 'writebinl',
    'kstep', 'kendl', 'kmaxl',
    'trackdir', 'sixtrack_input']
    env_var = self.env_var
    for keys in var:
      val=env_var[keys]
      if isfloat(val):
          val="%6g"%float(val)
      print '%-15s %s'%(keys,val)

  def st_mad6t_run(self):
    ''' store mad run files'''
    conn = self.conn
    env_var = self.env_var
    cols = SQLTable.cols_from_fields(tables.Mad_Run.fields)
    tab = SQLTable(conn,'mad6t_run',cols,tables.Mad_Run.key)
    extra_files = []
    a = []
    workdir = env_var['sixtrack_input']
    a = tab.selectl('distinct run_id')
    if a:
      a = [str(i[0]) for i in a]
    print "Looking for fort.2, fort.8, fort.16 in %s"%workdir
    data=[]
    for dirName in glob.glob(os.path.join(workdir,'mad.dorun_*')):
        print 'found mad run',dirName.split('/')[-1]
        for filename in os.listdir(dirName):
          if not (filename.endswith('.mask') or 'out' in filename
              or filename.endswith('log') or filename.endswith('lsf')):
            fnroot,seed=os.path.splitext(filename)
            seed=int(seed[1:])
            run_id = dirName.split('/')[-1]
            mad_in = sqlite3.Binary(
              compressBuf(os.path.join(dirName, filename))
              )
            out_file=os.path.join(dirName,fnroot+'.out.%d'%seed)
            log_file=os.path.join(dirName,fnroot+'_mad6t_%d.log'%seed)
            lsf_file=os.path.join(dirName,'mad6t_%d.lsf'%seed)
            mad_out = sqlite3.Binary(compressBuf(out_file))
            mad_lsf = sqlite3.Binary(compressBuf(lsf_file))
            mad_log = sqlite3.Binary(compressBuf(log_file))
            time = os.path.getmtime( log_file)
            data.append([run_id, seed, mad_in, mad_out, mad_lsf,mad_log,time])
          if filename.endswith('.mask'):
            path = os.path.join(dirName, filename)
            key = path.replace(env_var['scratchdir']+'/','')
            extra_files.append([key,path])
        if len(data)>0:
          tab.insertl(data)
    if extra_files:
      self.add_files(extra_files)

  def st_mad6t_run2(self):
    ''' store fort.3 and tmp files'''
    conn = self.conn
    env_var = self.env_var
    workdir = env_var['sixtrack_input']
    extra_files = []
    for filename in os.listdir(workdir):
      if 'fort.3' in filename or filename.endswith('.tmp'):
        path = os.path.join(workdir, filename)
        key =  os.path.join('sixtrack_input',filename)
        extra_files.append([key,path])
    if extra_files:
      self.add_files(extra_files)

  def st_mad6t_results(self):
    ''' store fort.2, fort.8, fort.16 files'''
    conn = self.conn
    env_var = self.env_var
    cols = SQLTable.cols_from_fields(tables.Mad_Res.fields)
    tab = SQLTable(conn,'mad6t_results',cols,tables.Mad_Res.key)
    maxtime = tab.selectl("max(fort_mtime)")[0][0]
    if not maxtime:
      maxtime = 0
    rows = []
    workdir = env_var['sixtrack_input']
    f2s = glob.glob(os.path.join(workdir,'fort.2_*.gz'))
    f8s = glob.glob(os.path.join(workdir,'fort.8_*.gz'))
    f16s = glob.glob(os.path.join(workdir,'fort.16_*.gz'))
    seeds=sorted(set([i.split('_')[-1].split('.')[0] for i in f2s+f8s+f16s]))
    update={2:0,8:0,16:0}
    for seed in seeds:
        row=[seed]
        for fn in [2,8,16]:
            ffn=os.path.join(workdir,'fort.%d_%s.gz'%(fn,seed))
            if os.path.exists(ffn):
                mtime=os.path.getmtime(ffn)
                #if mtime >maxtime: #bug to be checked
                row.append(sqlite3.Binary(open(ffn, 'r').read()))
                update[fn]+=1
            else:
              print "%s missing inserted null"%ffn
              row.append("")
        row.append(mtime)
        rows.append(row)
    if rows:
      tab.insertl(rows)
      rows = {}
    print ' number of fort.2 updated/found: %d/%d'%(update[2],len(f2s))
    print ' number of fort.8 updated/found: %d/%d'%(update[8],len(f8s))
    print ' number of fort.16 updated/found: %d/%d'%(update[16],len(f16s))

  def st_six_beta(self):
    ''' store sixdesktunes, betavalues '''
    conn = self.conn
    env_var = self.env_var
    cols = SQLTable.cols_from_fields(tables.Six_Be.fields)
    tab = SQLTable(conn,'six_beta',cols,tables.Six_Be.key)
    workdir = os.path.join(env_var['sixdesktrack'],self.LHCDescrip)
    data=[]
    print "Looking for sixdesktunes, betavalues in\n %s"%workdir
    gen_input=os.path.join(workdir,'general_input')
    if not os.path.exists(gen_input):
      print "Warning: %s not found"%gen_input
      gen=[0,0]
    else:
      content = sqlite3.Binary(compressBuf(gen_input))
      gen=[float(i) for i in open(gen_input).read().split()]
    for dirName in glob.glob('%s/*/simul/*'%workdir):
      dirn = dirName.split('/')
      seed = int(dirn[-3])
      tunex, tuney = dirn[-1].split('_')
      vals=[seed,tunex,tuney]
      lastmtime=0
      try:
        for fn in ['betavalues','sixdesktunes']:
          fullname=os.path.join(dirName,fn)
          mtime=os.path.getmtime(fullname)
          if mtime >lastmtime:
              lastmtime=mtime
          vals+=[float(i) for i in open(fullname).read().split()]
        vals.extend(gen)
        vals.append(mtime)
        data.append(vals)
      except ValueError:
        print "Error in %s"%fullname
    print " number of sixdesktunes, betavalues, mychrom inserted: %d"%len(data)
    tab.insertl(data)

  def st_six_input(self):
    ''' store input values (seed,tunes,amps,etc) along with fort.3 file'''
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    cols = SQLTable.cols_from_fields(tables.Six_In.fields)
    tab = SQLTable(conn,'six_input',cols,tables.Six_In.key)
    cols1 = SQLTable.cols_from_fields(tables.Six_Res.fields)
    tab1 = SQLTable(conn,'six_results',cols1,tables.Six_Res.key)
    f3_data={}
    for row in cur.execute('SELECT id,mtime,seed,simul,tunex,tuney,amp1,amp2,turns,angle FROM six_input'):
        f3_data[tuple(row[2:])]=row[:2]
    f10_data={}
    for row in cur.execute('SELECT DISTINCT six_input_id,mtime FROM six_results'):
        f10_data[row[0]]=row[1]
    count3 = 0
    count10 = 0
    workdir = os.path.join(env_var['sixdesktrack'],self.LHCDescrip)
    extra_files = []
    rows3 = []
    rows10 = []
    six_id_new = list(cur.execute('SELECT max(id) FROM six_input'))[0][0]
    if six_id_new is None:
      six_id_new=1
    else:
      six_id_new+=1
    print "Looking for fort.3.gz, fort.10.gz files in\n %s"%workdir
    file_count3=0
    file_count10 = 0
    for dirName in glob.iglob(os.path.join(workdir,'*','*','*','*','*','*')):
      f3=os.path.join(dirName, 'fort.3.gz')
      f10=os.path.join(dirName, 'fort.10.gz')
      ranges= dirName.split('/')[-3]
      if '_' in ranges:
        fn3_exists=fn10_exists=False
        if os.path.exists(f3):
            file_count3+=1
            fn3_exists=True
        if os.path.exists(f10):
            file_count10 += 1
            fn10_exists=True
        if file_count3%100==0:
           sys.stdout.write('.')
           sys.stdout.flush()
        if fn3_exists:
            jobparmams=split_job_params(dirName)
            six_id_old,mtime3_old=f3_data.get(jobparmams,[-1,0])
            mtime3 = os.path.getmtime(f3)
            if six_id_old==-1:
                six_id=six_id_new
                six_id_new+=1
            else:
                six_id=six_id_old
            if mtime3>mtime3_old:
              count3+=1
              f3file=sqlite3.Binary(compressBuf(f3))
              dirn = [six_id] + list(jobparmams) + [f3file,mtime3]
              rows3.append(dirn)
        if fn10_exists:
           mtime10 = os.path.getmtime(f10)
           mtime10_old=f10_data.get(six_id,0)
           if mtime10 > mtime10_old and os.path.getsize(f10)>0:
             countl = 1
             for lines in gzip.open(f10,"r"):
                rows10.append([six_id,countl]+lines.split()+[mtime10])
                countl += 1
             count10 += 1
        if len(rows3) == 6000:
          tab.insertl(rows3)
          tab1.insertl(rows10)
          rows3 = []
          rows10 = []
    tab.insertl(rows3)
    tab1.insertl(rows10)
    rows3 = []
    print '\n number of fort.3 updated/found: %d/%d'%(count3,file_count3)
    print ' number of fort.10 updated/found: %d/%d'%(count10,file_count10)
    sql="""CREATE VIEW IF NOT EXISTS results
           AS SELECT * FROM six_input INNER JOIN six_results
           ON six_input.id==six_results.six_input_id"""
    cur.execute(sql)
    sql="""SELECT count(*) FROM results"""
    results=list(cur.execute(sql))[0][0]
    sql="""SELECT COUNT(DISTINCT six_input_id) FROM six_results"""
#    sql="""SELECT COUNT(*) FROM six_results"""
    jobs=list(cur.execute(sql))[0][0]
    print " db now contains %d results from %d jobs"% (results,jobs)

  def execute(self,sql):
    cur= self.conn.cursor()
    cur.execute(sql)
    self.conn.commit()
    return list(cur)

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
        temp = path+'/'+self.LHCDescrip+'.'+str(file[1])
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
        f = open(temp,'w')
        f.write(decompressBuf(mad_in))
        temp = path+'/'+self.LHCDescrip+'.out.'+str(file[1])
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
        temp = path+'/'+self.LHCDescrip+'_mad_'+str(str(file[1])+'.log')
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
    path = os.path.join(env_var['sixdesktrack'],self.LHCDescrip)
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
    path = os.path.join(env_var['sixdesktrack'],self.LHCDescrip)
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
        temp = path1+'/fort.3.gz'
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
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
      if not dryrun:
        temp = path1+'/fort.10.gz'
        if os.path.exists(temp):
          print 'file already exists please remove it and try again'
          exit(0)
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
      print ' no fort.10 files missing'
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
      print ' no fort.10 files incomplete'
      return 0

#  def join10(self):
#    '''re-implementation of run_join10'''
#    verbose = self.verbose
#    dryrun = self.dryrun
#    conn = self.conn
#    cur = conn.cursor()
#    env_var = self.env_var
#    ista = int(env_var['ista'])
#    iend = int(env_var['iend'])
#    tuney = float(env_var['tuney'])
#    tunex = float(env_var['tunex'])
#    tune = str(tunex)+'_'+str(tuney)
#    print env_var['short'],env_var['long']
#    if env_var['short'] == 1:
#      amp1 = int(env_var['ns1s'])
#      amp2 = int(env_var['ns2s'])
#      ampd = int(env_var['nss'])
#    else:
#      amp1 = int(env_var['ns1l'])
#      amp2 = int(env_var['ns2l'])
#      ampd = int(env_var['nsincl'])
#    sql = """SELECT distinct turns,angle from six_input"""
#    cur.execute(sql)
#    val = cur.fetchall()
#    for seed in range(ista,iend+1):
#      workdir = os.path.join( env_var['sixdesktrack'],self.LHCDescrip,str(seed),'simul',tune)
#      join = os.path.join(workdir,str(amp1)+'-'+str(amp2))
#      #print join
#      for amp in range(amp1,amp2,ampd):
#        sql = """SELECT * from results
#        where six_input_id=id and seed=? and amp1=? and amp2=?"""
#        cur.execute(sql,[seed,amp,amp+2])
#        data = cur.fetchall()
#        while data:
#          path = os.path.join(join,str(data[0][7]),str(data[0][8]))
#          if not os.path.exists(path):
#            if not dryrun:
#              os.makedirs(path)
#            if verbose:
#              print 'creating directory',path
#          if amp == amp1:
#            if not dryrun:
#              f = gzip.open(os.path.join(path,'fort.10.gz'),'w')
#            if verbose:
#              print 'creating joined fort.10 file at',path
#            #print os.path.join(path,'fort.10.gz')
#          else:
#            if not dryrun:
#              f = gzip.open(os.path.join(path,'fort.10.gz'),'a')
#          if not dryrun: for j in xrange(30):
#              str1 = '\t'.join(
#                [str(data[0][i]) for i in range(12,72)]
#                )
#              str1 += '\n'
#              f.write(str1)
#              del data[0]
#            f.close()

  def st_boinc(self,conn):
    '''store fort.10 files from boinc directory to local DB '''
    env_var = self.env_var
    study = self.LHCDescrip
    cols=SQLTable.cols_from_fields(tables.Six_Res.fields)
    cols = [i.replace("STRING","VARCHAR(128)") for i in cols]
    cols = ['study VARCHAR(128)'] + cols
    tab = SQLTable(conn,'six_results',cols)
    maxtime = tab.selectl("max(mtime)")[0][0]
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
    sql="""SELECT seed,tunex,tuney,amp1,amp2,turns,angle,row_num
           FROM results"""
    return self.conn.cursor().execute(sql)

  def iter_job_params_comp(self):
    names='seed,tunex,tuney,amp1,amp2,turns,angle'
    sql='SELECT DISTINCT %s FROM six_input'%names
    return self.conn.cursor().execute(sql)

  def get_num_results(self):
    ''' get results count from DB '''
    return self.execute('SELECT count(*) FROM six_results')[0][0]/30

  def get_turnsl(self):
    ''' get turnsl = maximum number of turns for long queue from env table'''
    env_var = self.env_var
    return int(env_var['turnsl'])

  def get_seeds(self):
    ''' get seeds from env table'''
    env_var = self.env_var
    ista = int(env_var['ista'])
    iend = int(env_var['iend'])
    return range(ista,iend+1)

  def get_db_seeds(self):
    ''' get seeds from DB'''
    out=zip(*self.execute('SELECT DISTINCT seed FROM six_input'))[0]
    return out

  def check_seeds(self):
    """check if seeds defined in the environment are presently available in the database"""
    return not len(set(self.get_seeds())-set(self.get_db_seeds()))>0

  def check_angles(self):
    """check if angles defined in the environment are presently available in the database"""
    return not len(set(self.get_angles())-set(self.get_db_angles()))>0

  def get_angles(self):
    ''' get angles from env variables'''
    env_var = self.env_var
    kmaxl = int(env_var['kmaxl'])
    kinil = int(env_var['kinil'])
    kendl = int(env_var['kendl'])
    kstep = int(env_var['kstep'])
    s=90./(kmaxl+1)
    return np.arange(kinil,kendl+1,kstep)*s

  def get_db_angles(self):
    '''get angles from DB'''
    out=zip(*self.execute('SELECT DISTINCT angle FROM six_input'))[0]
    return out

  def get_amplitudes(self):
    '''get_amplitudes from env variables '''
    env_var = self.env_var
    nsincl = float(env_var['nsincl'])
    ns1l = float(env_var['ns1l'])
    ns2l = float(env_var['ns2l'])
    return [(a,a+nsincl) for a in np.arange(ns1l,ns2l,nsincl)]

  def iter_tunes(self):
    '''get tunes from env variables'''
    env_var = self.env_var
    qx = float(env_var['tunex'])
    qy = float(env_var['tuney'])
    while qx <= float(env_var['tunex1']) and qy <= float(env_var['tuney1']):
      yield qx,qy
      qx += float(env_var['deltax'])
      qy += float(env_var['deltay'])

  def get_tunes(self):
    '''get tunes from env variables'''
    return list(self.iter_tunes())

  def gen_job_params(self):
    '''generate jobparams based on values '''
    if self.env_var['long']==1:
      simul='simul'
      turns = '%E'%(float(self.env_var['turnsl']))
      turns = 'e'+str(int(turns.split('+')[1]))
      for seed in self.get_seeds():
        for tunex,tuney in self.get_tunes():
          for amp1,amp2 in self.get_amplitudes():
            for angle in self.get_angles():
              yield (seed,simul,tunex,tuney,amp1,amp2,turns,angle)
    if self.env_var['short']==1:
      simul='short'
      turns = '%E'%(float(self.env_var['turnss']))
      turns = 'e'+str(int(turns.split('+')[1]))
      for seed in self.get_seeds():
        for tunex,tuney in self.get_tunes():
          for amp1,amp2 in self.get_amplitudes():
            for angle in self.get_angles():
              yield (seed,simul,tunex,tuney,amp1,amp2,turns,angle)


  def get_missing_jobs(self):
    '''get missing jobs '''
    turnsl = '%E'%(float(self.env_var['turnsl']))
    turnsl = 'e'+str(int(turnsl.split('+')[1]))
    existing = self.execute("""SELECT seed,simul,tunex,tuney,amp1,amp2,
                               turns,angle from six_input""")
    existing= set(existing)
    needed=set(self.gen_job_params())
    #print sorted(existing)[0]
    #print sorted(needed)[0]
    return list(needed-existing)

  def get_running_jobs(self,missing,threshold=7*24*3600):
    running=set()
    for lsfjob in lsfqueue.parse_bjobs():
        if lsfjob in missing:
          job=self.running[lsfjob]
          if job.stat in ('PEND','RUN'):
            tmp="TrackOut job: %s %s %s %s %s"
            print tmp%(job.jobid,jobshort,job.submit_time,job.start_time,job.stat)
            if not (job.stat=='RUN' and job.run_since()>threshold):
               running.add(lsfjob)
    return running

  def make_lsf_missing_jobs(self):
    for job in self.get_missing_jobs():
       seed,simul,tunex,tuney,amp1,amp2,turns,angle=job
       tmp="%s%%%s%%s%%%s%%%s%%%s%%%s"
       ranges="%g_%g"%(amp1,amp2)
       tunes="%s_%s"%(tunex,tuney)
       missing.add(tmp%(name,seed,tunes,ranges,turns,angle))
    if len(missing)==0:
        print "No missing jobs"
    running=self.get_running_jobs(missing)
    if len(running)>0:
        print "%d job running"
    for job in missing-running:
        print job

  def inspect_jobparams(self):
    data=list(self.iter_job_params())
    names='seed,tunex,tuney,amp1,amp2,turns,angle,row'.split(',')
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
    return p

  def get_polar_col(self,col,seed,smooth=None):
    a,s,t=self.get_2d_col(col,seed)
    rad=np.pi*a/180
    x=s*np.cos(rad)
    y=s*np.sin(rad)
    t=self._movavg2d(t,smooth=smooth)
    return x,y,t
  def get_2d_col(self,col,seed):
    cmd="""SELECT angle,amp1+(amp2-amp1)*row_num/30,
            %s
            FROM results WHERE seed=%s ORDER BY angle,amp1,row_num"""
    cur=self.conn.cursor().execute(cmd%(col,seed))
    ftype=[('angle',float),('amp',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    angles=len(set(data['angle']))
    data=data.reshape(angles,-1)
    a,s,t=data['angle'],data['amp'],data['surv']
    return a,s,t
  def get_3d_col(self,col,cond=''):
    cmd="""SELECT seed,angle,amp1+(amp2-amp1)*row_num/30,
            %s
            FROM results %s ORDER BY seed,angle,amp1,row_num"""
    cur=self.conn.cursor().execute(cmd%(col,cond))
    ftype=[('seed',float),('angle',float),('amp',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    angles=len(set(data['angle']))
    seeds=len(set(data['seed']))
    data=data.reshape(seeds,angles,-1)
    ss,a,s,t=data['seed'],data['angle'],data['amp'],data['surv']
    return ss,a,s,t
  def plot_polar_col(self,col,seed,smooth=None):
    x,y,t=self.get_polar_col(col,seed)
    self._plot_polar(x,y,t,smooth=smooth)
    pl.title("%s: seed %d"%(col,seed))
  def plot_polarlog_col(self,col,seed,smooth=None,base=1):
    x,y,t=self.get_polar_col(col,seed)
    self._plot_polar(x,y,np.log10(t+base),smooth=smooth)
    pl.title("%s: seed %d"%(col,seed))
  def _movavg2d(self,t,smooth=None):
    if smooth is not None:
      if type(smooth)==int:
        smooth=np.ones((1.,smooth))/smooth
      elif type(smooth)==tuple:
        smooth=np.ones(smooth)/float(smooth[0]*smooth[1])
      s1,s2=smooth.shape
      s1-=1
      s2-=1
      t=scipy.signal.fftconvolve(smooth,t,mode='full')
      t[:,:s2]=t[:,s2,np.newaxis]
      t[:s1,:]=t[np.newaxis,s1,:]
    return t
  def _plot_polar(self,x,y,t,smooth=None):
    t=self._movavg2d(t,smooth=smooth)
    #pl.scatter(x,y,c=t,edgecolor='none',marker='o')
    #pl.pcolormesh(x,y,t,antialiased=True)
    pl.pcolormesh(x,y,t)
    pl.xlabel(r'$\sigma_x$')
    pl.ylabel(r'$\sigma_y$')
    pl.colorbar()
  def plot_polar_col_avg(self,col,smooth=None,cond=''):
    cmd="""SELECT %s
            FROM results ORDER BY seed,angle,amp1,row %"""
    cur=self.conn.cursor().execute(cmd%(col,cond))
    ftype=[('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    cmd="""SELECT COUNT(DISTINCT seed),COUNT(DISTINCT angle)
           FROM results %s"""
    self.conn.cursor().execute(cmd%cond)
    seeds,angles=self.conn.cursor().execute(cmd).fetchone()
    data=data.reshape(seeds,angles,-1)
    x,y,t=self.get_polar_col(col,1)
    self._plot_polar(x,y,data['surv'].mean(axis=0),smooth=smooth)
    pl.title('Survived turns')
  def get_col(self,col,seed,angle):
    cmd="""SELECT amp1+(amp2-amp1)*row_num/30,
            %s
            FROM results WHERE seed=%s AND angle=%s
            ORDER BY amp1,row_num"""
    cur=self.conn.cursor().execute(cmd%(col,seed,angle))
    ftype=[('amp',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    a,t=data['amp'],data['surv']
    return a,t
  def plot_col(self,col,seed,angle,lbl=None,ls='-'):
    a,t=self.get_col(col,seed,angle)
    if lbl==None:
      llb=col
    pl.plot(a,t,ls,label=lbl)
  def count_result_byseed(self):
    return self.execute('SELECT seed,count(*) FROM results GROUP BY seed')
  def plot_results(self):
    cmd="""SELECT angle, amp1, count(*)
           FROM results GROUP BY angle, amp1"""
    a,s,t=np.array(self.execute(cmd)).T
    rad=np.pi*a/180
    x=s*np.cos(rad)
    y=s*np.sin(rad)
    angles=self.count_distinct('angle')
    return x,y,t





  def get_survival_turns(self,seed):
    '''get survival turns from DB '''
    cmd="""SELECT angle,amp1+(amp2-amp1)*row_num/30,
        CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
        FROM six_results,six_input WHERE seed=%s AND id=six_input_id
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
    #pl.pcolormesh(x,y,t,antialiased=True)
    pl.scatter(x,y,bs=t)
    pl.xlabel(r'$\sigma_x$')
    pl.ylabel(r'$\sigma_y$')
    #pl.colorbar()

  def mk_analysis_dir(self,seed=None,tunes=None,angle=None):
    '''create analysis directory structure'''
    dirname='dares_'+self.LHCDescrip
    out=[mk_dir(dirname)]
    if seed is not None:
      seed=str(seed)
      seedname=os.path.join(dirname,seed)
      out.append(mk_dir(seedname))
    if tunes is not None:
      tunes=tune_dir(tunes)
      tunename=os.path.join(dirname,seed,tunes)
      out.append(mk_dir(tunename))
    if angle is not None:
      angle=str(angle)
      anglename=os.path.join(dirname,seed,angle)
      out.append(mk_dir(anglename))
    return out[-1]

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

  #@profile
  def read10b(self):
    dirname=self.mk_analysis_dir()
    rectype=[('tunex','float'),('tuney','float'),
             ('seed','int'),('betx','float'),('bety','float'),
             ('sigx1','float'),('sigy1','float'),
             ('emitx','float'),('emity','float'),
             ('sigxavgnld','float'),('sigyavgnld','float'),
             ('betx2','float'),('bety2','float'),
             ('distp','float'),('dist','float'),
             ('sturns1' ,'int'),('sturns2','int'),('turn_max','int'),
             ('amp1','float'),('amp2','float'),('angle','float'),
             ('mtime','float')]
    names=','.join(zip(*rectype)[0])
    turnse=self.env_var['turnse']
    tunex=float(self.env_var['tunex'])
    tuney=float(self.env_var['tuney'])
    sixdesktunes="%g_%g"%(tunex,tuney)
    ns1l=self.env_var['ns1l']
    ns2l=self.env_var['ns2l']
    sql='SELECT %s FROM results ORDER BY tunex,tuney,seed,amp1,amp2,angle'%names
    Elhc,Einj = self.execute('SELECT emitn,gamma from six_beta LIMIT 1')[0]
    anumber=1
    angles=self.get_angles()#np.unique(tmp['angle'])
    seeds=self.get_seeds()#np.unique(tmp['seed'])
    mtime=self.execute('SELECT max(mtime) from results')[0][0]
    final=[]
    sql1='SELECT %s FROM results WHERE betx>0 AND bety>0 AND emitx>0 AND emity>0 '%names
    for angle in angles:
        fndot='DAres.%s.%s.%s.%d'%(self.LHCDescrip,sixdesktunes,turnse,anumber)
        fndot=os.path.join(dirname,fndot)
        fhdot = open(fndot, 'w')
        for seed in seeds:
            ich1 = 0
            ich2 = 0
            ich3 = 0
            icount = 1.
            iin  = -999
            iend = -999
            alost1 = 0.
            alost2 = 0.
            achaos = 0
            achaos1 = 0
            sql=sql1+'AND seed=%g AND angle=%g ORDER BY amp1'%(seed,angle)
            inp=np.array(self.execute(sql),dtype=rectype)
            betx=inp['betx']
            betx2=inp['betx2']
            bety=inp['bety']
            bety2=inp['bety2']
            sigx1=inp['sigx1']
            sigy1=inp['sigy1']
            emitx=inp['emitx']
            emity=inp['emity']
            distp=inp['distp']
            dist=inp['dist']
            sigxavgnld=inp['sigxavgnld']
            sigyavgnld=inp['sigyavgnld']
            sturns1=inp['sturns1']
            sturns2=inp['sturns2']
            turn_max=inp['turn_max'].max()

            if inp.size<2 :
                print 'not enought data for angle = %s' %angle
                break

            zero = 1e-10
            xidx=(betx>zero) & (emitx>zero)
            yidx=(bety>zero) & (emity>zero)
            sigx1[xidx]=np.sqrt(betx[xidx]*emitx[xidx])
            sigy1[yidx]=np.sqrt(bety[yidx]*emity[yidx])
            iel=inp.size-1
            rat=0

            if sigx1[0]>0:
                rat=sigy1[0]**2*betx[0]/(sigx1[0]**2*bety[0])
            if sigx1[0]**2*bety[0]<sigy1[0]**2*betx[0]:
                rat=2
            if emity[0]>emitx[0]:
                rat=0
                dummy=np.copy(betx)
                betx=bety
                bety=dummy
                dummy=np.copy(betx2)
                betx2=bety2
                bety2=dummy
                dummy=np.copy(sigx1)
                sigx1=sigy1
                sigy1=dummy
                dummy=np.copy(sigxavgnld)
                sigxavgnld=sigyavgnld
                sigyavgnld=dummy
                dummy=np.copy(emitx)
                emitx=emity
                emity=dummy

            sigma=np.sqrt(betx[0]*Elhc/Einj)
            if abs(emity[0])>0 and abs(sigx1[0])>0:
                if abs(emitx[0])<zero :
                    rad=np.sqrt(1+(sigy1[0]**2*betx[0])/(sigx1[0]**2*bety[0]))/sigma
                else:
                    rad=np.sqrt((emitx[0]+emity[0])/emitx[0])/sigma
            else:
                rad=1
            if abs(sigxavgnld[0])>zero and abs(bety[0])>zero and sigma > 0:
                if abs(emitx[0]) < zero :
                    rad1=np.sqrt(1+(pow(sigyavgnld[0],2)*betx[0])/(pow(sigxavgnld[0],2)*bety[0]))/sigma
                else:
                    rad1=(sigyavgnld[0]*np.sqrt(betx[0])-sigxavgnld[0]*np.sqrt(bety2[0]))/(sigxavgnld[0]*np.sqrt(bety[0])-sigyavgnld[0]*np.sqrt(betx2[0]))
                    rad1=np.sqrt(1+rad1*rad1)/sigma
            else:
                rad1 = 1
            chaostest=np.where((distp>2.)|(distp<=0.5))[0]
            if len(chaostest)>0:
                iin=chaostest[0]
                achaos=rad*sigx1[iin]
            else:
                iin=iel
            chaos1test=np.where(dist > 1e-2)[0]
            if len(chaos1test)>0:
                iend=chaos1test[0]
                achaos1=rad*sigx1[iend]
            else:
                iend=iel
            alost2test=np.where( (sturns1<turn_max) | (sturns2<turn_max))[0]
            if len(alost2test)>0:
                ialost2=alost2test[0]
                alost2=rad*sigx1[ialost2]
            icount=1.
            if iin != -999 and iend == -999 : iend=iel
            if iin != -999 and iend > iin :
                for i in range(iin,iend+1) :
                    if(abs(rad*sigx1[i])>zero):
                        alost1 += rad1 * sigxavgnld[i]/rad/sigx1[i]
                    if(i!=iend):
                        icount+=1.
                alost1 = alost1/icount
                if alost1 >= 1.1 or alost1 <= 0.9:  alost1= -1. * alost1
            else:
                alost1 = 1.0

            alost1=alost1*alost2
            name2 = "DAres.%s.%s.%s"%(self.LHCDescrip,sixdesktunes,turnse)
            name1= '%s%ss%s%s-%s%s.%d'%(self.LHCDescrip,seed,sixdesktunes,ns1l, ns2l, turnse,anumber)
            if(seed<10):
                name1+=" "
            if(anumber<10):
                name1+=" "
            fmt=' %-39s  %10.6f  %10.6f  %10.6f  %10.6f  %10.6f  %10.6f\n'
            fhdot.write(fmt%( name1[:39],achaos,achaos1,alost1,alost2,rad*sigx1[0],rad*sigx1[iel]))
            final.append([name2, tunex, tuney, int(seed),
                           angle,achaos,achaos1,alost1,alost2,
                           rad*sigx1[0],rad*sigx1[iel],mtime])
        anumber+=1
        fhdot.close()
        print fndot
    cols=SQLTable.cols_from_fields(tables.Da_Post.fields)
    datab=SQLTable(self.conn,'da_post',cols)
    datab.insertl(final)

  def mk_da(self,force=False,nostd=False):
    dirname=self.mk_analysis_dir()
    cols=SQLTable.cols_from_fields(tables.Da_Post.fields)
    datab=SQLTable(self.conn,'da_post',cols)
    final=datab.select(orderby='angle,seed')
    turnse=self.env_var['turnse']
    tunex=float(self.env_var['tunex'])
    tuney=float(self.env_var['tuney'])
    sixdesktunes="%g_%g"%(tunex,tuney)
    ns1l=self.env_var['ns1l']
    ns2l=self.env_var['ns2l']
    if len(final)>0:
        an_mtime=final['mtime'].min()
        res_mtime=self.execute('SELECT max(mtime) FROM six_results')[0][0]
        if res_mtime>an_mtime or force is True:
            self.read10b()
            final=datab.select(orderby='angle,seed')
    else:
      self.read10b()
      final=datab.select(orderby='angle,seed')

    #print final['mtime']
    #print self.execute('SELECT max(mtime) FROM six_results')[0][0]

    fnplot='DAres.%s.%s.%s.plot'%(self.LHCDescrip,sixdesktunes,turnse)
    fnplot= os.path.join(dirname,fnplot)
    fhplot = open(fnplot, 'w')
    fn=0
    for angle in np.unique(final['angle']):
        fn+=1
        study= final['name'][0]
        idxangle=final['angle']==angle
        idx     =idxangle&(final['alost1']!=0)
        idxneg  =idxangle&(final['alost1']<0)
        finalalost=np.abs(final['alost1'][idx])
        imini=np.argmin(finalalost)
        mini=finalalost[imini]
        smini=final['seed'][idx][imini]
        imaxi=np.argmax(finalalost)
        maxi=finalalost[imaxi]
        smaxi=final['seed'][idx][imaxi]
        toAvg = np.abs(final['alost1'][idx])
        i = len(toAvg)
        mean = np.mean(toAvg)
        std = np.sqrt(np.mean(toAvg*toAvg)-mean**2)
        idxneg = (final['angle']==angle)&(final['alost1']<0)
        eqaper = np.where((final['alost2'] == final['Amin']))[0]
        nega = len(final['alost1'][idxneg])
        Amin = np.min(final['Amin'][idxangle])
        Amax = np.max(final['Amax'][idxangle])

        #for k in eqaper:
        #  msg="Angle %d, Seed %d: Dynamic Aperture below:  %.2f Sigma"
        #  print msg %( final['angle'][k],final['seed'][k], final['Amin'][k])

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
        name2 = "DAres.%s.%s.%s"%(self.LHCDescrip,sixdesktunes,turnse)
        if nostd:
          fhplot.write('%s %d %.2f %.2f %.2f %d %.2f %.2f\n'%(name2, fn, mini, mean, maxi, nega, Amin, Amax))
        else:
          fhplot.write('%s %d %.2f %.2f %.2f %d %.2f %.2f %.2f\n'%(name2, fn, mini, mean, maxi, nega, Amin, Amax, std))
    fhplot.close()
    print fnplot

# -------------------------------- da_vs_turns -----------------------------------------------------------
  def st_da_vst(self,data):
    ''' store da vs turns data '''
    cols  = SQLTable.cols_from_dtype(data.dtype)
    tab   = SQLTable(self.conn,'da_vsturn',cols,tables.Da_Vst.key)
    tab.insert(data)
  def get_da_vst(self,seed,tune):
    '''get da vs turns data from DB'''
    #change for new db version
    (tunex,tuney)=tune
    #check if table da_vsturn exists
    #yes -> return table
    #no  -> return 0 len table = []
    cmd="""SELECT name FROM sqlite_master
        WHERE type='table'
        ORDER BY name"""
    cur=self.conn.cursor().execute(cmd)
    ftype=[('name',np.str_,16)]
    tabnames=np.fromiter(cur,dtype=ftype)
    if 'da_vsturn' in tabnames['name']:
      ftype=[('seed',int),('tunex',float),('tuney',float),('dawavg',float),('dasavg',float),('dawsimp',float),('dassimp',float),('dawavgerr',float),('dasavgerr',float),('dasavgerrep',float),('dasavgerrepang',float),('dasavgerrepamp',float),('dawsimperr',float),('dassimperr',float),('nturn',float),('tlossmin',float),('mtime',float)]
      cmd="""SELECT *
           FROM da_vsturn WHERE seed=%s and tunex=%s and tuney=%s
           ORDER BY nturn"""
      cur=self.conn.cursor().execute(cmd%(seed,tunex,tuney))
      data=np.fromiter(cur,dtype=ftype)
    else:
      data=[]
    return data
  def mk_da_vst_ang(self,seed,tune,turnstep):
    """Da vs turns -- calculate da vs turns for divisors of angmax, 
    e.g. for angmax=29+1 for divisors [1, 2, 3, 5, 6, 10]"""
    RunDaVsTurnsAng(self,seed,tune,turnstep)
  def get_surv(self,seed,tune):
    '''get survival turns from DB calculated from emitI and emitII'''
    #change for new db version
    (tunex,tuney)=tune
    emit=float(self.env_var['emit'])
    gamma=float(self.env_var['gamma'])
    cmd="""SELECT angle,emitx+emity,
         CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
         FROM results WHERE seed=%s
         ORDER BY angle,emitx+emity"""
    cur=self.conn.cursor().execute(cmd%(seed))
    ftype=[('angle',float),('sigma',float),('sturn',float)]
    data=np.fromiter(cur,dtype=ftype)
    data['sigma']=np.sqrt(data['sigma']/(emit/gamma))
    angles=len(set(data['angle']))
    return data.reshape(angles,-1)

  def plot_da_vst(self,seed,tune,ampmin,ampmax,tmax,slog):
    """dynamic aperture vs number of turns, blue=simple average, red=weighted average"""
    data=self.get_da_vst(seed,tune)
    pl.close('all')
    pl.figure(figsize=(6,6))
    pl.errorbar(data['dasavg'],data['tlossmin'],xerr=data['dasavgerrep'],fmt='bo',markersize=2,label='simple average')
    pl.plot(data['dawavg'],data['tlossmin'],'ro',markersize=3,label='weighted average')
    pl.title('seed '+str(seed))
    pl.xlim([ampmin,ampmax])
    pl.xlabel(r'Dynamic aperture [$\sigma$]',labelpad=10,fontsize=12)
    pl.ylabel(r'Number of turns',labelpad=15,fontsize=12)
    plleg=pl.gca().legend(loc='best')
    for label in plleg.get_texts():
        label.set_fontsize(12)
    if(slog):
      pl.ylim([5.e3,float(tmax)])
      pl.yscale('log')
    else:
      pl.ylim([0,float(tmax)])
      pl.gca().ticklabel_format(style='sci', axis='y', scilimits=(0,0))
  def plot_surv_2d(self,seed,tune,ampmax=14):
    '''survival plot, blue=all particles, red=stable particles'''
    data=self.get_surv(seed,tune)
    s,a,t=data['sigma'],data['angle'],data['sturn']
    s,a,t=s[s>0],a[s>0],t[s>0]#delete 0 values
    tmax=np.max(t)
    sx=s*np.cos(a*np.pi/180)
    sy=s*np.sin(a*np.pi/180)
    sxstab=s[t==tmax]*np.cos(a[t==tmax]*np.pi/180)
    systab=s[t==tmax]*np.sin(a[t==tmax]*np.pi/180)
    pl.figure(figsize=(6,6))
    pl.scatter(sx,sy,20*t/tmax,marker='o',color='b',edgecolor='none')
    pl.scatter(sxstab,systab,4,marker='o',color='r',edgecolor='none')
    pl.title('seed '+str(seed),fontsize=12)
    pl.xlim([0,ampmax])
    pl.ylim([0,ampmax])
    pl.xlabel(r'Horizontal amplitude [$\sigma$]',labelpad=10,fontsize=12)
    pl.ylabel(r'Vertical amplitude [$\sigma$]',labelpad=10,fontsize=12)

