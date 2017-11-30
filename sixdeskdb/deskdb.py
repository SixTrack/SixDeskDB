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
from postProcessing import PostProcessing
import madout
from sqltable import SQLTable
import footprint

for t in (np.int8, np.int16, np.int32, np.int64,np.uint8, np.uint16, np.uint32, np.uint64):
  sqlite3.register_adapter(t, long)

def parse_env(studydir,logname=None):
  tmp="sh -c '%s . %s/sixdeskenv;. %s/sysenv; python -c %s'"
  if logname is not None:
    log='export LOGNAME="%s";'%logname
  else:
    log=""
  cmd=tmp%(log,studydir,studydir,'"import os;print os.environ"')
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

def mkrange(a,b,s):
   return map(float,np.round(np.arange(a,b*(1+1e-15),s),12))

def tune_dir(tune):
  """converts the list of tuples into the standard directory name, e.g. (62.31, 60.32) -> 62.31_60.32"""
  return str(tune[0])+'_'+str(tune[1])

def amp_dir(amps):
  """converts the list of tuples into the standard directory name, e.g. (2.0, 4.0) -> 2_4"""
  ampdirs=[]
  for aa in amps:
    ampdirs.append('%s_%s'%(int(aa[0]),int(aa[1])))
  return ampdirs

def ang_dir(angs):
  """converts the list of angle into the standard directory name, e.g. (85.5,87.0) -> ('85.5','87')"""
  angdirs=[]
  for aa in angs:
    if(aa%1<1.e-8):
      angdirs.append('%s'%(int(aa)))
    else:
      angdirs.append('%s'%(aa))
  return angdirs

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
  try:
    seed=int(seed)
    tunex,tuney=map(float,tunes.split('_'))
    amp1,amp2=map(float,rng.split('_'))
    angle=float(angle)
    #turns=10**int(turns[1])
  except ValueError as e:
      print("Error: path `%s` not compatible"%dirname)
      raise ValueError
  return seed,simul,tunex,tuney,amp1,amp2,turns,angle





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
  def from_dir(cls,studyDir,logname=None):
    '''create local Database for storing study'''
    sixdeskenv,sysenv=check_sixdeskenv(studyDir)
    env_var = parse_env(studyDir,logname=logname)
    dbname = env_var['LHCDescrip'] + ".db"
    db=cls(dbname,create=True)
    db.update_sixdeskenv(studyDir,logname=logname)
    db.st_mad6t_run()
    db.st_mad6t_run2()
    db.st_mad6t_results()
    db.st_six_beta()
    db.st_six_input()
    # db.st_six_results()
    return db

  def update_sixdeskenv(self,studyDir,logname=None):
    sixdeskenv,sysenv=check_sixdeskenv(studyDir)
    self.add_files([['sixdeskenv',sixdeskenv],['sysenv',sysenv]])
    env_var = parse_env(studyDir,logname=logname)
    for key in env_var.keys():
      if key not in tables.acc_var:
        del env_var[key]
    mtime=time.time()
    self.set_variables(env_var.items(),mtime)

  def update_from_dir_all(self):
    self.st_mad6t_run()
    self.st_mad6t_run2()
    self.st_mad6t_results()
    self.st_six_beta()
    self.st_six_input()

  def vars_replace_all(self,old,new):
    out=[]
    for k,v in self.env_var.items():
      if type(v) is str and old in v:
        print k,v
        out.append( (k,v.replace(old,new))  )
    mtime=time.time()
    self.set_variables(out,mtime)

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
  def __init__(self,dbname,create=False,debug=False):
    '''initialise variables and location for study creation 
        or database creation, usage listed in main.py'''
    self.debug=debug
    if not dbname.endswith('.db'):
        dbname+='.db'
    if create is False and not os.path.exists(dbname):
        raise ValueError,"File %s not found"%dbname
    try:
      conn = sqlite3.connect(dbname,isolation_level="IMMEDIATE")
      cur = conn.cursor()
      conn.create_function('',1,np.sqrt)
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
    self.logname=self.env_var.get('LOGNAME')

  def print_table_info(self):
      out=self.execute("SELECT name FROM sqlite_master WHERE type='table';")
      tables=[str(i[0]) for i in out]
      for tab in tables:
          rows=self.execute("SELECT count(*) FROM %s"%tab)[0][0]
          columms=[i[1] for i in self.execute("PRAGMA table_info(%s)"%tab)]
          print "%s(%d):\n  %s"%(tab,rows,', '.join(columms))

  def get_mad_runs(self):
      return [i[0] for i in self.execute('SELECT DISTINCT run_id FROM mad6t_run')]

  def get_mad_out(self,seed):
      sql="SELECT mad_out,run_id,mad_out_mtime FROM mad6t_run WHERE seed=%d ORDER BY mad_out_mtime "%seed
      data=self.execute(sql)
      data=[(decompressBuf(mad),ii,mt) for mad,ii,mt in data]
      return data
  def get_mad_in(self,seed):
      sql="SELECT mad_in,run_id FROM mad6t_run WHERE seed=%d ORDER BY mad_out_mtime "%seed
      data=self.execute(sql)
      data=[(decompressBuf(mad),ii) for mad,ii in data]
      return data
  def extract_mad_out(self,seed):
      data=self.get_mad_out(seed)
      out=[]
      for mad,ii,mt in data:
          out.append(madout.extract_mad_out(StringIO(mad)))
      return out
  def mad_out(self):
      mad_runs=self.execute('SELECT DISTINCT run_id FROM mad6t_run')
      if len(mad_runs)==0:
          print "No mad out data"
      for run, in mad_runs:
          print "Checking %s"%run
          sql="SELECT seed,mad_out FROM mad6t_run WHERE run_id=='%s' ORDER BY seed"%run
          data=self.execute(sql)
          data=[(seed,decompressBuf(out)) for seed,out in data]
          resname=os.path.join(self.mk_analysis_dir(),run)+'.csv'
          madout.check_mad_out(data,resname)
          print resname

  def set_env(self,**args):
      mtime=time.time()
      self.set_variables(args.items(),mtime)

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
  def get_result_colums(self):
    cols=zip(*self.execute('pragma table_info(results)'))[1]
    return [l.split(':')[0] for l in cols]

  def info(self):
    ''' provide info of study'''
    var = [['LHCDescrip'], ['platform', 'madlsfq', 'lsfq'],
           ['runtype', 'e0', 'gamma'], ['beam', 'dpini',],
           ['istamad', 'iendmad'],
           ['ns1l', 'ns2l', 'nsincl', 'sixdeskpairs'],
           ['tunex','tunex1','deltax'],
           ['tuney','tuney1','deltay'],
           ['turnsl', 'turnsle', 'writebinl',],
           ['kstep', 'kendl', 'kmaxl',],
           ['sixdesktrack'], ['sixtrack_input']]
    env_var = self.env_var
    for vl in var:
      for keys in vl:
         val=env_var[keys]
         print '%-15s'%('%s=%s;'%(keys,repr(val))),
      print

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
    dirNames =glob.glob(os.path.join(workdir,'mad.dorun_*'))
    dirNames+=glob.glob(os.path.join(workdir,'mad.mad6t*'))
    for dirName in dirNames:
        print 'found mad run',dirName.split('/')[-1]
        done=set()
        for filename in os.listdir(dirName):
          time = None
          fnroot,seed=os.path.splitext(filename)
          if seed[1:].isdigit():
              if not fnroot.endswith('.out'):
                seed=int(seed[1:])
                run_id = dirName.split('/')[-1]
                mad_in = sqlite3.Binary(compressBuf(os.path.join(dirName, filename)))
                out_file=os.path.join(dirName,fnroot+'.out.%d'%seed)
                log_file=os.path.join(dirName,fnroot+'_mad6t_%d.log'%seed)
                lsf_file=os.path.join(dirName,'mad6t_%d.lsf'%seed)
                mad_out = sqlite3.Binary(compressBuf(out_file))
                done.add(out_file)
                if os.path.isfile(log_file):
                  done.add(log_file)
                  mad_log = sqlite3.Binary(compressBuf(log_file))
                else:
                  mad_log = None
                if os.path.isfile(lsf_file):
                  done.add(lsf_file)
                  mad_lsf = sqlite3.Binary(compressBuf(lsf_file))
                else:
                  mad_lsf = None
                if os.path.isfile(out_file):
                  time = os.path.getmtime( out_file)
                data.append([run_id, seed, mad_in, mad_out, mad_lsf,mad_log,time])
          else:
            path=os.path.join(dirName, filename)
            if path not in done:
              key = path.replace(env_var['scratchdir']+'/','')
              path = os.path.join(dirName, filename)
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
              mtime=0
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
    print "Looking for betavalues, sixdesktunes, general_input in\n %s"%workdir
    gen_input=os.path.join(workdir,'general_input')
    if not os.path.exists(gen_input):
      print "Warning: %s not found"%gen_input
      gen=[0,0]
    else:
      #content = sqlite3.Binary(compressBuf(gen_input))
      gen=[float(i) for i in open(gen_input).read().split()]
    defaults={'betavalues':[0]*14,'sixdesktunes':[0]*5}
    for dirName in glob.glob('%s/*/simul/*'%workdir):
      dirn = dirName.split('/')
      seed = int(dirn[-3])
      tunex, tuney = dirn[-1].split('_')
      vals=[seed,tunex,tuney]
      lastmtime=0
      for fn in ['betavalues','sixdesktunes']:
        fullname=os.path.join(dirName,fn)
        if os.path.isfile(fullname):
          mtime=os.path.getmtime(fullname)
          if mtime >lastmtime:
            lastmtime=mtime
          vals+=[float(i) for i in open(fullname).read().split()]
        else:
          if not (fn=='sixdesktunes' and self.env_var['chrom']==1):
            print("'%s' not found, filling data with zeros"%fullname)
          vals+=defaults[fn]
      vals.extend(gen)
      vals.append(mtime)
      data.append(vals)
    print " number of sixdesktunes, betavalues inserted: %d"%len(data)
    tab.insertl(data)

#  def insert_fort3(self,data):
#    """insert fort.3
#       jobparams, mtime, fh
#       jobparams=seed,simul,tunex,tuney,amp1,amp2,turns,angle
##    """
#    conn = self.conn
#    cur = conn.cursor()
#    env_var = self.env_var
#    cols = SQLTable.cols_from_fields(tables.Six_In.fields)
#    tab = SQLTable(conn,'six_input',cols,tables.Six_In.key)
#    f3_data={}
#    for row in cur.execute('SELECT id,mtime,seed,simul,tunex,tuney,amp1,amp2,turns,angle FROM six_input'):
#        f3_data[tuple(row[2:])]=row[:2]
#    count3 = 0
#    six_id_new = list(cur.execute('SELECT max(id) FROM six_input'))[0][0]
#    if six_id_new is None:
#      six_id_new=1
#    else:
#      six_id_new+=1
#    for jobparams, mtime, fh in data:
#       six_id_old,mtime_old=f3_data.get(jobparmams,[-1,0])
#       if six_id_old==-1:
#          six_id=six_id_new
#          six_id_new+=1
#       else:
#         six_id=six_id_old
#       if mtime3>mtime3_old:
#        count3+=1
#        f3file=sqlite3.Binary(compressBuf(f3))
#        dirn = [six_id] + list(jobparmams) + [f3file,mtime3]
#        rows3.append(dirn)
#        if len(rows3) == 6000:
#          tab.insertl(rows3)
#          tab1.insertl(rows10)
#          rows3 = []
#          rows10 = []
#       tab.insertl(rows3)
#       tab1.insertl(rows10)
#       rows3 = []
#    print '\n number of fort.3 updated/found: %d/%d'%(count3,file_count3)
#    sql="""CREATE VIEW IF NOT EXISTS results
#           AS SELECT * FROM six_input INNER JOIN six_results
#           ON six_input.id==six_results.six_input_id"""
#    cur.execute(sql)
#    sql="""SELECT COUNT(DISTINCT six_input_id) FROM six_results"""
#    jobs=list(cur.execute(sql))[0][0]
#    print " db now contains %d jobs"% (jobs)

  def st_six_input(self):
    ''' store input values (seed,tunes,amps,etc) along with fort.3 file'''
    conn = self.conn
    cur = conn.cursor()
    env_var = self.env_var
    cols3 = SQLTable.cols_from_fields(tables.Six_In.fields)
    tab3 = SQLTable(conn,'six_input',cols3,tables.Six_In.key)
    cols10 = SQLTable.cols_from_fields(tables.Six_Res.fields)
    tab10 = SQLTable(conn,'six_results',cols10,tables.Six_Res.key)
    colsfma = SQLTable.cols_from_fields(tables.Fma.fields)
    tabfma = SQLTable(conn,'six_fma',colsfma,tables.Fma.key)
    f3_data={}
    for row in cur.execute('SELECT id,mtime,seed,simul,tunex,tuney,amp1,amp2,turns,angle FROM six_input'):
        f3_data[tuple(row[2:])]=row[:2]
    f10_data={}
    for row in cur.execute('SELECT DISTINCT six_input_id,mtime FROM six_results'):
        f10_data[row[0]]=row[1]
    fma_data={}
    for row in cur.execute('SELECT DISTINCT six_input_id,mtime_fma FROM six_fma'):
        fma_data[row[0]]=row[1]
    count3   = 0
    count10  = 0
    countfma = 0
    workdir = os.path.join(env_var['sixdesktrack'],self.LHCDescrip)
    extra_files = []
    rows3   = []
    rows10  = []
    rowsfma = []
    six_id_new = list(cur.execute('SELECT max(id) FROM six_input'))[0][0]
    if six_id_new is None:
      six_id_new=1
    else:
      six_id_new+=1
    print "Looking for fort.3.gz, fort.10.gz, fma_sixtrack files in\n %s"%workdir
    file_count3   = 0
    file_count10  = 0
    file_countfma = 0
    for dirName in glob.iglob(os.path.join(workdir,'*','*','*','*','*','*')):
      f3=os.path.join(dirName, 'fort.3.gz')
      f10=os.path.join(dirName, 'fort.10.gz')
      ffma=os.path.join(dirName, 'fma_sixtrack.gz')
      ranges= dirName.split('/')[-3]
      if '_' in ranges:
        fn3_exists=fn10_exists=fnfma_exists=False
        if os.path.exists(f3):
            file_count3+=1
            fn3_exists=True
        if os.path.exists(f10):
            file_count10 += 1
            fn10_exists=True
        if os.path.exists(ffma):
            file_countfma += 1
            fnfma_exists=True
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
             try:
               for lines in gzip.open(f10,"r"):
                line=[six_id,countl]+lines.split()+[mtime10]
                if len(line)!=63:
                    print(line)
                    print("Error in %s"%f10)
                    print("%d columns found expected 60"%len(line) )
                    raise Exception
                rows10.append(line)
                countl += 1
             except :
                print "Error in opening: ",f10
                raise Exception
             count10 += 1
        if fnfma_exists:
           mtimefma = os.path.getmtime(ffma)
           mtimefma_old=fma_data.get(six_id,0)
           if mtimefma > mtimefma_old and os.path.getsize(ffma)>0:
             countl = 1
             for lines in gzip.open(ffma,"r"):
                if (lines.rfind('#') < 0):#skip header
                  rowsfma.append([six_id,countl]+lines.split()+[mtimefma])
                  countl += 1
             countfma += 1
        if len(rows3) == 6000:
          tab3.insertl(rows3)
          tab10.insertl(rows10)
          tabfma.insertl(rowsfma)
          rows3 = []
          rows10 = []
          rowsfma = []
    tab3.insertl(rows3)
    tab10.insertl(rows10)
    tabfma.insertl(rowsfma)
    rows3 = []
    print '\n number of fort.3 updated/found: %d/%d'%(count3,file_count3)
    print ' number of fort.10 updated/found: %d/%d'%(count10,file_count10)
    print ' number of fma_sixtrack updated/found: %d/%d'%(countfma,file_countfma)
# create view of six_results and six_fma
    for six_tab in ['results','fma']:
      sql=("""CREATE VIEW IF NOT EXISTS %s
              AS SELECT * FROM six_input INNER JOIN six_%s
              ON six_input.id==six_%s.six_input_id""")%((six_tab,)*3)
      cur.execute(sql)
      sql="""SELECT count(*) FROM %s"""%(six_tab)
      results=list(cur.execute(sql))[0][0]
      sql=("""SELECT COUNT(DISTINCT six_input_id) FROM six_%s""")%(six_tab)
#      sql="""SELECT COUNT(*) FROM six_%s"""%(six_tab)
      jobs=list(cur.execute(sql))[0][0]
      print " db now contains %d %s from %d jobs"% (results,six_tab,jobs)

  def execute(self,sql):
    cur= self.conn.cursor()
    cur.execute(sql)
    self.conn.commit()
    return list(cur)

  def st_boinc_results(self):
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
    workdir = env_var['sixdeskboincdir']
    extra_files = []
    rows3 = []
    rows10 = []
    print "Looking for fort.10 files in\n %s"%workdir
    file_count10 = 0
    for dirName in glob.iglob(os.path.join(workdir,'*')):
      f10=os.path.join(dirName)
      file_count10 += 1
      if file_count3%100==0:
         sys.stdout.write('.')
         sys.stdout.flush()
      jobparmams=split_job_params(dirName.split('__','/')) # to change for boinc TO CLEAN
      six_id_old,mtime3_old=f3_data.get(jobparmams,[-1,0])
      mtime10 = os.path.getmtime(f10)
      mtime10_old=f10_data.get(six_id,0)
      if mtime10 > mtime10_old and os.path.getsize(f10)>0:
         countl = 1
         for lines in file.open(f10,"r"):
           rows10.append([six_id,countl]+lines.split()+[mtime10])
           countl += 1
         count10 += 1
         if count10 == 1000:
           tab1.insertl(rows10)
    tab1.insertl(rows10)
    print ' number of fort.10 updated/found: %d/%d'%(count10,file_count10)
    sql="""CREATE VIEW IF NOT EXISTS results
           AS SELECT * FROM six_input INNER JOIN six_results
           ON six_input.id==six_results.six_input_id"""
    cur.execute(sql)
    sql="""SELECT count(*) FROM results"""
    results=list(cur.execute(sql))[0][0]
    sql="""SELECT COUNT(DISTINCT six_input_id) FROM six_results"""
    jobs=list(cur.execute(sql))[0][0]
    print " db now contains %d results from %d jobs"% (results,jobs)

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
    sql = """select seed,tunex,tuney,amp1,amp2,turns,angle from six_input
    where not exists(select 1 from six_results where id=six_input_id)"""
    a = self.execute(sql)
    return a

  def get_incomplete_fort10(self):
    '''get input values for which fort.10 is incomplete '''
    # conn = self.conn
    # cur = conn.cursor()
    # env_var = self.env_var
    # newid = self.newid
    pairs=self.env_var['sixdeskpairs']
    sql = """select seed,tunex,tuney,amp1,amp2,turns,angle from six_input
    where not exists(select 1 from six_results where id=six_input_id and 
    row_num=%d)"""%(pairs)
    a = self.execute(sql)
    return a
  def make_job_trackdir(self,seed,simul,tunes,amp1,amp2,turnse,angle):
    sixdesktrack=self.env_var['sixdesktrack']
    base=os.path.join(sixdesktrack,self.LHCDescrip)
    t='%s/%d/%s/%s/%s/e%s/%g/'
    rng="%g_%g"%(amp1,amp2)
    tunes="%g_%g"%(tunes[0],tunes[1])
    return t%(base,seed,simul,tunes,rng,turnse,angle)

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
    pairs=self.env_var('sixdeskpairs')
    return self.execute('SELECT count(*) FROM six_results')[0][0]/pairs

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
    sql='SELECT DISTINCT seed FROM six_input ORDER BY seed'
    out=zip(*self.execute(sql))[0]
    return out

  def get_db_amplitudes(self):
    ''' get seeds from DB'''
    sql='SELECT DISTINCT amp1,amp2 FROM six_input ORDER BY seed'
    out=zip(*self.execute(sql))[0]
    return out

  def get_db_tunes(self):
    ''' get tunes from DB'''
    sql='SELECT DISTINCT tunex,tuney FROM six_input ORDER BY tunex,tuney'
    out=self.execute(sql)
    return out

  def check_seeds(self):
    """check if seeds defined in the environment are presently available in the database"""
    return not len(set(self.get_seeds())-set(self.get_db_seeds()))>0

  def check_angles(self):
    """check if angles defined in the environment are presently available in the database"""
    return not len(set(self.get_angles())-set(self.get_db_angles()))>0

  def check_table(self,tab):
    """check if table tab exists in database"""
    cmd="""SELECT name FROM sqlite_master
        WHERE type='table'
        ORDER BY name"""
    cur=self.conn.cursor().execute(cmd)
    ftype=[('name',np.str_,30)]
    tabnames=np.fromiter(cur,dtype=ftype)
    return (tab in tabnames['name'])

  def check_view(self,tab):
    """check if view tab exists in database"""
    cmd="""SELECT name FROM sqlite_master
        WHERE type='view'
        ORDER BY name"""
    cur=self.conn.cursor().execute(cmd)
    ftype=[('name',np.str_,30)]
    tabnames=np.fromiter(cur,dtype=ftype)
    return (tab in tabnames['name'])

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
    out=zip(*self.execute('SELECT DISTINCT angle FROM six_input ORDER by ANGLE'))[0]
    return out

  def get_amplitudes(self):
    '''get_amplitudes from env variables '''
    env_var = self.env_var
    nsincl = float(env_var['nsincl'])
    ns1l = float(env_var['ns1l'])
    ns2l = float(env_var['ns2l'])
    return [(float(a),float(np.round(a+nsincl,12)))
            for a in mkrange(ns1l,ns2l,nsincl)[:-1]]

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
    env_var = self.env_var
    tunex = float(env_var['tunex'])
    tuney = float(env_var['tuney'])
    tunex1 = float(env_var['tunex1'])
    tuney1 = float(env_var['tuney1'])
    deltax = float(env_var['deltax'])
    deltay = float(env_var['deltay'])
    out=[(tunex,tuney)]
    while  tunex<=tunex1 and tuney<=tuney1:
        if len(out)>1000:
            raise ValueError("Too many tunes to generate")
        if tunex<tunex1:
          tunex=round(tunex+deltax,12)
        if tuney<tuney1:
          tuney=round(tuney+deltay,12)
        out.append((tunex,tuney))
        if tunex==tunex1 and tuney==tuney1:
            break
    return out

  def get_anbn_fort16(self):
    '''returns a dictionary of the multipolar errors asigned to each element
    based on fort.16. E.g to get the 'a1' errors of element 'mb.a8r3.b1..1' 
    in seed '1': dict[(1,'mb.a8r3.b1..1')]['a1']
    As 'mb.a8r3.b1..1' can occur multiple times, dict[(1,'mb.a8r3.b1..1')]['a1']
    returns a list of the values of all occurances.'''
    lst=self.execute('SELECT seed,fort16 FROM mad6t_results ORDER by seed')
    data,name={},''
    anbn=['b'+str(n+1) for n in range(20)]+['a'+str(n+1) for n in range(20)]
    for seed,fb16 in lst:
      f16=gzip.GzipFile(fileobj=StringIO(fb16))
      for line in f16:
        ll=line.split()
        if len(ll)==1:#condition for line=name
          name=ll[0]
          if name!='':
            canbn=0#counts the lines after each name
          if (seed,name) not in data: data[(seed,name)]={}
        if len(ll)>1:
            for ab in ll:
              if anbn[canbn] in data[(seed,name)]:
                data[(seed,name)][anbn[canbn]].append(float(ab))
              else:
                data[(seed,name)][anbn[canbn]]=[float(ab)]
              canbn=canbn+1
    return data

  def get_anbn_fort3mad(self):
    '''returns a dictionary of which multipolar errors are turned on
       for each element based on fort.3.mad. E.g to get the 'a1' 
       errors of element 'mb.a8r3.b1..1': dict['mb.a8r3.b1']['a1']'''
    cmd="""SELECT path,content FROM files
            ORDER BY path"""
    for fn,fb in self.execute(cmd):
      if('fort.3.mad' in fn): fn3,fb3=fn,fb
    name,data='',{}
    anbn=[]
    for n in range(20): anbn.extend(['b'+str(n+1)+'rms','b'+str(n+1),'a'+str(n+1)+'rms','a'+str(n+1)])
    f3=gzip.GzipFile(fileobj=StringIO(fb3))
    for line in f3:
      ll=line.split()
      if len(ll)==3:#condition for line=name
        name=ll[0]
        if name!='':
          canbn=0#counts the lines after each name
        if name not in data: data[name]={}
      if len(ll)==4:
          for ab in ll:
            if anbn[canbn] in data[name]:
              data[name][anbn[canbn]].append(float(ab))
            else:
              data[name][anbn[canbn]]=[float(ab)]
            canbn=canbn+1
    return data

  def gen_job_params(self):
    '''generate jobparams based on values '''
    if self.env_var['long']==1:
      simul='simul'
      turns = 'e'+str(self.env_var['turnsle'])
      for seed in self.get_seeds():
        for tunex,tuney in self.get_tunes():
          for amp1,amp2 in self.get_amplitudes():
            for angle in self.get_angles():
              yield (seed,simul,tunex,tuney,amp1,amp2,turns,angle)
    if self.env_var['short']==1:
      simul='short'
      turns = 'e'+str(self.env_var['turnsse'])
      for seed in self.get_seeds():
        for tunex,tuney in self.get_tunes():
          for amp1,amp2 in self.get_amplitudes():
            for angle in self.get_angles():
              yield (seed,simul,tunex,tuney,amp1,amp2,turns,angle)


  def get_missing_jobs(self):
    '''get missing jobs '''
    existing=set(self.get_existing_results())
    needed=set(self.gen_job_params())
    #print sorted(existing)[0]
    #print sorted(needed)[0]
    return list(needed-existing)


  def get_existing_input(self):
    turnsl = '%E'%(float(self.env_var['turnsl']))
    turnsl = 'e'+str(int(turnsl.split('+')[1]))
    existing = self.execute("""SELECT seed,simul,tunex,tuney,amp1,amp2,
                               turns,angle from six_input""")
    return existing

  def get_existing_results(self):
    turnsl = '%E'%(float(self.env_var['turnsl']))
    turnsl = 'e'+str(int(turnsl.split('+')[1]))
    existing = self.execute("""SELECT seed,simul,tunex,tuney,amp1,amp2,
                               turns,angle from results where row_num=1""")
    return existing

  def get_running_jobs(self,missing,threshold=7*24*3600):
    running=set()
    try:
        bjobs=lsfqueue.parse_bjobs()
    except IOError:
        print "Cannot check running jobs"
        return running
    for lsfjob in bjobs:
        if lsfjob in missing:
          job=self.running[lsfjob]
          if job.stat in ('PEND','RUN'):
            tmp="TrackOut job: %s %s %s %s %s"
            print tmp%(job.jobid,jobshort,job.submit_time,job.start_time,job.stat)
            if not (job.stat=='RUN' and job.run_since()>threshold):
               running.add(lsfjob)
    return running

  def make_lsf_missing_jobs(self):
    tmp="%s%%%s%%s%%%s%%%s%%%s%%%s"
    missing=set()
    name=self.LHCDescrip
    for job in self.get_missing_jobs():
       seed,simul,tunex,tuney,amp1,amp2,turns,angle=job
       ranges="%s_%s"%(amp1,amp2)
       tunes="%s_%s"%(tunex,tuney)
       missing.add(tmp%(name,seed,tunes,ranges,turns,angle))
    if len(missing)==0:
        print "No missing jobs"
    else:
       running=self.get_running_jobs(missing)
       if len(running)>0:
          print "%d job running"
       rerun=missing-running
       workdir=os.path.join(self.env_var['sixdeskwork'],'lsfjobs')
       if not os.path.isdir(workdir):
           os.mkdir(workdir)
       fn=os.path.join(workdir,'missing_jobs')
       open(fn,'w').write('\n'.join(rerun))
       print "Writing %s"%fn
       if len(rerun)>0:
           print 'To launch jobs do:'
           home=self.env_var['sixdeskhome']
           print 'cd %s;set_env %s; run_missing_jobs' %(home,name)

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
    cmd="""SELECT angle,sigx1*sigx1+sigy1*sigy1,
            %s
            FROM results
            WHERE seed=%s ORDER BY angle,sigx1*sigx1+sigy1*sigy1"""
    cur=self.conn.cursor().execute(cmd%(col,seed))
    ftype=[('angle',float),('ampsq',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    angles=len(set(data['angle']))
    data=data.reshape(angles,-1)
    a,s,t=data['angle'],np.sqrt(data['ampsq']),data['surv']
    return a,s,t
  def get_3d_col(self,col,cond=''):
    cmd="""SELECT seed,angle,sigx1*sigx1+sigy1*sigy1,
            %s
            FROM results %s ORDER BY seed,angle,sigx1*sigx1+sigy1*sigy1"""
    cur=self.conn.cursor().execute(cmd%(col,cond))
    ftype=[('seed',float),('angle',float),('ampsq',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    angles=len(set(data['angle']))
    seeds=len(set(data['seed']))
    data=data.reshape(seeds,angles,-1)
    ss,a,s,t=data['seed'],data['angle'],np.sqrt(data['ampsq']),data['surv']
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
  def get_col(self,col,seed,angle,tunes=None):
    """ Get the column from table results specified by the seed and the angle
        as a function of the amplitude of the track particles in sigma

        Example:
            db.get_col('sturns1',1,45)
    """
    cmd="""SELECT sigx1*sigx1+sigy1*sigy1, %s FROM results
            WHERE tunex=%s AND tuney=%s AND seed=%s AND angle=%s
            ORDER BY amp1,row_num"""
    if tunes is None:
        tunes=self.get_tunes()[0]
    cur=self.conn.cursor().execute(cmd%(col,tunes[0],tunes[1],seed,angle))
    ftype=[('ampsq',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    a,t=np.sqrt(data['ampsq']),data['surv']
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
    cmd="""SELECT angle,(sigx1*sigx1+sigy1*sigy1),
        CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
        FROM six_results,six_input WHERE seed=%s AND id=six_input_id
        ORDER BY angle,sigx1*sigx1+sigy1*sigy1"""
    # cmd="""SELECT angle,(sigx1*2+sigy1*2),
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
    cmd=""" SELECT seed,angle,sigx1*sigx1+sigy1*sigy1,
        CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
        FROM six_results,six_input WHERE id=six_input_id
        ORDER BY angle,sigx1*sigx1+sigy1*sigy1"""
    cur=self.conn.cursor().execute(cmd)
    ftype=[('seed',float),('angle',float),('ampsq',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    angles=len(set(data['angle']))
    seeds=len(set(data['seed']))
    data=data.reshape(seeds,angles,-1)
    a,s,t=data['angle'],np.sqrt(data['ampsq']),data['surv']
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
    pl.scatter(x,y,c=t)
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
  def has_table(self,name):
    sql="SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'"
    return self.execute(sql%name)
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
             ('row_num','int'),
             ('mtime','float')]
    names=','.join(zip(*rectype)[0])
    turnsl=self.env_var['turnsl']
    turnse=self.env_var['turnse']
    ns1l=self.env_var['ns1l']
    ns2l=self.env_var['ns2l']
    Elhc,Einj = self.execute('SELECT emitn,gamma from six_beta LIMIT 1')[0]
    anumber=1
    angles=self.get_db_angles()
    seeds=self.get_db_seeds()
    pairs=self.env_var['sixdeskpairs']
    mtime=self.execute('SELECT max(mtime) from results')[0][0]
    final=[]
    sql1='SELECT %s FROM results WHERE betx>0 AND bety>0 AND emitx>1e-10 AND emity>1e-10 AND turn_max=%d AND amp1>=%s AND  amp1<=%s'%(names,turnsl,ns1l,ns2l)
    LHCDescrip=self.LHCDescrip
    for tunex,tuney in self.get_db_tunes():
        sixdesktunes="%s_%s"%(tunex,tuney)
        sql1+=' AND tunex=%s AND tuney=%s '%(tunex,tuney)
        for angle in angles:
            fndot='DAres.%s.%s.%s.%d'%(LHCDescrip,sixdesktunes,turnse,anumber)
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
                achaos = 0.
                achaos1 = 0.
                sql=sql1+' AND seed=%s '%seed
                #sql+=' AND ROUND(angle,5)=ROUND(%s,5) '%angle
                sql+=' AND angle=%s '%angle
                sql+=' ORDER BY sigx1*sigx1+sigy1*sigy1 '
                if self.debug:
                    print sql
                inp=np.array(self.execute(sql),dtype=rectype)
                if len(inp)==0:
                    msg="Warning: all particle lost for angle %s and seed %s"
                    print msg%(angle,seed)
                    continue
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
                amp2=inp['amp2'][-1]
                row_num=inp['row_num'][-1]
                if row_num<pairs :
                    truncated=True
                else:
                    truncated=False
                zero = 1e-10
                #xidx=(betx>zero) & (emitx>zero)
                #yidx=(bety>zero) & (emity>zero)
                #sigx1[xidx]=np.sqrt(betx[xidx]*emitx[xidx])
                #sigy1[yidx]=np.sqrt(bety[yidx]*emity[yidx])
                sigx1=np.sqrt(betx*emitx)
                sigy1=np.sqrt(bety*emity)
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
                    if abs(emitx[0])>=zero :
                        eex=emitx[0]
                        eey=emity[0]
                    else:
                        eey=sigy1[0]**2/bety[0]
                        eex=sigx1[0]**2/betx[0]
                    rad=np.sqrt(1+eey/eex)/sigma
                else:
                    rad=1
                if abs(sigxavgnld[0])>zero and abs(bety[0])>zero and sigma > 0:
                    if abs(emitx[0]) < zero:
                        eey=sigyavgnld[0]**2/bety[0]
                        eex=sigxavgnld[0]**2/betx[0]
                        rad1=np.sqrt(1+eey/eex)/sigma
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
                alost2test=np.where((sturns1<turn_max)|(sturns2<turn_max))[0]
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
                firstamp=rad*sigx1[0]
                lastamp=rad*sigx1[iel]
                if alost1==0. and alost2==0. and truncated:
                    alost1=rad*sigx1[-1]
                    alost2=rad*sigx1[-1]
                    fmt="Warning: tune %s_%s, angle %s, seed %d, range %s-%s: stepwise survival"
                    print fmt%(tunex,tuney,angle,seed,ns1l,ns2l)
                elif alost1==0. and alost2==0.:
                    alost1=0
                    alost2=0
                    fmt="Warning: tune %s_%s, angle %s, seed %d, range %s-%s: All particle survived, DA set to 0"
                    print fmt%(tunex,tuney,angle,seed,ns1l,ns2l)
                name2 = "DAres.%s.%s.%s"%(self.LHCDescrip,sixdesktunes,turnse)
                name1= '%s%ss%s%s-%s%s.%d'%(self.LHCDescrip,seed,sixdesktunes,ns1l, ns2l, turnse,anumber)
                if(seed<10):
                    name1+=" "
                if(anumber<10):
                    name1+=" "
                fmt=' %-39s  %10.6f  %10.6f  %10.6f  %10.6f  %10.6f  %10.6f\n'
                fhdot.write(fmt%( name1[:39],achaos,achaos1,alost1,alost2,firstamp,lastamp))
                final.append([name2, turnsl,tunex, tuney, int(seed),
                               angle,achaos,achaos1,alost1,alost2,
                               rad*sigx1[0],rad*sigx1[iel],mtime])
            anumber+=1
            fhdot.close()
            print fndot
    cols=SQLTable.cols_from_fields(tables.Da_Post.fields)
    datab=SQLTable(self.conn,'da_post',cols,tables.Da_Post.key,recreate=True)
    datab.insertl(final)

  def mk_da(self,force=False,nostd=False):
    """calculate DA for each seed and angle
    from fort.10 files
    """
    dirname=self.mk_analysis_dir()
    cols=SQLTable.cols_from_fields(tables.Da_Post.fields)
    datab=SQLTable(self.conn,'da_post',cols)
    turnsl=self.env_var['turnsl']
    turnse=self.env_var['turnse']
    for tunex,tuney in self.get_db_tunes():
        sixdesktunes="%s_%s"%(tunex,tuney)
        wh="turnsl=%s AND tunex=%s AND tuney=%s"%(turnsl,tunex,tuney)
        final=datab.select(where=wh,orderby='angle,seed')
        if len(final)>0:
            an_mtime=final['mtime'].min()
            res_mtime=self.execute('SELECT max(mtime) FROM six_results')[0][0]
            if res_mtime>an_mtime or force is True:
                self.read10b()
                #PostProcessing(self).readplotb()
                final=datab.select(where=wh,orderby='angle,seed')
        else:
          self.read10b()
          #PostProcessing(self).readplotb()
          final=datab.select(where=wh,orderby='angle,seed')
          if len(final)==0:
              print "Error: No data available for analysis for `%s`"%wh
              continue

        ns1l=self.env_var['ns1l']
        ns2l=self.env_var['ns2l']
        #print final['mtime']
        #print self.execute('SELECT max(mtime) FROM six_results')[0][0]

        fnplot='DAres.%s.%s.%s.plot'%(self.LHCDescrip,sixdesktunes,turnse)
        fnplot= os.path.join(dirname,fnplot)
        fhplot= open(fnplot, 'w')
        fnsumm='DAres.%s.%s.%s.summ'%(self.LHCDescrip,sixdesktunes,turnse)
        fnsumm= os.path.join(dirname,fnsumm)
        fhsumm= open(fnsumm, 'w')
        fn=0
        #for k in eqaper:
        #  msg="Angle %-4g, Seed %2d: Dynamic Aperture below:  %.2f Sigma"
        #  print msg %( final['angle'][k],final['seed'][k], final['Amin'][k])
        for angle in np.unique(final['angle']):
            fn+=1
            study= final['name'][0]
            idxangle=final['angle']==angle
            idx     =idxangle&(final['alost1']!=0)
            idxzero =idxangle&(final['alost1']==0)
            if all(idxzero==idxangle):
                print "No sufficient data to determine DA for angle %s"%angle
                continue
            for seed in final['seed'][idxzero]:
                fmt="Warning: tune %s_%s, angle %s, seed %d, range %s-%s: all stable particles"
                print fmt%(tunex,tuney,angle,seed,ns1l,ns2l)
            idxneg  =idxangle&(final['alost1']<0)
            finalalost=np.abs(final['alost1'][idx])
            imini=np.argmin(finalalost)
            mini=finalalost[imini]
            smini=final['seed'][idx][imini]
            imaxi=np.argmax(finalalost)
            maxi=finalalost[imaxi]
            eqaper = np.where((final['alost2'] == final['Amin']))[0]
            smaxi=final['seed'][idx][imaxi]
            toAvg = np.abs(final['alost1'][idx])
            i = len(toAvg)
            mean = np.mean(toAvg)
            std = np.sqrt(np.mean(toAvg*toAvg)-mean**2)
            idxneg = (final['angle']==angle)&(final['alost1']<0)
            nega = len(final['alost1'][idxneg])
            Amin = np.min(final['Amin'][idxangle])
            Amax = np.max(final['Amax'][idxangle])

            print "Angle:    %.2f"%angle
            if i == 0:
              print "Dynamic Aperture below:  %.2f Sigma"%Amax
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
              fmt1='%s %d %.2f %.2f %.2f %d %.2f %.2f\n'
              fhplot.write(fmt1%(name2,fn,mini,mean,maxi,nega,Amin,Amax))
            else:
              fmt2='%s %d %.2f %.2f %.2f %d %.2f %.2f %.2f\n'
              #angle_rad=angle/180*np.pi
              fhplot.write(fmt2%(name2, fn, mini, mean, maxi, nega,
                                 Amin, Amax, std))
            fmt3='%.2f %.2f %.2f %d %.2f %.2f %.2f %.2f %.2f %.2f %d %.2f\n'
            co=np.cos(angle/180*np.pi)
            si=np.sin(angle/180*np.pi)
            fhsumm.write(fmt3%(mini*co,mini*si,mini,smini,
                               mean*co,mean*si,mean,
                               maxi*co,maxi*si,maxi,smaxi,angle))
        fhplot.close()
        fhsumm.close()
        print fnplot

# -------------------------------- turn by turn data -----------------------------------------------------------
#  def download_tbt(self,seed=None):
#    '''routine that downloads all the data and saves it in the sqltable sixtrack_tbt'''
#    studio     = self.LHCDescrip
#    if(seeds==None):
#      seeds    = self.get_db_seeds()
#    if(type(seeds) is int):
#      seeds=[seeds]
#    ampls      = amp_dir(self.get_amplitudes())
#    angles     = ang_dir(self.get_db_angles())
#    tunes      = '%s_%s'%(self.env_var['tunex'],self.env_var['tuney'])
#    exp_turns  = self.env_var['turnse']
#    np         = 2*self.env_var['sixdeskpairs']
#    tbt_data=downloader(studio, seeds, ampls, angles, tunes, exp_turns,np,setenv=True)
#    dbname=create_db(tbt_data,studio,seedinit,seedend,nsi,nsf,angles)
#    print ('Turn by turn tracking data successfully stored in %s.db' %dbname)
# -------------------------------- fma -------------------------------------------------------------------
  def get_fma_methods(self):
    """returns list of all methods available for FMA analysis
    """
    return ['TUNENEWT1','TUNEABT','TUNEABT2','TUNENEWT','TUNEFIT','TUNEAPA','TUNEFFT','TUNEFFTI','TUNELASK']
  def get_db_fma_inputfile_method(self):
    """returns a list of inputfiles and methods used 
    in db for FMA analysis

    Returns:
    --------
    data: list
        with format [(inputfile1,tunemethod1),(inputfile2,tunemethod2),...]
    """
    if(self.check_table('six_fma') and self.check_table('six_input')):
      cmd="""SELECT inputfile,method
           FROM fma ORDER BY inputfile"""
      cur=self.conn.cursor().execute(cmd)
      return list(set(cur.fetchall()))
  def get_fma(self,seed,tune,turns,inputfile,method):
    """get data from fma_sixtrack. The data is stored
    in table six_fma and can be accesed via the view
    fma (view of six_fma and six_input)

    Parameters:
    ----------
    seed : seed, e.g. 1
    tune : optics tune, e.g. (62.28, 60.31)
    turns : name of directory for number of turns tracked, e.g. 'e4'
    inputfile: name of the inputfile used for the FMA analysis, e.g. IP3_DUMP_1
    method: method used to calculate the tunes, e.g. TUNELASK

    Returns:
    --------
    data: ndarray (structured) with 
        id,seed,simul,tunex,tuney,amp1,amp2,turns,angle,fort3,mtime,six_input_id,
        row_num from six_input table
        and from six_fma table:
        inputfile: inputfile name e.g. IP3_DUMP_1
        method: method used for tune calculation e.g. TUNELASK
        ,part_id,q1,q2,q3
    """
    (tunex,tuney)=tune
    if(self.check_view('fma')):
      ftype=np.dtype([('id',int),('seed',int),('simul','|S100'),('tunex',float),('tuney',float),('amp1',float),('amp2',float),('turns','|S100'),('angle',float),('fort3','V'),('mtime',float),('six_input_id',int), ('row_num',int), ('inputfile' ,'|S100'), ('method', '|S100'), ('part_id', int), ('q1', float), ('q2', float), ('q3', float), ('eps1_min', float), ('eps2_min', float), ('eps3_min', float), ('eps1_max', float), ('eps2_max', float), ('eps3_max', float), ('eps1_avg', float), ('eps2_avg', float), ('eps3_avg', float), ('eps1_0', float), ('eps2_0', float), ('eps3_0', float), ('phi1_0', float), ('phi2_0', float), ('phi3_0', float), ('mtime_fma',float)])
      cmd="""SELECT *
           FROM fma WHERE seed=%s AND tunex=%s AND tuney=%s AND turns='%s'
           AND inputfile='%s' AND method='%s' ORDER BY inputfile,method"""
      cur=self.conn.cursor().execute(cmd%(seed,tunex,tuney,turns,inputfile,method))
      data=np.fromiter(cur,dtype=ftype)
    else:
      data=[]
    return data
  def get_fma_intersept(self,seed,tune,turns,files,var=None):
    """get data with seed *seed*, tune *tune*,turns *turns*
    which exists in (inputfile1,method1),(inputfile2,method2),etc.
    for each amp1,amp2,angle,part_id

    Parameters:
    ----------
    seed : seed, e.g. 1
    tune : optics tune, e.g. (62.28, 60.31)
    turns : name of directory for number of turns tracked, e.g. 'e4'
    files: list of inputfiles and methods with
           files=[(inputfile_0,method_0),...,(inputfile_n,method_n)]
           e.g. to compare only two files
           files=[('IP3_DUMP_1','TUNELASK'),('IP3_DUMP_2','TUNELASK')]
    method: method used to calculate the tunes, e.g. TUNELASK
    var: list of variables to be extracted, e.g. for fma analysis
         ['q1','q2','q3','eps1_0','eps2_0','eps3_0']
         if var= None, all variables are extracted.
    Returns:
    --------
    data: ndarray (structured) with 
        seed,tunex,tuney,fma0_amp1,fma1_amp1,fma0_amp2,fma1_amp2,...
        where seed,tunex,tuney are identical for 
        (inputfile_0,method_0) to (inputfile_n,method_n)
    """
# to be changed, q1[(amp1,amp2)]=[0.32,0.33, ...]
# q1={}
# q1.setdefault((a1,a2),[]).append(...)
    if len(files) < 2:
      raise Exception("ERROR in get_fma_intersept: you need to define at least 2 (inputfile,method) pairs to calcute the difference in tune!!!")
    (tunex,tuney)=tune
    nfma = len(files) # number of (inputfile,method)
    if(self.check_view('fma')):
# create n tables: fma0: inputfile1,method1, fma1: inputfile2,method2
# then select the common rows by requesting that amp1,angle,part_id match
# other option to use inner join
      for i in range(nfma):
        t='fma%s'%i
#       delete tables fma0,...,fman if they exist
        cmd="""DROP TABLE IF EXISTS %s"""%(t)
        self.conn.cursor().execute(cmd)
#       create the tables
        (inputfile,method)=files[i]
        print '... getting values for %s and %s'%(inputfile,method)
        cmd="""CREATE TABLE %s AS SELECT * FROM fma WHERE seed=%s AND tunex=%s AND tuney=%s AND turns='%s' AND inputfile='%s' AND method='%s'"""%(t,seed,tunex,tuney,turns,inputfile,method)
        self.conn.cursor().execute(cmd)
# extract data from fma0 and fma1 where amp1,amp2,angle,part_id exist in both tables
# create string for mysql command to extract data from fma0 and fma1
# column names are renamed to amp1 -> fma0_amp1, fma1_amp2
#     Create sql command
#     a) list of fma tables for sql command (FROM statement in sql command cmd12
      fma_tables=','.join([ 'fma%s'%i for i in xrange(nfma) ])
#     b) list of variables
      if var==None:
        var=['amp1','amp2','angle','inputfile','method','part_id','q1','q2','q3','eps1_min','eps2_min','eps3_min','eps1_max','eps2_max','eps3_max','eps1_avg','eps2_avg','eps3_avg','eps1_0','eps2_0','eps3_0','phi1_0','phi2_0','phi3_0']
      else:
        varmin=['amp1','amp2','angle','inputfile','method','part_id'] # minimum set of variables to extract data
        var.extend(varmin)
        var=list(set(var))
#     c) create command for sql command cmd12 - select fma0.amp1 as fma0_amp1 ...
#        need to convert . to _ in order to import it later as structured array
      varstr=''
      for t in fma_tables.split(','):
        t_var=()
        for v in var:
          t_var=t_var+(t,v,)*2
        varstr=varstr + (', %s.%s AS %s_%s'*len(var))%t_var
#     c) create command for sql command cmd12 -  WHERE (fma0.amp1=fma1.amp1 AND fma0.amp2=fma1.amp2 ...)
      wherestr=''
      for t in fma_tables.split(','):
        if t != 'fma0':
          for v in 'amp1 amp2 angle part_id'.split():
            wherestr=wherestr + 'fma0.%s=%s.%s AND '%(v,t,v)
      wherestr=wherestr[:-5] # delete last AND
      cmd12="""SELECT fma0.seed, fma0.tunex, fma0.tuney, fma0.turns %s FROM %s WHERE (%s)"""%(varstr,fma_tables,wherestr)
      print '... intersepting tables, returning only values for which amp1,amp2,angle,part_id exist in all (inputfile,method) pairs'
      cur=self.conn.cursor().execute(cmd12)
# construct list for ftype
      laux=[('seed',int),('tunex',float),('tuney',float),('turns','|S100')] # common variables
      for t in fma_tables.split(','):
        for v in var:
          if v in ['inputfile','method']: typ='|S100'
          elif v == 'part_id': typ=int
          else: typ=float
          laux.append(('%s_%s'%(t,v),typ))
      ftype=np.dtype(laux)
      try:
        data=np.fromiter(cur,dtype=ftype)
      except ValueError:
        print 'ERROR in get_fma_intersept: check your dtype definition'
        print 'dtype = %s'%ftype
        data=[]
    else:
      data=[]
    for t in fma_tables.split(','):
      cmd="""DROP TABLE IF EXISTS %s"""%(t)
      self.conn.cursor().execute(cmd)
    return data
  def plot_fma_footprint(self,seed,tune,turns,inputfile,method,eps1='eps1_0',eps2='eps2_0',dq=None,vmin=None,vmax=None):
    """plot q1 vs q2 colorcoded by sqrt((eps1+eps2)/eps0)
    
    Parameters:
    ----------
    seed : seed, e.g. 1
    tune : optics tune, e.g. (62.28, 60.31)
    turns : name of directory for number of turns tracked, e.g. 'e4'
    inputfile: name of the inputfile used for the FMA analysis, e.g. IP3_DUMP_1
    method: method used to calculate the tunes, e.g. TUNELASK
    eps1: emittance mode 1, e.g. eps1_0 is the initial emittance 
        (this is equal to the initial emittance if 
        the particles are tracked from 1 to ... turns)
        eps1_min for minimum emittance, eps1_max for maximum emittance
        and eps1_avg for average emittance (over turns used for tune
        analysis)
    eps2: emittance mode 2 (see eps1 for example parameters)
    vmin,vmax: plot range for sqrt((eps1+eps2)/eps0) (larger/smaller
        are saturated)
    dq: plot range is (tune-dq,tune+dq)
    """
    data=self.get_fma(seed,tune,turns,inputfile,method)
    eps0=self.env_var['emit']*self.env_var['pmass']/self.env_var['e0']
    amp=np.sqrt((data[eps1]+data[eps2])/eps0)
    if vmin==None: vmin=np.min(amp)
    if vmax==None:
      vmax=np.max(amp)
      vmax_db=np.max(self.get_db_amplitudes())+self.env_var['nsincl']
      if vmax>vmax_db:# sometimes very large amplitudes occur, which are not important -> saturate them
          vmax=vmax_db
    pl.scatter(data['q1'],data['q2'],c=amp,vmin=vmin,vmax=vmax,linewidth=0)
    cbar=pl.colorbar()
    cbar.set_label(r'$\sigma=\sqrt{\frac{\epsilon_{1,%s}+\epsilon_{2,%s}}{\epsilon_0}}, \ \epsilon_{0,N}=\epsilon_0/\gamma = %2.2f  \ \mu \rm m$'%(eps1.split('_')[1],eps2.split('_')[1],self.env_var['emit']),labelpad=30,rotation=270)
    pl.xlabel(r'$Q_1$')
    pl.ylabel(r'$Q_2$')
    pl.title('%s %s'%(inputfile,method))
# limit plotrange to 1.e-2 distance from lattice tune
    if dq==None: dq=1.e-2
    print """plot_fma_footprint: limit plotrange to %2.2e distance from lattice tune
  in order to exclude chaotic tunes"""%dq
    pl.xlim(np.modf(tune[0])[0]-dq,np.modf(tune[0])[0]+dq)
    pl.ylim(np.modf(tune[1])[0]-dq,np.modf(tune[1])[0]+dq)
    pl.grid()
  def plot_fma_action_tune(self,seed,tune,turns,inputfile,method,mode,eps1='eps1_0',eps2='eps2_0',vmin=None,vmax=None):
    """plot sig1 vs sig2 colorcoded by the tune 
    of mode *mode* with 
    sig[12]=sqrt(eps[12]/eps0)

    Parameters:
    ----------
    seed : seed, e.g. 1
    tune : optics tune, e.g. (62.28, 60.31)
    turns : name of directory for number of turns tracked, e.g. 'e4'
    inputfile: name of the inputfile used for the FMA analysis, e.g. IP3_DUMP_1
    method: method used to calculate the tunes, e.g. TUNELASK
    mode: mode of the tune, 1=horizontal, 2=vertical, 3=longitudinal
    eps1: emittance mode 1, e.g. eps1_0 is the initial emittance 
        (this is equal to the initial emittance if 
        the particles are tracked from 1 to ... turns)
        eps1_min for minimum emittance, eps1_max for maximum emittance
        and eps1_avg for average emittance (over turns used for tune
        analysis)
    eps2: emittance mode 2 (see eps1 for example parameters)
    vmin,vmax: plotrange for tune of mode *mode* (values outside
        [vmin,vmax] are saturated
        default values: vmin=min(tune),vmax=max(tune)
    """
    data=self.get_fma(seed,tune,turns,inputfile,method)
    eps0=self.env_var['emit']*self.env_var['pmass']/self.env_var['e0']
# set colorbar limits
    if vmin == None: vmin = np.min(data['q%s'%mode])
    if vmax == None: vmax = np.max(data['q%s'%mode])
    pl.scatter(np.sqrt(data[eps1]/eps0),np.sqrt(data[eps2]/eps0),c=data['q%s'%mode],linewidth=0,vmin=vmin,vmax=vmax)
    cbar=pl.colorbar()
    cbar.set_label(r'$Q_{%s}$'%mode,labelpad=30,rotation=270)
    pl.xlabel(r'$\sigma_x=\sqrt{\frac{\epsilon_{1,%s}}{\epsilon_0}} , \ \epsilon_{0,N}=\epsilon_0/\gamma = %2.2f \ \mu \rm m$'%(eps1.split('_')[1],self.env_var['emit']))
    pl.ylabel(r'$\sigma_y=\sqrt{\frac{\epsilon_{2,%s}}{\epsilon_0}} , \ \epsilon_{0,N}=\epsilon_0/\gamma = %2.2f \ \mu \rm m$'%(eps2.split('_')[1],self.env_var['emit']))
    pl.title('%s %s'%(inputfile,method))
  def plot_fma_scatter(self,seed,tune,turns,files,var1='eps1_0',var2='eps2_0',dqmode='trans',vmin=None,vmax=None):
    """scatter plot var1 vs var2 of inputfile_0 colorcoded by the 
    difference in tune over all (inputfile,method) pairs in *files*.
    With the parameter *dqmode* the calculation of the difference in
    tune can be selected with:
      'trans':        using Q1 and Q2
      'q1','q2','q3': using only Q1 or only Q2 or only Q3
    Calculation of dQ:
      * two (inputfile,method):
          dQ_j   =log10(|Q_{1,j}-Q_{0,j}|), j=1,2,3
      * more than 2 (inputfile,method) pairs:
        The diffusion in tune is defined as the maximum difference with
        respect to the average tune over all (inputfile,method) pairs.
          Q_{j,avg}=1/nfma*sum_{i=1}^{nfma}(Q_{i,j}), j=1,2,3
          dQ_j   =log10(max_{i=1}^{nfma}(|Q_{i,j}-Q_{j,avg}|)), j=1,2,3
        where nfma is the number of (inputfile,method) pairs.
    Parameters:
    ----------
    seed : seed, e.g. 1
    tune : optics tune, e.g. (62.28, 60.31)
    turns : name of directory for number of turns tracked, e.g. 'e4'
    files: list of inputfiles and methods with
           files=[(inputfile_0,method_0),...,(inputfile_n,method_n)]
           e.g. to compare only two files
           files=[('IP3_DUMP_1','TUNELASK'),('IP3_DUMP_2','TUNELASK')]
    method: method used to calculate the tunes, e.g. TUNELASK
    var1: variable 1
        for amplitudes: eps[12]_0 for initial emittance,
        eps[12]_min for minimum emittance, eps[12]_max for maximum emittance
        and eps[12]_avg for average emittance (over turns used for tune
        analysis)
        for tunes: q[123] for tunes from inputfile1
    var2: variable 2 (see var1 for example parameters)
    dqmode: defines in which plane the diffusion in tune is calculated
        possible values are: 'trans','q1','q2','q3':
          'trans': take the maximum diffusion over the transverse tunes
             explicitly dq=max(dq1_max,dq2_max)
          'q1','q2','q3': use only Q1 or Q2 or Q3
    vmin,vmax: plotrange for delta Q (values outside
        [vmin,vmax] are saturated)
        default values: vmin=-3,vmax=-7
    """
    data=self.get_fma_intersept(seed=seed,tune=tune,turns=turns,files=files)
    nfma = len(files)
    if nfma < 2:
      raise Exception("ERROR in plot_fma_scatter: you need to define at least 2 (inputfile,method) pairs to compare")
# only 2 files -> take diff in tune over two files
    elif nfma == 2:
      if dqmode in ['q1','q2','q3']:
        dqmax=np.abs(data['fma0_%s']-data['fma1_%s'])
      if dqmode == 'trans':
        dq1=np.abs(data['fma0_q1']-data['fma1_q1'])
        dq2=np.abs(data['fma0_q2']-data['fma1_q2'])
        dqmax=np.amax([dq1,dq2],axis=0)
# > 2 files -> max_{i=1,nfma}(q_i-avg(q_i))
    else:
      if dqmode in ['q1','q2','q3']:
        dqavg = np.mean([ data['fma%s_%s'%(i,dqmode)] for i in xrange(nfma) ],axis=0)
        dqmax = np.amax([ np.abs(data['fma%s_%s'%(i,dqmode)]-dqavg) for i in xrange(nfma) ],axis=0)
      if dqmode == 'trans':
        dqavg12={}
        dqmax12={}
        for qm in ['q1','q2']:
          dqavg12[qm] = np.mean([ data['fma%s_%s'%(i,qm)] for i in xrange(nfma) ],axis=0)
          dqmax12[qm] = np.amax([ np.abs(data['fma%s_%s'%(i,qm)]-dqavg12[qm]) for i in xrange(nfma) ],axis=0)
        dqmax=np.amax([dqmax12['q1'],dqmax12['q2']],axis=0)
# define infinitely small (1.e-60) value if dqmax = 0 as log(0) is not defined    
    np.place(dqmax,dqmax==0,[1.e-60])
    dq=np.log10(dqmax)
# default plot range for dq (colorbar)
    if vmin == None: vmin = -3
    if vmax == None: vmax = -7
# amplitude vs dq
    if('eps' in var1 and 'eps' in var2):
      eps0=self.env_var['emit']*self.env_var['pmass']/self.env_var['e0']
      pl.scatter(np.sqrt(data['fma0_%s'%var1]/eps0),np.sqrt(data['fma0_%s'%var2]/eps0),c=dq,marker='.',linewidth=0,vmin=vmin,vmax=vmax)
      if(not self.check_table('da_post')):
        print 'WARNING: Table da_post does not exist! To create it and plot the DA, please run db.mk_da()!'
      else:
        self.plot_da_angle_seed(seed,marker=None,linestyle='-',color='k')
      pl.xlabel(r'$\sigma_x=\sqrt{\frac{\epsilon_{1,%s}}{\epsilon_0}}, \ \epsilon_{0,N}=\epsilon_0/\gamma = %2.2f \ \mu \rm m$'%(var1.split('_')[1],self.env_var['emit']))
      pl.ylabel(r'$\sigma_y=\sqrt{\frac{\epsilon_{2,%s}}{\epsilon_0}}, \ \epsilon_{0,N}=\epsilon_0/\gamma = %2.2f \ \mu \rm m$'%(var2.split('_')[1],self.env_var['emit']))
# tune vs dq
    elif('q' in var1 and 'q' in var2):
      pl.scatter(data['fma0_%s'%var1],data['fma0_%s'%var2],c=dq,marker='.',linewidth=0,vmin=vmin,vmax=vmax,s=5)
      pl.xlabel('$Q_%s$'%(var1.split('q')[1]))
      pl.ylabel('$Q_%s$'%(var2.split('q')[1]))
# limit plotrange to dqlim distance from lattice tune
      dqlim=5.e-2
      print """plot_fma_scatter: limit plotrange to %2.2e distance from lattice tune
  in order to exclude chaotic tunes"""%dqlim
      pl.xlim(np.modf(tune[0])[0]-dqlim,np.modf(tune[0])[0]+dqlim)
      pl.ylim(np.modf(tune[1])[0]-dqlim,np.modf(tune[1])[0]+dqlim)
    elif('amp' in var1 or 'amp' in var2):
      npart = self.env_var['sixdeskpairs']
      ampr = data['fma0_amp1']+(data['fma0_amp2']-data['fma0_amp1'])/(npart-1)*(data['fma0_part_id']/2-1)
      ampx = ampr*np.cos(data['fma0_angle']*np.pi/180.)
      ampy = ampr*np.sin(data['fma0_angle']*np.pi/180.)
      pl.scatter(ampx,ampy,c=dq,marker='.',linewidth=0,vmin=vmin,vmax=vmax,s=5)
      pl.xlabel(r'$\sigma_x$')
      pl.ylabel(r'$\sigma_y$')
    cbar=pl.colorbar()
    if nfma <2:
      if dqmode in ['q1','q2','q3']:
        mode=dqmode.split('q')[1]
        cbar.set_label(r'$\log10{(|Q_{%s,1}-Q_{%s,0}|)}$'%(mode,mode),labelpad=40,rotation=270)
      if dqmode == 'trans':
        cbar.set_label(r'$\log10{(\max_{i=1,2}(|Q_{i,1}-Q_{i,0}|))}$',labelpad=40,rotation=270)
    else:
      if dqmode in ['q1','q2','q3']:
        mode=dqmode.split('q')[1]
        cbar.set_label(r'$\log10{(\max_{i=1}^{\rm nfma}|Q_{%s,i}-\bar Q_{%s}|)}$'%(mode,mode),labelpad=40,rotation=270)
      if dqmode == 'trans':
        cbar.set_label(r'$\max_{j=1,2}(\log10{(\max_{i=1}^{\rm nfma}|Q_{j,i}-\bar Q_{j}|)}$',labelpad=40,rotation=270)
    pl.grid()
  def plot_res(self,m,n,l=0,qz=0,color='b',linestyle='-'):
    """plot resonance of order (m,n,l) where l is
    the order of the sideband with frequency qz in
    the current plot range"""
    footprint.plot_res(m=m,n=n,l=l,qz=qz,color=color,linestyle=linestyle)
  def plot_res_order(self,o,l=0,qz=0,c1='b',lst1='-',c2='b',lst2='--',c3='g',annotate=False):
    """plot resonance lines of order o and sidebands
    of order l and frequency qz in current plot
    range"""
    footprint.plot_res_order(o=o,l=l,qz=qz,c1=c1,lst1=lst1,c2=c2,lst2=lst2,c3=c3,annotate=annotate)
# -------------------------------- da_vs_turns -----------------------------------------------------------
  def st_da_vst(self,data,recreate=False):
    ''' store da vs turns data in database'''
    cols  = SQLTable.cols_from_dtype(data.dtype)
    tab   = SQLTable(self.conn,'da_vst',cols,tables.Da_Vst.key,recreate)
    tab.insert(data)
  def st_da_vst_fit(self,data,recreate=False):
    ''' store da vs turns fit data in database'''
    cols  = SQLTable.cols_from_dtype(data.dtype)
    tab   = SQLTable(self.conn,'da_vst_fit',cols,tables.Da_Vst_Fit.key,recreate=False)
    tab.insert(data)
  def get_da_vst(self,seed,tune):
    '''get da vs turns data from DB'''
    turnsl=self.env_var['turnsl']
    (tunex,tuney)=tune
    #check if table da_vst exists in database
    if(self.check_table('da_vst')):
      ftype=[('seed',int),('tunex',float),('tuney',float),('turn_max',int),('dawtrap',float),('dastrap',float),('dawsimp',float),('dassimp',float),('dawtraperr',float),('dastraperr',float),('dastraperrep',float),('dastraperrepang',float),('dastraperrepamp',float),('dawsimperr',float),('dassimperr',float),('nturn',float),('tlossmin',float),('mtime',float)]
      cmd="""SELECT *
           FROM da_vst WHERE seed=%s AND tunex=%s AND tuney=%s AND turn_max=%d
           ORDER BY nturn"""
      cur=self.conn.cursor().execute(cmd%(seed,tunex,tuney,turnsl))
      data=np.fromiter(cur,dtype=ftype)
    else:
      #02/11/2014 remaned table da_vsturn to da_vst - keep da_vsturn for backward compatibility - note this table did not include the turn_max!!!
      #check if table da_vsturn exists in database
      if(self.check_table('da_vsturn')):
        ftype=[('seed',int),('tunex',float),('tuney',float),('dawtrap',float),('dastrap',float),('dawsimp',float),('dassimp',float),('dawtraperr',float),('dastraperr',float),('dastraperrep',float),('dastraperrepang',float),('dastraperrepamp',float),('dawsimperr',float),('dassimperr',float),('nturn',float),('tlossmin',float),('mtime',float)]
        cmd="""SELECT *
             FROM da_vsturn WHERE seed=%s AND tunex=%s AND tuney=%s
             ORDER BY nturn"""
        cur=self.conn.cursor().execute(cmd%(seed,tunex,tuney))
        data=np.fromiter(cur,dtype=ftype)
      #if tables da_vst and da_vsturn do not exist, return an empty list
      else:      
        data=[]
    return data
  def get_da_vst_fit(self,seed,tune):
    '''get da vs turns data from DB'''
    turnsl=self.env_var['turnsl']
    (tunex,tuney)=tune
    if(self.check_table('da_vst_fit')):
      ftype=[('seed',float),('tunex',float),('tuney',float),('turn_max',int),('fitdat',np.str_, 30),('fitdaterr',np.str_, 30),('fitndrop',float),('kappa',float),('dkappa',float),('res',float),('dinf',float),('dinferr',float),('b0',float),('b0err',float),('b1mean',float),('b1meanerr',float),('b1std',float),('mtime',float)]
      cmd="""SELECT *
           FROM da_vst_fit WHERE seed=%s AND tunex=%s AND tuney=%s AND turn_max=%d
           ORDER BY fitdat,fitdaterr,fitndrop"""
      cur=self.conn.cursor().execute(cmd%(seed,tunex,tuney,turnsl))
      data=np.fromiter(cur,dtype=ftype)
    #if tables da_vst_fit does not exist, return an empty list
    else:      
      data=[]
    return data
  def mk_da_vst_ang(self,seed,tune,turnstep):
    """Da vs turns -- calculate da vs turns for divisors of angmax, 
    e.g. for angmax=29+1 for divisors [1, 2, 3, 5, 6, 10]"""
    RunDaVsTurnsAng(self,seed,tune,turnstep)
  def get_surv(self,seed,tune=None):
    '''get survival turns from DB calculated from emitI and emitII'''
    #change for new db version
    if tune is None:
        tune=self.get_db_tunes()[0]
    (tunex,tuney)=tune
    emit=float(self.env_var['emit'])
    gamma=float(self.env_var['gamma'])
    turnsl=self.env_var['turnsl']
    cmd="""SELECT angle,emitx+emity,
         CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
         FROM results WHERE seed=%s AND tunex=%s AND tuney=%s AND turn_max=%s
         ORDER BY angle,emitx+emity"""
    cur=self.conn.cursor().execute(cmd%(seed,tunex,tuney,turnsl))
    ftype=[('angle',float),('sigma',float),('sturn',float)]
    data=np.fromiter(cur,dtype=ftype)
    data['sigma']=np.sqrt(data['sigma']/(emit/gamma))
    angles=len(set(data['angle']))
    return data.reshape(angles,-1)

  def plot_da_vst(self,seed,tune,ldat,ldaterr,ampmin,ampmax,tmax,slog,sfit,fitndrop):
    """plot dynamic aperture vs number of turns where ldat,ldaterr is the data and 
    the associated error to be plotted. The data is plotted in blue and the fit in red.
    ldat and ldaterr can be given as ldat='dawsimp',ldaterr='dawsimperr') or as list 
    ldat=['dawsimp','dassimp'] and ldaterr=['dawsimperr','dassimperr']."""
    data=self.get_da_vst(seed,tune)
    pl.close('all')
    pl.figure(figsize=(6,6))
    if(len(ldat)==1):#plot data in blue and fit in red
      fmtdat=['bo']
      fmtfit=['r-']
    else:#if several curves are plotted, plot data with points and the fit as solid line in the same color
      fmtdat=['bo','go','ro','co','mo','yo'] 
      fmtfit=['b-','g-','r-','c-','m-','y-'] 
    if(len(ldat)==len(ldaterr)):
      dmax=len(ldat)
      for dd in range(dmax):
        pl.errorbar(data[ldat[dd]],data['tlossmin'],xerr=data[ldaterr[dd]],fmt=fmtdat[dd],markersize=2,label=ldat[dd])
        if(sfit):
          fitdata=self.get_da_vst_fit(seed,tune)
          fitdata=fitdata[fitdata['fitdat']==ldat[dd]]
          fitdata=fitdata[fitdata['fitdaterr']==ldaterr[dd]]
          fitdata=fitdata[np.abs(fitdata['fitndrop']-float(fitndrop))<1.e-6]
          if(len(fitdata)==1):
            pl.plot(fitdata['dinf']+fitdata['b0']/(np.log(data['tlossmin']**np.exp(-fitdata['b1mean']))**fitdata['kappa']),data['tlossmin'],fmtfit[dd])
          else:
            print('Warning: no fit data available or data ambigious!')
    else:
       print('Error in PlotDaVsTurns: ldat and ldaterr must have the same length! Aborting!')
       sys.exit(0)
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
  def plot_surv_2d(self,seed,tune=None,ampmax=14):
    '''survival plot, blue=all particles, red=stable particles'''
    if tune is None:
        tune=self.get_db_tunes()[0]
    data=self.get_surv(seed,tune)
    s,a,t=data['sigma'],data['angle'],data['sturn']
    s,a,t=s[s>0],a[s>0],t[s>0]#delete 0 values
    tmax=np.max(t)
    sx=s*np.cos(a*np.pi/180)
    sy=s*np.sin(a*np.pi/180)
    sxstab=s[t==tmax]*np.cos(a[t==tmax]*np.pi/180)
    systab=s[t==tmax]*np.sin(a[t==tmax]*np.pi/180)
    pl.scatter(sx,sy,20*t/tmax,marker='o',color='b',edgecolor='none')
    pl.scatter(sxstab,systab,4,marker='o',color='r',edgecolor='none')
    pl.title('seed '+str(seed),fontsize=12)
    pl.xlim([0,ampmax])
    pl.ylim([0,ampmax])
    pl.xlabel(r'Horizontal amplitude [$\sigma$]',labelpad=10,fontsize=12)
    pl.ylabel(r'Vertical amplitude [$\sigma$]',labelpad=10,fontsize=12)
  def get_da_angle(self,tunes=None):
    """returns DA results for all seeds and angles"""
    if tunes is None:
        tunes=self.get_tunes()[0]
    angles=self.get_db_angles()
    seeds=self.get_db_seeds()
    sql="SELECT seed,angle,alost1 FROM da_post WHERE tunex==%s AND tuney==%s ORDER by seed,angle"%(tunes[0],tunes[1])
    if(not self.check_table('da_post')):
      print 'WARNING: Table da_post does not exist!'
      print '... running db.mk_da()'
      self.mk_da()
    data=np.array(self.execute(sql)).reshape(len(seeds),len(angles),-1)
    return data
  def get_da_angle_seed(self,seed):
    """returns DA results for seed *seed* and all angles"""
    angles=self.get_db_angles()
    sql="SELECT seed,angle,alost1 FROM da_post WHERE seed==%s ORDER by seed,angle"%(seed)
    if(not self.check_table('da_post')):
      print 'WARNING: Table da_post does not exist!'
      print '... running db.mk_da()'
      self.mk_da()
    data=np.array(self.execute(sql))
    return data
  def plot_da_angle(self,label=None,color='r',ashift=0,marker='o',
                    alpha=0.1,mec='none',**args):
    """plot DA (alost1) vs sigma_x and sigma_y"""
    data=self.get_da_angle()
    for ddd in data:
        s,angle,sig=ddd.T
        angle=(angle+ashift)*np.pi/180
        x=abs(sig)*np.cos(angle)
        y=abs(sig)*np.sin(angle)
        if label is None:
          pl.plot(x,y,marker,mfc=color,mec=mec,alpha=alpha,color=color,**args)
        else:
          pl.plot(x,y,marker,mfc=color,mec=mec,alpha=alpha,label=label,
                  color=color,**args)
          label=None
    #pl.xlabel(r'angle')
    #xa,xb=pl.xlim()
    #ya,yb=pl.xlim()
    #t=np.linspace(0,90,30)*np.pi/180
    #for ss in np.arange(2,min(xb,yb),2):
    #    pl.plot(ss*np.cos(t),ss*np.sin(t),'k-')
    pl.xlabel(r'$\sigma_x$')
    pl.ylabel(r'$\sigma_y$')
    return self
  def plot_da_seed(self,seed,label=None,color='k',marker='o',linestyle='-',alpha=1.0,mec='none'):
    """plot the angle vs the DA (alost1) for one seed *seed*"""
    data=self.get_da_angle_seed(seed)
    if label is None:
      pl.plot(data[:,1],data[:,2],marker=marker,linestyle=linestyle,mfc=color,mec=mec,alpha=alpha)
    else:
      pl.plot(data[:,1],data[:,2],marker=marker,linestyle=linestyle,mfc=color,mec=mec,alpha=alpha,label='%s'%(label))
      pl.legend()
    pl.xlabel('angle')
    pl.ylabel(r'DA [$\sigma$]')
  def plot_da_angle_seed(self,seed,label=None,color='k',ashift=0,marker='o',linestyle='-',alpha=1.0,mec='none'):
    """plot the DA (alost1) expressed in sigmax
    and sigmay for one seed *seed*"""
    data=self.get_da_angle_seed(seed)
    s,angle,sig=data.T
    angle=(angle+ashift)*np.pi/180
    x=abs(sig)*np.cos(angle)
    y=abs(sig)*np.sin(angle)
    if label is None:
      pl.plot(x,y,marker=marker,linestyle=linestyle,mfc=color,mec=mec,alpha=alpha)
    else:
      pl.plot(x,y,marker=marker,linestyle=linestyle,mfc=color,mec=mec,alpha=alpha,label=label)
      pl.legend()
    pl.xlabel('angle')
    pl.ylabel(r'DA [$\sigma$]')

  def check_zeroda(self):
      if self.has_table('da_post'):
         out=self.execute('select tunex,tuney,seed,angle from da_post where alost1==0')
         for tunex,tuney,seed,angle in out:
            print "Tune %s_%s, seed %d, angle %d has alost1=0"%(tunex,tuney,seed,angle)
      else:
        print "No DA command issued yet"
  def get_overlap_angle(self,tunes,seed,angle,colname):
    ss="""select %%s,%s from results
          where tunex=%s and tuney=%s and seed=%s and
                angle=%s and row_num=%%s
          order by amp1"""%(colname,tunes[0],tunes[1],seed,angle)
    pairs=self.env_var['sixdeskpairs']
    l1=np.array(self.execute(ss%('amp1',1))[1:])
    l2=np.array(self.execute(ss%('amp2',pairs))[:-1])
    return l1,l2
  def compare_overlap_angle(self,tunes,seed,angle,colname,threshold):
    l1,l2=self.get_overlap_angle(tunes,seed,angle,colname)
    check=l1[:,1]-l2[:,1]
    idx=abs(check)>threshold
    if len(check[idx])>0:
        print "Check %s: %s != %s"%(
                  [tunes,seed,angle,colname],
                  l1[idx,1],l2[idx,1])
    return l1[idx,0]
  def compare_overlap(self,colname,threshold):
    out=[]
    for tunes in self.get_tunes():
      for seed in self.get_seeds():
         for angle in self.get_angles():
            amps=self.compare_overlap_angle(tunes,seed,angle,colname,threshold)
            if len(amps)>0:
               out.append([tunes,seed,angle,colname,amps])
    return out
  def get_simul(self):
    if self.env_var['long']==1:
        return 'simul'
    elif self.env_var['short']==1:
        return 'short'
  def check_overlap(self, bad_jobs):
    turnse=self.env_var['turnse']
    st=self.env_var['nsincl']
    checks=[('sturns1',0)]
    simul=self.get_simul()
    noproblem=True
    for colname,threshold in checks:
      res=self.compare_overlap(colname,threshold)
      for tunes,seed,angle,colname,amps in res:
        noproblem=False
        amps=['%g-%g:%g-%g'%(a-st,a,a,st+a) for a in amps]
        msg="Error in tunes=%s, seed=%s, angle=%s, colname=%s, amps=%s"
        print(msg%(tunes,seed,angle,colname,', '.join(amps)))
        try:
          self.get_surv(seed,tunes)
          dirname=self.mk_analysis_dir(seed,tunes)
          pl.close('all')
          pl.figure(figsize=(6,6))
          self.plot_surv_2d(seed,tunes)#suvival plot
          pl.savefig('%s/DAsurv.%s.png'%(dirname,turnse))
          print('... saving plot %s/DAsurv.%s.png'%(dirname,turnse))
        except Exception as e:
          print e
          print('... malformed datasets in seed=%s turnes=%s'%(seed,tunes))
        amps2=sorted(set(sum([amp.split(':') for amp in amps],[])))
        for amp1,amp2 in [map(float,a.split('-')) for a in amps2]:
            jdir=self.make_job_trackdir(seed,simul,tunes,amp1,amp2,turnse,angle)
            print('Check %s'%jdir)
            job = (seed,simul,tunes[0],tunes[1],amp1,amp2,"e"+str(turnse),angle)
            bad_jobs.add(job)
    return noproblem
  def check_zero_fort10(self, bad_jobs):
      #lst=self.execute('select  seed,tunex,tuney,amp1,amp2,turns,angle,row_num from results where betx==0')
      #lst=set(lst)
      #noproblem=True
      #for res in sorted(lst):
      #    noproblem=False
      #    print "Zero results for %s"%list(res)
      #    bad_jobs.add( ??? )
      return True
  def check_completed_results(self, bad_jobs):
      print ("Check missing results")
      for job in self.get_missing_jobs():
          print job, 'missing'
          bad_jobs.add(job)
      return len(self.get_missing_jobs())==0
  def make_job_work_string(self, job):
    tmp="%s%%%s%%s%%%s%%%s%%%s%%%.14g\n"
    name=self.LHCDescrip
    seed,simul,tunex,tuney,amp1,amp2,turns,angle=job
    ranges="%.14g_%.14g"%(amp1,amp2)
    tunes="%.14g_%.14g"%(tunex,tuney)
    return tmp%(name,seed,tunes,ranges,turns[1:],angle)
  def update_work_dir(self, bad_jobs):
    good_jobs = set(self.gen_job_params())-bad_jobs
    def write_jobs(filename, jobs):
      with open(filename, "w") as f:
        for job in sorted(jobs):
          f.write(self.make_job_work_string(job))
    write_jobs("work/completed_cases"  ,good_jobs)
    write_jobs("work/incomplete_cases", bad_jobs)
  def check_results(self, update_work=False):
     bad_jobs=set()
     noproblem=self.check_completed_results(bad_jobs)
     if noproblem:
        noproblem=self.check_zero_fort10(bad_jobs)
     if noproblem:
        noproblem=self.check_overlap(bad_jobs)
     if update_work:
        self.update_work_dir(bad_jobs)
     return noproblem
  def get_fort3(self,seed,amp1,angle,tunes=None):
    ss="""select fort3 from six_input
          where seed=%s and
                tunex=%s and tuney=%s and
                amp1=%s and turns='%s'
                and angle=%s"""
    if tunes is None:
        tunes=self.get_tunes()[0]
    turns='e%g'%self.env_var['turnsle']
    ss=ss%(seed,tunes[0],tunes[1],amp1,turns,angle)
    fort3=self.execute(ss)[0][0]
    return decompressBuf(decompressBuf(fort3))
  def get_fort_2_8_16(self,seed):
    ss="""select fort2, fort8, fort16 from mad6t_results
          where seed=%s"""%(seed)
    forts=map(decompressBuf,self.execute(ss)[0])
    return forts
  def extract_job(self,dest,seed,amp1,angle,tunes=None):
    fc2,fc8,fc16=self.get_fort_2_8_16(seed)
    fc3=self.get_fort3(seed,amp1,angle,tunes)
    print("Prepare directory '%s'"%dest)
    if not os.path.isdir(dest):
      os.mkdir(dest)
    open(os.path.join(dest,'fort.2'),'w').write(fc2)
    open(os.path.join(dest,'fort.3'),'w').write(fc3)
    open(os.path.join(dest,'fort.8'),'w').write(fc8)
    open(os.path.join(dest,'fort.16'),'w').write(fc16)
    sixtrack=self.env_var['SIXTRACKEXE']
    sixtrackln=os.path.join(dest,'sixtrack')
    if not os.path.exists(sixtrackln):
      os.symlink(sixtrack,sixtrackln)
  def extract_madinout(self,dest,seed):
    madin=self.get_mad_in(seed)
    madin=dict( (ii,data) for data,ii in madin)
    madout=self.get_mad_out(seed)
    madout=dict( (ii,data) for data,ii,mt in madout)
    print(madin,madout)
    for run in madin.keys():
      print("Prepare directory '%s/%s'"%(dest,run))
      if not os.path.isdir(dest):
        os.mkdir(dest)
      fn=self.LHCDescrip+".%d.madx"%selfseed
      open(os.path.join(dest,run,fn),'w').write(madin[run])
    for run in madout.keys():
      print("Prepare directory '%s/%s'"%(dest,run))
      if not os.path.isdir(dest):
        os.mkdir(dest)
      fn=self.LHCDescrip+".%d.madx"%selfseed
      open(os.path.join(dest,run,fn),'w').write(madout[run])


