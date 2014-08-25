import os, gzip, time, sys

from collections import namedtuple

import numpy as np
try:
  import matplotlib.pyplot as pl
except RuntimeError,msg:
  pass


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

def split_boincfile(fn):
  ll=fn.lstrip('sd').lstrip('_').split('__')
  data=ll[-8:]
  study,seed,simul,tunes,rng,turns,angle=data
  if '-' in rng:
    join='-'
  else:
    join='_'
  seed=int(seed)
  tunex,tuney=map(float,tunes.split('_'))
  amp1,amp2=map(float,rng.split(join))
  angle=float(angle.split('_')[0])
  turns=10**int(turns)
  # print 'split_boincfile: ',study,seed,tunex,tuney,amp1,amp2,join,turns,angle
  return study,seed,tunex,tuney,amp1,amp2,join,turns,angle

def extract_kmax(l):
  name,val=l.split('=')
  name=name.split('/')[0]
  val=float(val.split(';')[0])
  return name,val

def minmaxavg(l,fmt="%13e"):
  l=np.array(l)
  mi=l.min()
  ma=l.max()
  av=l.mean()
  tmp="min %s avg %s max %s"%(fmt,fmt,fmt)
  return tmp%(mi,av,ma)


def guess_range(l):
  l=sorted(set(l))
  start=l[0]
  if len(l)>1:
    step=sorted(set(np.diff(l)))[0]
  else:
    step=None
  stop=l[-1]
  return start,stop,step


def parse_env(studydir):
  tmp="sh -c '. %s/sixdeskenv;. %s/sysenv; python -c %s'"
  cmd=tmp%(studydir,studydir,'"import os;print os.environ"')
  return eval(os.popen(cmd).read())

pytype=dict(float=float,int=int,str=str)

class SixDeskDir(object):
  fields=[
  ('ista','int','initial seed number'),
  ('iend','int','final seed number'),
  ('tunex','float','initial tunex'),
  ('tuney','float','initial tuney'),
  ('deltax','float','step in tunex'),
  ('deltay','float','step in tuney'),
  ('tunex1','float','final tunex'),
  ('tuney1','float','final tuney'),
  ('kinil','float','initial angle index'),
  ('kendl','float','final angle index'),
  ('kmaxl','float','max angle index'),
  ('kstep','float','final angle step'),
  ('ns1l','float','initial amplitude'),
  ('ns2l','float','final amplitude'),
  ('nsincl','float','amplitude step'),
  ('turnsl','int','turns for long run')]
  def __init__(self,studydir=None,**kwargs):
    if studydir is not None:
      if 'boinc' not in studydir:
        studydir=studydir.strip().replace('/sixdeskenv','')
        self.studydir=studydir
        opts=parse_env(self.studydir)
        opts.update(kwargs)
        self.__dict__.update(opts)
        for name,ftype,desc in SixDeskDir.fields:
          if hasattr(self,name):
            setattr(self,name,pytype[ftype](getattr(self,name)))
      else:
        studydir=studydir.strip()
        self.studydir=studydir
        self.fileown='%s/owner'%studydir
        self.fileres='%s/results'%studydir
    else:
      opts={}
        

  def __repr__(self):
    out=[]
    tmp='%-15s:= %s'
    out.append(tmp%('LHCDescrip',self.LHCDescrip))
    out.append(tmp%('sixtrack_input',self.sixtrack_input))
    out.append(tmp%('sixdesktrack',self.sixdesktrack))
    tunex='%6g:%6g:%6g'%(self.tunex,self.tunex1,self.deltax)
    tuney='%6g:%6g:%6g'%(self.tuney,self.tuney1,self.deltay)
    out.append(tmp%('tunex',tunex))
    out.append(tmp%('tuney',tuney))
    seed='%6d:%6d:%6d'%(self.ista,self.iend,1)
    out.append(tmp%('seed',seed))
    angle='%6d:%6d:%6d'%(self.kinil,self.kendl,self.kstep)
    out.append(tmp%('angle',angle))
    return '\n'.join(out)

  def replace_scratch(self,newscratch):
    for n in ['sixdesktrack','sixtrack_input']:
      setattr(self,n,getattr(self,n).replace(self.scratchdir,newscratch))
    self.scratchdir=newscratch
    return self

  def get_seeds(self):
    return range(self.ista,self.iend+1)

  def get_angles(self):
    s=90./(self.kmaxl+1)
    return np.arange(self.kinil,self.kendl+1,self.kstep)*s

  def get_amplitudes(self):
    return [(a,a+self.nsincl) for a in np.arange(self.ns1l,self.ns2l,self.nsincl)]

  def iter_tunes(self):
    qx=self.tunex;qy=self.tuney
    while qx<=self.tunex1 and qy<=self.tuney1:
      yield qx,qy
      qx+=self.deltax
      qy+=self.deltay

  def get_tunes(self):
    return list(self.iter_tunes())

  def get_num_fort10(self):
    return len(list(self.iter_job_params()))

  def trackdir_exists(self):
    base=os.path.join(self.sixdesktrack,self.LHCDescrip)
    return os.path.isdir(base)

  def get_job_dirname(self,seed,tunex,tuney,amp1,amp2,turns,angle):
    base=os.path.join(self.sixdesktrack,self.LHCDescrip)
    t='%s/%d/simul/%s/%s/e%d/%g/'
    turne=np.log10(turns)
    rng="%g_%g"%(amp1,amp2)
    tunes="%4.2f_%4.2f"%(tunex,tuney)
    return t%(base,seed,tunes,rng,turne,angle)

  def iter_job_params(self):
    for seed in self.get_seeds():
      for tunex,tuney in self.get_tunes():
        for amp1,amp2 in self.get_amplitudes():
          for angle in self.get_angles():
            yield (seed,tunex,tuney,amp1,amp2,self.turnsl,angle)

  def iter_job_dirnames(self):
    for ppp in self.iter_job_params():
      yield self.get_job_dirname(*ppp)

  def iter_fort10_filenames(self):
    for dn in self.iter_job_dirnames():
      yield os.path.join(dn,'fort.10.gz')

  def iter_fort3_filenames(self):
    for dn in self.iter_job_dirnames():
      yield os.path.join(dn,'fort.3.gz')

  def get_fort2_filenames(self,seed):
    pass

  def get_fort8_filenames(self,seed):
    pass

  def get_fort16_filenames(self,seed):
    pass

  def get_betavalue_fn(self,seed,tunex,tuney):
    base=os.path.join(self.sixdesktrack,self.LHCDescrip)
    t='%s/%d/simul/%s/betavalues'
    tunes="%4.2f_%4.2f"%(tunex,tuney)
    return t%(base,seed,tunes)

  def iter_betavalue_filenames(self):
    for seed in self.get_seeds():
      for tunex,tuney in self.iter_tunes():
        yield self.get_betavalue_fn(seed,tunex,tuney)

  def get_betavalues_filemanes(self):
    missing=[]
    found=[]
    for fn in self.iter_betavalue_filenames():
      if os.path.exists(fn):
        found.append(fn)
      else:
        missing.append(fn)
    return found, missing

  def get_betavalue_stats(self):
    found,missing=self.get_betavalues_filemanes()
    nfound=len(found)
    ntot=nfound+len(missing)
    print "Found: %d betavalue files out of %d"%(nfound,ntot)

  def iter_found_fort10(self):
    base=os.path.join(self.sixdesktrack,self.LHCDescrip)
    cmd='find "%s" -type f -name "fort.10.gz"'%base
    print cmd
    i=0
    for l in os.popen(cmd):
      i+=1
      if i%1000==0:
        print i
      yield l.strip()
      
  def iter_found_boincfile(self):
    if os.path.isfile(fileown):
        f=open(fileown,'r')
        owner=(f.readlines())[0].rstrip('\n')
        if owner=='dbanfi': 
            results=os.listdir(fileres)
    return result 



  def iter_results_boinc(self):
    i=0
    for fn in os.listdir(self.fileres):
      # print fn
      if fn=='results': continue
      params=split_boincfile(fn)
      study,seed,tunex,tuney,amp1,amp2,join,turns,angle=params
      if join=='_':
        for nnn,lll in enumerate(open(self.fileres+'/'+fn)):
          try:
            f10=tuple(map(float,lll.split()))
            i+=1
            if i%1000==0: print i
          except ValueError,msg:
            print fn,nnn,lll
            print msg
            raise ValueError
          # print params+(nnn,)+f10
          yield params+(nnn,)+f10
      # print self.fileres+'/'+fn
      os.system('rm %s'%(self.fileres+'/'+fn))  
      
        
  def iter_results(self):
    for fn in self.iter_found_fort10():
      params=split_fort10fn(fn)
      study,seed,tunex,tuney,amp1,amp2,join,turns,angle=params
      if join=='_':
        for nnn,lll in enumerate(gzip.open(fn)):
          try:
            f10=tuple(map(float,lll.split()))
          except ValueError,msg:
            print fn,nnn,lll
            print msg
            raise ValueError
          yield params+(nnn,)+f10          


  def inspect_existing_fort10(self):
    single=[]
    joined=[]
    for fn in list(self.iter_found_fort10()):
      data=split_fort10fn(fn)
      study,seed,tunex,tuney,amp1,amp2,join,turns,angle=data
      if join=='_':
        single.append(data)
      else:
        joined.append(data)
    names='study,seed,tunex,tuney,amp1,amp2,join,turns,angle'.split(',')
    single=dict(zip(names,zip(*single)))
    joined=dict(zip(names,zip(*joined)))
    def pr_stat(single):
      ln=len(single['study'])
      if ln>0:
        print 'seed',guess_range(single['seed'])
        print 'amp',guess_range(single['amp1']+single['amp2'])
        print 'tunex',guess_range(single['tunex'])
        print 'tuney',guess_range(single['tuney'])
        print 'angle',guess_range(single['angle'])
        print 'turns',guess_range(single['turns'])
        print ln
    pr_stat(single)
    pr_stat(joined)


