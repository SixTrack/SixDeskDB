#!/usr/bin/python
import os
import sys
import re
import gzip
import time

import numpy as np
try:
  import matplotlib.pyplot as pl
except RuntimeError,msg:
  pass

class ObjDebug(object):
  def __getattribute__(self,k):
    ga=object.__getattribute__
    sa=object.__setattr__
    cls=ga(self,'__class__')
    modname=cls.__module__
    mod=__import__(modname)
    reload(mod)
    sa(self,'__class__',getattr(mod,cls.__name__))
    return ga(self,k)


def _parseval(val):
  try:
    val=int(val)
  except ValueError:
    try:
      val=float(val)
    except ValueError:
      pass
  return val


def _getoption(basedir='.'):
  tmp="sh -c '. %s/sixdeskenv;. %s/sysenv; env'"
  cmd=tmp%(basedir,basedir)
  opt={}
  for l in os.popen(cmd):
    name,val=l.strip().split('=',1)
    opt[name]=_parseval(val)
  return opt

def minmaxavg(l,fmt="%13e"):
  l=np.array(l)
  mi=l.min()
  ma=l.max()
  av=l.mean()
  tmp="min %s avg %s max %s"%(fmt,fmt,fmt)
  return tmp%(mi,av,ma)


def parse_bjobs():
  cmd='/usr/bin/bjobs'
  if os.path.exists(cmd):
    out={}
    fh=os.popen('%s -W -a' %cmd)
    fh.readline()
    for l in fh:
      job=LSFJob(l.split())
      out[job.job_name]=job
  else:
    print "Error: %s not found" % cmd
    out=None
  return out

def jobs_stats(jobs):
  pending=[v for j,v in jobs.items() if v.stat=='PEND']
  running=[v for j,v in jobs.items() if v.stat=='RUN']
  done=[v for j,v in jobs.items() if v.stat=='DONE']
  print "Jobs running : %d" % len(running)
  print "Jobs pending : %d" % len(pending)
  print "Jobs just done: %d" % len(done)



class LSFJob(tuple):
  __slots__=()
  jobid          =property(lambda x: x[ 0])
  user           =property(lambda x: x[ 1])
  stat           =property(lambda x: x[ 2])
  queue          =property(lambda x: x[ 3])
  from_host      =property(lambda x: x[ 4])
  exec_host      =property(lambda x: x[ 5])
  job_name       =property(lambda x: x[ 6])
  submit_time    =property(lambda x: x[ 7])
  proj_name      =property(lambda x: x[ 8])
  cpu_used       =property(lambda x: x[ 9])
  mem            =property(lambda x: x[10])
  swap           =property(lambda x: x[11])
  pids           =property(lambda x: x[12])
  start_time     =property(lambda x: x[13])
  finish_time    =property(lambda x: x[14])
  def run_since(self):
    newdate=time.strftime('%Y/')+self.start_time
    t=time.mktime(time.strptime(newdate,'%Y/%m/%d-%H:%M:%S'))
    return time.time()-t


class Mad6tOut(ObjDebug):
  def __init__(self,**opt):
    self.basedir=opt['sixtrack_input']
    self.LHCDescrip=opt['LHCDescrip']
    self.ista=opt['ista']
    self.iend=opt['iend']
    print "Mad6tOut basedir: %s"%self.basedir
  def get_outdirnames(self):
    out=[]
    for l in os.listdir(self.basedir):
      if l.startswith('mad.'):
        print "Mad6tOut rundir found: %s" % l
        fdir=os.path.join(self.basedir,l)
        ftime=os.path.getmtime(fdir)
        out.append((ftime,fdir))
    return [l[1] for l in sorted(out)]
  def get_outfnames(self):
    self.missing_seed=[]
    out=[]
    try:
      basedir=self.get_outdirnames()[-1]
    except IndexError:
      raise ValueError, "Mad6tOut no mad_run directory found"
    print "Mad6tOut rundir used: %s" % basedir
    for i in range(self.ista,self.iend+1):
      out_fn=os.path.join(basedir,"%s.out.%d"%(self.LHCDescrip,i))
      if os.path.exists(out_fn):
        out.append(out_fn)
      else:
        print "Mad6tOut Error: outfn '%s' does not exists" %out_fn
        self.missing_seed.append(i)
    print "Mad6tOut found %d out file names"%len(out)
    return out
  def get_jobname(self,seed):
    return "%s_%s_mad6t_%d"%(self.workspace,self.LHCDescrip,seed)
  def check_out(self):
    self.closest2=[]
    self.closest1=[]
    self.closest0=[]
    self.kqs={}
    self.kqt={}
    for fn in self.get_outfnames():
      for l in open(fn):
        l=l.strip()
        if l.startswith('closest'):
          if l.startswith('closest2 =  '):
            self.closest2.append(float(l.split('=')[1].split(';')[0]))
          elif l.startswith('closest1 =  '):
            self.closest1.append(float(l.split('=')[1].split(';')[0]))
          elif l.startswith('closest0 =  '):
            self.closest0.append(float(l.split('=')[1].split(';')[0]))
        elif 'kmqsmax*100' in l:
          name,val=extract_kmax(l)
          self.kqs.setdefault(name,[]).append(val)
        elif 'kmqtmax*100' in l:
          name,val=extract_kmax(l)
          self.kqt.setdefault(name,[]).append(val)
    print "Mad6tOut clo0: %s"%minmaxavg(self.closest0)
    print "Mad6tOut clo1: %s"%minmaxavg(self.closest1)
    print "Mad6tOut clo2: %s"%minmaxavg(self.closest2)
    kqsmax=[max(abs(m) for m in l) for l in zip(*self.kqs.values())]
    kqtmax=[max(abs(m) for m in l) for l in zip(*self.kqt.values())]
    print "Mad6tOut kqt : %s"%minmaxavg(kqtmax)
    print "Mad6tOut kqs : %s"%minmaxavg(kqsmax)
  def get_forts_filenames(self):
    out=[]
    for fort in [2,16,8]:
      for seed in range(self.ista,self.iend+1):
        yield 'fort.%d_%d.gz'%(fort,seed)
  def check_forts(self):
    for fn in self.get_forts_filenames():
      ffn=os.path.join(self.basedir,fn)
      if os.path.exists(ffn):
        if os.path.getsize(ffn)<10:
          print "Mad6tOut %s too small"
      else:
          print "Mad6tOut %s does not exists"
  def check_all(self):
    self.check_out()
    self.check_forts()



def extract_kmax(l):
  name,val=l.split('=')
  name=name.split('/')[0]
  val=float(val.split(';')[0])
  return name,val




class TrackOut(object):
  names="""ista iend
  ns1l ns2l nsincl sixdeskpairs
  kinil kendl kmaxl kstep
  turnsl turnsle
  tunex tuney
  short long
  sixdesktrack LHCDescrip sixdeskwork
  sixdeskhome sixdeskstudy forcelsf
  """.split()
  def class_reload(self):
    import sixdeskpy
    reload(sixdeskpy)
    self.__class__=getattr(sixdeskpy,self.__class__.__name__)
  def __init__(self,**opt):
    for attr in TrackOut.names:
      setattr(self,attr,opt.get(attr))
  def gen_betavaluefn(self,seed,tunes):
    base=os.path.join(self.sixdesktrack,self.LHCDescrip)
    t='%s/%d/simul/%s/betavalues'
    tunes="%4.2f_%4.2f"%tunes
    return t%(base,seed,tunes)
  def gen_fort10fn(self,seed,tunes,rng,turne,angle):
    base=os.path.join(self.sixdesktrack,self.LHCDescrip)
    t='%s/%d/simul/%s/%s/e%d/%g/fort.10.gz'
    rng="%d_%d"%rng
    tunes="%4.2f_%4.2f"%tunes
    return t%(base,seed,tunes,rng,turne,angle)
  def get_angles(self):
    s=90./(self.kmaxl+1)
    return np.arange(self.kinil,self.kendl+1,self.kstep)*s
  def get_seeds(self):
    return range(self.ista,self.iend+1)
  def get_ranges(self):
    return [(a,a+self.nsincl) for a in range(self.ns1l,self.ns2l,self.nsincl)]
  def gen_fort10(self):
    tunes=(self.tunex,self.tuney)
    for seed in self.get_seeds():
      for rng in self.get_ranges():
        for angle in self.get_angles():
          fn=self.gen_fort10fn(seed,tunes,rng,self.turnsle,angle)
          yield fn
  def gen_betavalues(self):
    tunes=(self.tunex,self.tuney)
    for seed in self.get_seeds():
      fn=self.gen_betavaluefn(seed,tunes)
      yield fn
  def check_betavalues(self):
    out=[]
    for fn in self.gen_betavalues():
      if os.path.exists(fn):
        out.append(map(float,open(fn).read().split()))
      else:
        out.append([0.]*14)
    names="betx,alfx,bety,alfy,qx,qy,dqx,dqy,x,px,y,py,z,pz".split(',')
    self.betavalues=dict(zip(names,zip(*out)))
    for n in names:
      print "TrackOut %-4s: %s"%(n,minmaxavg(self.betavalues[n]))
  def check_fort10(self):
    self.found=[]
    self.missing=[]
    tunes=(self.tunex,self.tuney)
    for fn in self.gen_fort10():
      if os.path.exists(fn):
        self.found.append(fn)
      else:
        self.missing.append(fn)
    print "TrackOut fort10: found %d" % len(self.found)
    print "TrackOut fort10: missing %d" % len(self.missing)
    return self
  def get_summ10fn(self):
    return "%s_%s"%(self.LHCDescrip,'fort10.npy')
  def parse_fort10(self):
    out=[]
    mask=[]
    seeds=len(self.get_seeds())
    ranges=len(self.get_ranges())
    angles=len(self.get_angles())
    pairs=self.sixdeskpairs
    for fn in self.gen_fort10():
      if os.path.exists(fn):
        mask.append(True)
      else:
        mask.append(False)
      out.append(Fort10(fn).parse().data)
    self.mask10=np.array(mask).reshape(seeds,ranges,angles)
    self.fort10=out
    self.fort10=np.array(out).reshape(seeds,ranges,angles,30,60)
    self.fort10=self.fort10.transpose(4,0,2,1,3).reshape(60,seeds,
                                                         angles,ranges*pairs)
    self.save_fort10()
  def save_fort10(self,fn=None):
    if fn is None:
      fn=self.get_summ10fn()
    np.save(fn,self.fort10)
    print "TrackOut saved: %s" % fn
    pass
  def load_fort10(self,fn=None):
    self.fort10=None
    if fn is None:
      fn=self.get_summ10fn()
    if not os.path.exists(fn):
      print "TrackOut fort10 not found: %s"%fn
      self.parse_fort10()
    print "TrackOut loading: %s"%fn
    self.fort10=np.load(fn)
    return self
  def get_missinglsfjobs(self):
    regexp=re.compile('(.*)/([^/]+)/(\d\d?)/(simul)/([\d_.]*)/(\d\d?_\d\d?)/e(\d)/([\d_.]+)/fort.10.gz')
    tmp="%s%%%s%%s%%%s%%%s%%%s%%%s"
    missing=[]
    for f10 in self.missing:
      base,name,seed,sim,tunes,ranges,turns,angle=regexp.match(f10).groups()
      lsfjob=tmp%(name,seed,tunes,ranges,turns,angle)
      missing.append(lsfjob)
    self.missing=missing
    return missing
  def make_missing_jobs(self):
    force=bool(self.forcelsf)
    if hasattr(self,'missing'):
      self.get_missinglsfjobs()
    if len(self.missing)==0:
      print "TrackOut make missing_jobs: no missing jobs"
    else:
      if force==False:
        self.running=parse_bjobs()
        if self.running is None:
           print "Cannot check running jobs, use force=True"
           return
      else:
        self.running={}
      missingfn=os.path.join(self.sixdeskwork,'lsfjobs','missing_jobs')
      rerun=[]
      for lsfjob in self.missing:
        name,seed,s,tunes,ranges,turns,angle=lsfjob.split('%')
        jobshort="%2s %s %5s %s %4s"%(seed,tunes,ranges,turns,angle)
        inlsf=False
        if lsfjob in self.running:
          job=self.running[lsfjob]
          if job.stat in ('PEND','RUN'):
            tmp="TrackOut job: %s %s %s %s %s"
            print tmp%(job.jobid,jobshort,job.submit_time,job.start_time,job.stat)
            inlsf=True
            if job.stat=='RUN':
              exp=job.run_since()
              if exp>2*24*3600:
                inlsf=False
        if not inlsf:
          print "TrackOut job: %s adding" %(jobshort)
          rerun.append(lsfjob)
      fh=open(missingfn,'w')
      for lsfjob in rerun:
          fh.write("%s\n"%lsfjob)
      fh.close()
      print "TrackOut fort10: found %d" % len(self.found)
      print "TrackOut fort10: missing %d" % len(self.missing)
      jobs_stats(self.running)
      print 'TrackOut fort10: Added %d job(s) in %s' % (len(rerun),missingfn)
      if len(rerun)>0:
        print 'To launch jobs do:'
        print 'cd %s;set_env %s; run_missing_jobs' %(self.sixdeskhome,self.LHCDescrip)


fort10fields=[
 'Maximum turn number',
 'Stability Flag (0=stable, 1=lost)',
 'Horizontal Tune',
 'Vertical Tune',
 'Horizontal beta-function',
 'Vertical beta-function',
 'Horizontal amplitude 1st particle',
 'Vertical amplitude 1st particle',
 'Relative momentum deviation Deltap',
 'Final distance in phase space',
 'Maximumslope of distance in phase space',
 'Horizontal detuning',
 'Spread of horizontal detuning',
 'Vertical detuning',
 'Spread of vertical detuning',
 'Horizontal factor to nearest resonance',
 'Vertical factor to nearest resonance',
 'Order of nearest resonance',
 'Horizontal smear',
 'Vertical smear',
 'Transverse smear',
 'Survived turns 1st particle',
 'Survived turns 2nd particle',
 'Starting seed for random generator',
 'Synchrotron tune',
 'Horizontal amplitude 2nd particle',
 'Vertical amplitude 2nd particle',
 'Minimum horizontal amplitude',
 'Mean horizontal amplitude',
 'Maximum horizontal amplitude',
 'Minimum vertical amplitude',
 'Mean vertical amplitude',
 'Maximum vertical amplitude',
 'Minimum horizontal amplitude (linear decoupled)',
 'Mean horizontal amplitude (linear decoupled)',
 'Maximum horizontal amplitude (linear decoupled)',
 'Minimum vertical amplitude (linear decoupled)',
 'Mean vertical amplitude (linear decoupled)',
 'Maximum vertical amplitude (linear decoupled)',
 'Minimum horizontal amplitude (nonlinear decoupled)',
 'Mean horizontal amplitude (nonlinear decoupled)',
 'Maximum horizontal amplitude (nonlinear decoupled)',
 'Minimum vertical amplitude (nonlinear decoupled)',
 'Mean vertical amplitude (nonlinear decoupled)',
 'Maximum vertical amplitude (nonlinear decoupled)',
 'Emittance Mode I',
 'Emittance Mode II',
 'Secondary horizontal beta-function',
 'Secondary vertical beta-function',
 "Q'x",
 "Q'y",
 'Dummy1',
 'Dummy2',
 'Dummy3',
 'Dummy4',
 'Dummy5',
 'Dumy6',
 'Dummy7',
 'Internal1',
 'Internal2']

class Fort10(object):
  def __init__(self,fn):
    self.filename=fn
  def parse(self):
    if os.path.exists(self.filename):
      self.data=np.fromstring(gzip.open(self.filename).read(),sep=' ')
    else:
      self.data=np.zeros(1800,dtype=float)
    self.data=self.data.reshape((30,60))
    return self
  @classmethod
  def printfields(cls):
    for n,l in enumerate(fort10fields):
      print "%2d %s"%(n,l)

class Summ10(ObjDebug):
  def __init__(self,data):
    self.data=data
    self.seeds=self.data.shape[1]
    self.angles=self.data.shape[2]
    self.ampls=self.data.shape[3]
  def get_ampl(self):
    return self.data[6],self.data[7]
    #return self.data[27],self.data[30]
    #return self.data[34],self.data[37]
  def plot_ampl(self):
    x,y=self.get_ampl()
    #for seed in range(self.seeds):
    for seed in [0]:
      for angle in range(self.angles):
        pl.plot(x[seed,angle],y[seed,angle],'r.')


class SixDesk(object):
  def __init__(self,basedir='.',**opts):
    self.basedir=basedir
    self.opts=_getoption(basedir=basedir)
    self.opts.update(opts)
    print "Study LHCDescrip: %s" % self.opts['LHCDescrip']
  def get_name(self):
    return self.opts['LHCDescrip']
  def check_mad6t(self):
    mad6t=Mad6tOut(**self.opts)
    mad6t.check_all()
    return mad6t
  def check_mad6t(self):
    mad6t=Mad6tOut(**self.opts)
    mad6t.check_all()
    return mad6t
  def check_lsf(self):
    track=TrackOut(**self.opts)
    track.check_betavalues()
    track.check_fort10()
    track.make_missing_jobs()
    return track
  def check_track(self):
    track=TrackOut(**self.opts)
    track.check_fort10()
    return track
  def save_fort10(self):
    track=TrackOut(**self.opts)
    track.parse_fort10()
    track.save_fort10()
    return track
  def load_fort10(self):
    track=TrackOut(**self.opts)
    track.load_fort10()
    return Summ10(track.fort10)
  def check_optics(self):
    mad6t=Mad6tOut(**self.opts)
    mad6t.check_all()
    track=TrackOut(**self.opts)
    track.check_betavalues()
  def help(self):
    print """Usage:
    %s <cmd> [basedir [name value]]
    cmd:
      check_mad6t
      check_optics
      check_track
      check_lsf
      save_fort10
    """
  def exe(self,cmd='help'):
    if hasattr(self,cmd):
      return getattr(self,cmd)()


if __name__=='__main__':
  from  sixdeskpy import *
  basedir='.'
  opts={}
  cmd='help'
  if len(sys.argv)>1:
    cmd=sys.argv[1]
    if len(sys.argv)>2:
      basedir=sys.argv[2]
    if len(sys.argv)>3:
      for name,val in zip(sys.argv[3::2],sys.argv[4::2]):
        opts[name]=_parseval(val)
  study=SixDesk(basedir,**opts)
  study.exe(cmd)

