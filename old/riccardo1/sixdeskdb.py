import sqlite3

import numpy as np
import matplotlib.pyplot as pl
import scipy.signal

from sqltable import SQLTable


def guess_range(l):
  l=sorted(set(l))
  start=l[0]
  if len(l)>1:
    step=np.diff(l).min()
  else:
    step=None
  stop=l[-1]
  return start,stop,step


class Fort10(object):
  fields=[
  ('turn_max', 'int', 'Maximum turn number'),
  ('sflag', 'int', 'Stability Flag (0=stable, 1=lost)'),
  ('qx', 'float', 'Horizontal Tune'),
  ('qy', 'float', 'Vertical Tune'),
  ('betx', 'float', 'Horizontal beta-function'),
  ('bety', 'float', 'Vertical beta-function'),
  ('sigx1', 'float', 'Horizontal amplitude 1st particle'),
  ('sigy1', 'float', 'Vertical amplitude 1st particle'),
  ('deltap', 'float', 'Relative momentum deviation Deltap'),
  ('dist', 'float', 'Final distance in phase space'),
  ('distp', 'float', 'Maximumslope of distance in phase space'),
  ('qx_det', 'float', 'Horizontal detuning'),
  ('qx_spread', 'float', 'Spread of horizontal detuning'),
  ('qy_det', 'float', 'Vertical detuning'),
  ('qy_spread', 'float', 'Spread of vertical detuning'),
  ('resxfact', 'float', 'Horizontal factor to nearest resonance'),
  ('resyfact', 'float', 'Vertical factor to nearest resonance'),
  ('resorder', 'int', 'Order of nearest resonance'),
  ('smearx', 'float', 'Horizontal smear'),
  ('smeary', 'float', 'Vertical smear'),
  ('smeart', 'float', 'Transverse smear'),
  ('sturns1', 'int', 'Survived turns 1st particle'),
  ('sturns2', 'int', 'Survived turns 2nd particle'),
  ('sseed', 'float', 'Starting seed for random generator'),
  ('qs', 'float', 'Synchrotron tune'),
  ('sigx2', 'float', 'Horizontal amplitude 2nd particle'),
  ('sigy2', 'float', 'Vertical amplitude 2nd particle'),
  ('sigxmin', 'float', 'Minimum horizontal amplitude'),
  ('sigxavg', 'float', 'Mean horizontal amplitude'),
  ('sigxmax', 'float', 'Maximum horizontal amplitude'),
  ('sigymin', 'float', 'Minimum vertical amplitude'),
  ('sigyavg', 'float', 'Mean vertical amplitude'),
  ('sigymax', 'float', 'Maximum vertical amplitude'),
  ('sigxminld', 'float', 'Minimum horizontal amplitude (linear decoupled)'),
  ('sigxavgld', 'float', 'Mean horizontal amplitude (linear decoupled)'),
  ('sigxmaxld', 'float', 'Maximum horizontal amplitude (linear decoupled)'),
  ('sigyminld', 'float', 'Minimum vertical amplitude (linear decoupled)'),
  ('sigyavgld', 'float', 'Mean vertical amplitude (linear decoupled)'),
  ('sigymaxld', 'float', 'Maximum vertical amplitude (linear decoupled)'),
  ('sigxminnld', 'float','Minimum horizontal amplitude (nonlinear decoupled)'),
  ('sigxavgnld', 'float', 'Mean horizontal amplitude (nonlinear decoupled)'),
  ('sigxmaxnld', 'float','Maximum horizontal amplitude (nonlinear decoupled)'),
  ('sigyminnld', 'float', 'Minimum vertical amplitude (nonlinear decoupled)'),
  ('sigyavgnld', 'float', 'Mean vertical amplitude (nonlinear decoupled)'),
  ('sigymaxnld', 'float', 'Maximum vertical amplitude (nonlinear decoupled)'),
  ('emitx', 'float', 'Emittance Mode I'),
  ('emity', 'float', 'Emittance Mode II'),
  ('betx2', 'float', 'Secondary horizontal beta-function'),
  ('bety2', 'float', 'Secondary vertical beta-function'),
  ('qpx', 'float', "Q'x"),
  ('qpy', 'float', "Q'y"),
  ('dum1', 'float', 'Dummy1'),
  ('dum2', 'float', 'Dummy2'),
  ('dum3', 'float', 'Dummy3'),
  ('dum4', 'float', 'Dummy4'),
  ('dum5', 'float', 'Dummy5'),
  ('dum6', 'float', 'Dummy6'),
  ('dum7', 'float', 'Dummy7'),
  ('int1', 'float', 'Internal1'),
  ('int2', 'float', 'Internal2')]

class JobParams(object):
  fields=[
    ('study','str',''),
    ('seed','int',''),
    ('tunex','float',''),
    ('tuney','float',''),
    ('amp1','float',''),
    ('amp2','float',''),
    ('joined','str',''),
    ('turns','int',''),
    ('angle','float',''),
    ('row','int','') ]

class Study(object):
    fields=[
  ('trackdir','str','path hosting the tracking results e.g. name/'),
  ('LHCDescrip','str','name of the job and mask file'),
  ('sixtrack_input','str','path hosting the sixtrack input'),
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


class SixDeskDB(object):
  def __init__(self,dbname):
    self.dbname=dbname
    self.db=sqlite3.connect(dbname)
    self.make_result_table()
  def make_result_table(self):
    jobnames=[c[0] for c in JobParams.fields]
    res=JobParams.fields+Fort10.fields
    cols=SQLTable.cols_from_fields(res)
    self.results=SQLTable(self.db,'results',cols,keys=jobnames)
  def add_results(self,data):
    db=self.db;table='results'
    sql="REPLACE INTO %s VALUES (%s)"
    vals=','.join(('?')*len(self.results.cols))
    sql_cmd=sql%(table,vals)
    cur=db.cursor()
    cur.executemany(sql_cmd, data)
    db.commit()
  def execute(self,sql):
    cur=self.db.cursor()
    cur.execute(sql)
    self.db.commit()
    return list(cur)
  def inspect_results(self):
    names='seed,tunex,tuney,amp1,amp2,turns,angle'
    for name in names.split(','):
      sql="SELECT DISTINCT %s FROM results"%name
      print sql
      data=[d[0] for d in self.execute(sql)]
      print name, guess_range(data)
  def iter_job_params(self):
    names='study,seed,tunex,tuney,amp1,amp2,turns,angle,row'
    sql='SELECT DISTINCT %s FROM results'%names
    return self.db.cursor().execute(sql)
  def iter_job_params_comp(self):
    names='seed,tunex,tuney,amp1,amp2,turns,angle'
    sql='SELECT DISTINCT %s FROM results'%names
    return self.db.cursor().execute(sql)
  def get_num_results(self):
    return self.execute('SELECT count(*) from results')[0][0]
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
    p['LHCDescrip']=data['study'][0]
    return p
  def get_survival_turns(self,seed):
    cmd="""SELECT angle,amp1+(amp2-amp1)*row/30,
            CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
            FROM results WHERE seed=%s ORDER BY angle,sigx1"""
    cmd="""SELECT angle,sqrt(sigx1**2+sigy1**2),
            CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
            FROM results WHERE seed=%s ORDER BY angle,sigx1"""
    cur=self.db.cursor().execute(cmd%seed)
    ftype=[('angle',float),('amp',float),('surv',float)]
    data=np.fromiter(cur,dtype=ftype)
    angles=len(set(data['angle']))
    return data.reshape(angles,-1)
  def plot_survival_2d(self,seed,smooth=None):
    data=self.get_survival_turns(seed)
    a,s,t=data['angle'],data['amp'],data['surv']
    self._plot_survival_2d(a,s,t,smooth=smooth)
    pl.title('Seed %d survived turns'%seed)
  def plot_survival_2d_avg(self,smooth=None):
    cmd="""SELECT seed,angle,amp1+(amp2-amp1)*row/30,
            CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
            FROM results ORDER BY seed,angle,sigx1"""
    cur=self.db.cursor().execute(cmd)
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
    table=np.array([mk_tuple(a,s,t,nlim) for nlim in np.arange(0,1.1e6,1e4)]).T
    nlim,tmin,tavg,tmax,savg,savg2=table
    pl.plot(savg,tmin,label='min')
    pl.plot(savg,tavg,label='avg')
    pl.plot(savg,tmax,label='max')
    return table

