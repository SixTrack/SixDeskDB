# da vs turns module
import numpy as np
import matplotlib.pyplot as pl
import glob, sys, os, time
from deskdb import SixDeskDB,tune_dir,mk_dir

# basic functions
def get_divisors(n):
  """finds the divisors of an integer number"""
  large_divisors = []
  for i in xrange(1, int(np.sqrt(n) + 1)):
    if n % i is 0:
      yield i
      if i is not n / i:
        large_divisors.insert(0, n / i)
  for divisor in large_divisors:
    yield divisor

# functions necessary for the analysis
#@profile
def get_min_turn_ang(s,t,a,it):
  """returns array with (angle,minimum sigma,sturn) of particles with lost turn number < it.

  check if there is a particle with angle ang with lost turn number <it
  if true: lost turn number and amplitude of the last stable particle is saved = particle "before" the particle with the smallest amplitude with nturns<it
  if false: the smallest lost turn number and the largest amplitude is saved 
  """
  # s,t,a are ordered by angle,amplitude
  angles,sigmas=t.shape# angles = number of angles, sigmas = number of amplitudes
  ftype=[('angle',float),('sigma',float),('sturn',float)]
  mta=np.zeros(angles,dtype=ftype)
  # enumerate(a[:,0]) returns (0, a[0]), (1, a[1]), (2, a[2]), ... = iang, ang where iang = index of the array (0,1,2,...) for ang = angle (e.g. [1.5, ... , 1.5] , [3.0, ... ,3.0])
  for iang,ang in enumerate(a[:,0]):
    tang = t[iang]
    sang = s[iang]
    iturn = tang<it # select lost turn number < it
    if(any(tang[iturn])):
      sangit=sang[iturn].min()
      argminit=sang.searchsorted(sangit) # get index of smallest amplitude with sturn<it - amplitudes are ordered ascending
      mta[iang]=(ang,sang[argminit-1],tang[argminit-1])#last stable amplitude -> index argminit-1
    else:
      mta[iang]=(ang,sang.max(),tang.min())
  return mta
def select_ang_surv(data,seed,nang):
  """returns data reduced to ((angmax+1)/nang)-1 angles -> nang being the divisor of angmax"""
  angmax=len(data['angle'][:,0])#number of angles
  print nang
  if(nang not in list(get_divisors(angmax+1))[:-2]):
    print('%s is not a divisor of %s or two large - the two largest divisors are not used')%(nang,angmax+1)
    sys.exit(0)
  #define variables for only selection of angles
  s,a,t=data['sigma'][nang::nang+1],data['angle'][nang::nang+1],data['sturn'][nang::nang+1]
  ftype=[('angle',float),('sigma',float),('sturn',float)]
  dataang=np.ndarray(np.shape(a),dtype=ftype)
  dataang['sigma'],dataang['angle'],dataang['sturn']=s,a,t
  return dataang
#@profile
def mk_da_vst(data,seed,tune,turnstep):
  """returns 'seed','tunex','tuney','dawavg','dasavg','dawsimp','dassimp',
             'dawavgerr','dasavgerr','dasavgerrep','dasavgerrepang',
             'dasavgerrepamp','dawsimperr','dassimperr','nturn','tlossmin',
             'mtime'
  the da is in steps of turnstep
  das:       integral over radius 
             das = 2/pi*int_0^(2pi)[r(theta)]dtheta=<r(theta)>
                 = 2/pi*dtheta*sum(a_i*r(theta_i))
  daw:       integral over phase space
             daw = (int_0^(2pi)[(r(theta))^4*sin(2*theta)]dtheta)^1/4
                 = (dtheta*sum(a_i*r(theta_i)^4*sin(2*theta_i)))^1/4
  simple average (avg): a_i=1
  simpson rule (simp):  a_i=(55/24.,-1/6.,11/8.,1,....1,11/8.,-1/6.,55/24.)
                        numerical recipes open formulas 4.1.15 and 4.1.18      
  """
  mtime=time.time()
  (tunex,tuney)=tune
  s,a,t=data['sigma'],data['angle'],data['sturn']
  tmax=np.max(t[s>0])#maximum number of turns
  #set the 0 in t to tmax*100 in order to check if turnnumber<it (any(tang[tang<it])<it in get_min_turn_ang)
  t[s==0]=tmax*100
  angmax=len(a[:,0])#number of angles
  angstep=np.pi/(2*(angmax+1))#step in angle in rad
  ampstep=np.abs((s[s>0][1])-(s[s>0][0]))
#  print angstep
#  print ampstep
  ftype=[('seed',int),('tunex',float),('tuney',float),('dawavg',float),('dasavg',float),('dawsimp',float),('dassimp',float),('dawavgerr',float),('dasavgerr',float),('dasavgerrep',float),('dasavgerrepang',float),('dasavgerrepamp',float),('dawsimperr',float),('dassimperr',float),('nturn',float),('tlossmin',float),('mtime',float)]
  l_turnstep=len(np.arange(turnstep,tmax,turnstep))
  daout=np.ndarray(l_turnstep,dtype=ftype)
  for nm in daout.dtype.names:
    daout[nm]=np.zeros(l_turnstep)
  dacount=0
  currentdawavg=0
  currenttlossmin=0
  #define integration coefficients at beginning and end which are unequal to 1
  ajsimp_s=np.array([55/24.,-1/6.,11/8.])#Simpson rule
  ajsimp_e=np.array([11/8.,-1/6.,55/24.])
  warn=True
  for it in np.arange(turnstep,tmax,turnstep):
#  for it in np.arange(turnstep,turnstep+100000,turnstep):
    mta=get_min_turn_ang(s,t,a,it)
    mta_angle=mta['angle']*np.pi/180#convert to rad
    l_mta_angle=len(mta_angle)
    mta_sigma=mta['sigma']
    if(l_mta_angle>6):
      # define coefficients for simpson rule (simp)
      # ajsimp =  [55/24.,-1/6.,11/8.,1,....1,11/8.,-1/6.,55/24. ]
      ajsimp=np.concatenate((ajsimp_s,np.ones(l_mta_angle-6),ajsimp_e))
      calcsimp=True
    else:
      if(warn):
        print('WARNING! mk_da_vst - You need at least 7 angles to calculate the da vs turns with the simpson rule! da*simp* will be set to 0.')
        warn=False 
      calcsimp=False
    # ---- simple average (avg)
    # int
    dawavgint = ((mta_sigma**4*np.sin(2*mta_angle)).sum())*angstep
    dawavg    = (dawavgint)**(1/4.)
    dasavg    = (2./np.pi)*(mta_sigma).sum()*angstep
    # error
    dawavgerrint   = np.abs(((mta_sigma**3*np.sin(2*mta_angle)).sum())*angstep*ampstep)
    dawavgerr      = np.abs(1/4.*dawavgint**(-3/4.))*dawavgerrint
#    print('dawavgerrint=%s'%dawavgerrint)
#    print('dawavg      =%s'%dawavg)
#    print('dawavgerr   =%s'%dawavgerr)
    dasavgerr      = np.abs(angstep*ampstep*l_mta_angle)*(2./np.pi)
#    print('dasavg      =%s'%dasavg)
#    print('dasavgerr   =%s'%dasavgerr)
    dasavgerrepang = ((np.abs(np.diff(mta_sigma))).sum())/(2*angmax)
    dasavgerrepamp = ampstep/2
    dasavgerrep    = np.sqrt(dasavgerrepang**2+dasavgerrepamp**2)
    # ---- simpson rule (simp)
    if(calcsimp):
      # int
      dawsimpint = (ajsimp*((mta_sigma**4)*np.sin(2*mta_angle))).sum()*angstep
      dawsimp    = (dawsimpint)**(1/4.)
      dassimpint = (ajsimp*mta_sigma).sum()*angstep
      dassimp    = (2./np.pi)*dassimpint
      # error
      dawsimperrint = (ajsimp*(4*(mta_sigma**3)*np.sin(2*mta_angle))).sum()*angstep*ampstep
      dawsimperr    = np.abs(1/4.*dawsimpint**(-3/4.))*dawsimperrint
      dassimperr = np.abs(angstep*ampstep*(ajsimp.sum()))*(2./np.pi) 
    else:
      (dawsimp,dassimp,dawsimperr,dassimperr)=np.zeros(4)
    tlossmin=np.min(mta['sturn'])
    if(dawavg!=currentdawavg and it-turnstep > 0 and tlossmin!=currenttlossmin):
      daout[dacount]=(seed,tunex,tuney,dawavg,dasavg,dawsimp,dassimp,dawavgerr,dasavgerr,dasavgerrep,dasavgerrepang,dasavgerrepamp,dawsimperr,dassimperr,it-turnstep,tlossmin,mtime)
#      daout[dacount]=(seed,tunex,tuney,dawavg,dasavg,dawsimp,dassimp,dasavgerrep,dasavgerrepang,dasavgerrepamp,it-turnstep,tlossmin,mtime)
      dacount=dacount+1
    currentdawavg =dawavg
    currenttlossmin=tlossmin
  return daout[daout['dawavg']>0]#delete 0 from errors

# functions to reload and create da.out files for previous scripts
def save_daout_old(data,path):
  daoutold=data[['dawavg','dasavg','dasavgerrep','dasavgerrepang','dasavgerrepamp','nturn','tlossmin']]
  np.savetxt(path+'/DAold.out',daoutold,fmt='%.6f %.6f %.6f %.6f %.6f %d %d')
def reload_daout_old(path):
  ftype=[('dawavg',float),('dasavg',float),('dasavgerrep',float),('dasavgerrepang',float),('dasavgerrepamp',float),('nturn',float),('tlossmin',float)]
  return np.loadtxt(glob.glob(path+'/DAold.out*')[0],dtype=ftype,delimiter=' ')
def save_daout(data,path):
  daout=data[['seed','tunex','tuney','dawavg','dasavg','dawsimp','dassimp','dawavgerr','dasavgerr','dasavgerrep','dasavgerrepang','dasavgerrepamp','dawsimperr','dassimperr','nturn','tlossmin','mtime']]
#  daoutold=data[['dawavg','dasavg','dasavgerrep','dasavgerrepang','dasavgerrepamp','nturn','tlossmin']]
  np.savetxt(path+'/DA.out',daout,fmt='%d %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %d %d %.12f')
def reload_daout(path):
  ftype=[('seed',int),('tunex',float),('tuney',float),('dawavg',float),('dasavg',float),('dawsimp',float),('dassimp',float),('dawavgerr',float),('dasavgerr',float),('dasavgerrep',float),('dasavgerrepang',float),('dasavgerrepamp',float),('dawsimperr',float),('dassimperr',float),('nturn',float),('tlossmin',float),('mtime',float)]
  return np.loadtxt(glob.glob(path+'/DA.out*')[0],dtype=ftype,delimiter=' ')
def save_dasurv(data,path):
  np.savetxt(path+'/DAsurv.out',np.reshape(data,-1),fmt='%.8f %.8f %d')
def reload_dasurv(path):
  ftype=[('angle', '<f8'), ('sigma', '<f8'), ('sturn', '<f8')]
  data=np.loadtxt(glob.glob(path+'/dasurv.out*')[0],dtype=ftype,delimiter=' ')
  angles=len(set(data['angle']))
  return data.reshape(angles,-1)
def plot_surv_2d_stab(db,lbl,mksize,cl,seed,tune,ampmax):
  '''survival plot: stable area of two studies'''
  data=db.get_surv(seed,tune)
  s,a,t=data['sigma'],data['angle'],data['sturn']
  s,a,t=s[s>0],a[s>0],t[s>0]#delete 0 values
  tmax=np.max(t)
  sxstab=s[t==tmax]*np.cos(a[t==tmax]*np.pi/180)
  systab=s[t==tmax]*np.sin(a[t==tmax]*np.pi/180)
  pl.scatter(sxstab,systab,mksize,marker='o',color=cl,edgecolor='none',label=lbl)
  pl.title('seed '+str(seed),fontsize=12)
  pl.xlim([0,ampmax])
  pl.ylim([0,ampmax])
  pl.xlabel(r'Horizontal amplitude [$\sigma$]',labelpad=10,fontsize=12)
  pl.ylabel(r'Vertical amplitude [$\sigma$]',labelpad=10,fontsize=12)
def plot_surv_2d_comp(db,dbcomp,lbl,complbl,seed,tune,ampmax):
  '''survival plot: stable area of two studies'''
  data=db.get_surv(seed,tune)
  datacomp=dbcomp.get_surv(seed,tune)
  pl.close('all')
  pl.figure(figsize=(6,6))
  plot_surv_2d_stab(db,lbl,10,'b',seed,tune,ampmax)
  plot_surv_2d_stab(dbcomp,complbl,2,'r',seed,tune,ampmax)
  pl.legend(loc='best')
def plot_comp_da_vst(db,dbcomp,lblname,complblname,seed,tune,ampmin,ampmax,tmax,slog):
  """plot dynamic aperture vs number of turns, blue/green=simple average, red/orange=weighted average"""
  data=db.get_da_vst(seed,tune)
  datacomp=dbcomp.get_da_vst(seed,tune)
  pl.close('all')
  pl.figure(figsize=(6,6))
  pl.errorbar(data['dasavg'],data['tlossmin'],xerr=data['dasavgerrep'],fmt='bo',markersize=2,label='simple average '+lblname)
  pl.plot(data['dawavg'],data['tlossmin'],'ro',markersize=3,label='weighted average '+lblname)
  pl.errorbar(datacomp['dasavg'],datacomp['tlossmin'],xerr=datacomp['dasavgerrep'],fmt='go',markersize=2,label='simple average '+complblname)
  pl.plot(datacomp['dawavg'],datacomp['tlossmin'],'o',color='orange',markersize=3,label='weighted average '+complblname)
  pl.title('seed '+str(seed))
  pl.xlim([ampmin,ampmax])
  pl.xlabel(r'Dynamic aperture [$\sigma$]',labelpad=10,fontsize=12)
  pl.ylabel(r'Number of turns',labelpad=15,fontsize=12)
  plleg=pl.gca().legend(loc='best')
  for label in plleg.get_texts():
    label.set_fontsize(12)
  if(slog):
    pl.ylim([5.e3,tmax])
    pl.yscale('log')
  else:
    pl.ylim([0,tmax])
    pl.gca().ticklabel_format(style='sci',axis='y',scilimits=(0,0))
def clean_dir_da_vst(db,files):
  '''create directory structure and if force=true delete old files of da vs turns analysis'''
  for seed in db.get_seeds():
    for tune in db.get_tunes():
      pp=db.mk_analysis_dir(seed,tune)# create directory
      if(len(files)>0):#delete old plots and files
        for filename in files:
          ppf=os.path.join(pp,filename)
          if(os.path.exists(ppf)): os.remove(ppf)
  if(len(files)>0):
    print('remove old {0} ... files in '+db.LHCDescrip).format(files)

# for error analysis - data is not saved in database but output files are generated
def RunDaVsTurnsAng(db,seed,tune,turnstep):
  """Da vs turns -- calculate da vs turns for divisors of angmax, 
     e.g. for angmax=29+1 for divisors [1, 2, 3, 5, 6, 10]"""
  # start analysis
  try:
    turnstep=int(float(turnstep))
  except [ValueError,NameError,TypeError]:
    print('Error in RunDaVsTurns: turnstep must be integer values!')
    sys.exit(0)
  if(seed not in db.get_db_seeds()):
    print('WARNING: Seed %s is missing in database !!!'%seed)
    sys.exit(0)
  if(tune not in db.get_tunes()):
    print('WARNING: tune %s is missing in database !!!'%tune)
    sys.exit(0)
  seed=int(seed)
  print('analyzing seed {0} and tune {1}...').format(str(seed),str(tune))
  dirname=db.mk_analysis_dir(seed,tune)#directory struct already created in clean_dir_da_vst, only get dir name (string) here
  print('... get survival data')
  dasurvtot= db.get_surv(seed,tune)
  a=dasurvtot['angle']
  angmax=len(a[:,0])#number of angles
  print('... number of angles: %s, divisors: %s'%(angmax,str(list(get_divisors(angmax+1))[0:-2])))
  for nang in list(get_divisors(angmax+1))[0:-2]:
    dirnameang='%s/%s'%(dirname,nang)
    mk_dir(dirnameang)
    dasurv=select_ang_surv(dasurvtot,seed,nang)
    print('... calculate da vs turns')
    daout=mk_da_vst(dasurv,seed,tune,turnstep)
    save_daout(daout,dirnameang)
    print('... save da vs turns data in {0}/DA.out').format(dirnameang)
# in analysis - putting the pieces together
def RunDaVsTurns(db,force,outfile,outfileold,turnstep):
  '''Da vs turns -- calculate da vs turns for study dbname'''
  # start analysis
  try:
    turnstep=int(float(turnstep))
  except [ValueError,NameError,TypeError]:
    print('Error in RunDaVsTurns: turnstep must be integer values!')
    sys.exit(0)
  if(not db.check_seeds()):
    print('!!! Seeds are missing in database !!!')
  for seed in db.get_db_seeds():
    seed=int(seed)
    print('analyzing seed {0} ...').format(str(seed))
    for tune in db.get_tunes():
      print('analyzing tune {0} ...').format(str(tune))
      dirname=db.mk_analysis_dir(seed,tune)#directory struct already created in clean_dir_da_vst, only get dir name (string) here
      print('... get survival data')
      dasurv= db.get_surv(seed,tune)
      print('... get da vs turns data')
      daout = db.get_da_vst(seed,tune)
      if(len(daout)>0):#reload data, if input data has changed redo the analysis
        an_mtime=daout['mtime'].min()
        res_mtime=db.execute('SELECT max(mtime) FROM six_results')[0][0]
        if res_mtime>an_mtime or force is True:
          files=['DA.out','DAsurv.out','DA.png','DAsurv.png','DAsurv_log.png','DAsurv_comp.png','DAsurv_comp_log.png']
          clean_dir_da_vst(db,files)# create directory structure and delete old files
          print('... input data has changed - recalculate da vs turns')
          daout=mk_da_vst(dasurv,seed,tune,turnstep)
          print('.... save data in database')
          db.st_da_vst(daout)
      else:#create data
        print('... calculate da vs turns')
        daout=mk_da_vst(dasurv,seed,tune,turnstep)
        print('.... save data in database')
        db.st_da_vst(daout)
      if(outfile):# create dasurv.out and da.out files
        save_dasurv(dasurv,dirname)
        print('... save survival data in {0}/DAsurv.out').format(dirname)
        save_daout(daout,dirname)
        print('... save da vs turns data in {0}/DA.out').format(dirname)
      if(outfileold):
        save_daout_old(daout,dirname)
        print('... save da vs turns (old data format) data in {0}/DAold.out').format(dirname)
def PlotDaVsTurns(db,ampmaxsurv,ampmindavst,ampmaxdavst,tmax,plotlog):
  print('Da vs turns -- create survival and da vs turns plots')
  try:
    ampmaxsurv=float(ampmaxsurv)
    ampmindavst=float(ampmindavst)
    ampmaxdavst=float(ampmaxdavst)
  except [ValueError,NameError,TypeError]:
    print('Error in RunDaVsTurns: ampmaxsurv and amprangedavst must be float values!')
    sys.exit(0)
  #remove all files
  if(plotlog):
    files=['DA_log.png','DAsurv.png']
  else:
    files=['DA.png','DAsurv.png']
  clean_dir_da_vst(db,files)# create directory structure and delete old files if force=true
  if(not db.check_seeds()):
    print('!!! Seeds are missing in database !!!')
  for seed in db.get_db_seeds():
    seed=int(seed)
    for tune in db.get_tunes():
      dirname=db.mk_analysis_dir(seed,tune)#directory struct already created in clean_dir_da_vst, only get dir name (string) here
      pl.close('all')
      db.plot_surv_2d(seed,tune,ampmaxsurv)#suvival plot
      pl.savefig(dirname+'/DAsurv.png')
      print('... saving plot {0}/DAsurv.png').format(dirname)
      db.plot_da_vst(seed,tune,ampmindavst,ampmaxdavst,tmax,plotlog)#da vs turns plot
      if(plotlog==True):
        pl.savefig(dirname+'/DA_log.png')
        print('... saving plot {0}/DA_log.png').format(dirname)
      else:
        pl.savefig(dirname+'/DA.png')
        print('... saving plot {0}/DA.png').format(dirname)

def PlotCompDaVsTurns(db,dbcomp,lblname,complblname,ampmaxsurv,ampmindavst,ampmaxdavst,tmax,plotlog):
  '''Comparison of two studies: survival plots (area of stable particles) and Da vs turns plots'''
  try:
    ampmaxsurv=float(ampmaxsurv)
    ampmindavst=float(ampmindavst)
    ampmaxdavst=float(ampmaxdavst)
    tmax=int(float(tmax))
  except ValueError,NameError:
    print('Error in RunDaVsTurns: ampmaxsurv and amprangedavst must be float values and tmax an integer value!')
    sys.exit(0)
  #remove all files
  if(plotlog):
    files=['DA_comp_log.png','DAsurv_comp.png']
  else:
    files=['DA_comp.png','DAsurv_comp.png']
  clean_dir_da_vst(db,files)# create directory structure and delete old files if force=true
# start analysis
  if(not db.check_seeds()):
    print('Seeds are missing in database!')
  for seed in db.get_db_seeds():
    seed=int(seed)
    for tune in db.get_tunes():
      dirname=db.mk_analysis_dir(seed,tune)#directories already created with 
      pl.close('all')
      plot_surv_2d_comp(db,dbcomp,lblname,complblname,seed,tune,ampmaxsurv)
      pl.savefig(dirname+'/DAsurv_comp.png')
      print('... saving plot {0}/DAsurv_comp.png').format(dirname)
      plot_comp_da_vst(db,dbcomp,lblname,complblname,seed,tune,ampmindavst,ampmaxdavst,tmax,plotlog)
      if(plotlog==True):
        pl.savefig(dirname+'/DA_comp_log.png')
        print('... saving plot {0}/DA_comp_log.png').format(dirname)
      else:
        pl.savefig(dirname+'/DA_comp.png')
        print('... saving plot {0}/DA_comp.png').format(dirname)

#      # case: reload data
#      else:
#        try:
#          daout = reload_daout(dirname)
#        except IndexError:
#          print('Error in RunDaVsTurns - DA.out file not found for seed {0}!').format(str(seed))
#          sys.exit(0)
#        try:
#          dasurv= reload_dasurv(dirname)
#        except IndexError:
#          print('Error in RunDaVsTurns - DAsurv.out file not found for seed {0}!').format(str(seed))
#          sys.exit(0)
