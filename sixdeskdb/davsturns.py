# DA vs turns module
import os as os
import numpy as np
import matplotlib.pyplot as pl
import glob as glob
from SixdeskDB import SixDeskDB

# basic functions
def ang_to_i(ang,angmax):
  """converts angle [degrees] to index (sixtrack)"""
  return int(ang/(90./(angmax+1))-1)

# functions necessary for the analysis
def get_min_turn_ang(s,t,a,it):
  """returns array with (angle,minimum sigma,sturn) of particles with lost turn number < it.

  check if there is a particle with angle ang with lost turn number <it
  if true: lost turn number and amplitude of the last stable particle is saved = particle "before" the particle with the smallest amplitude with nturns<it
  if false: the smallest lost turn number and the largest amplitude is saved 
  """
  angmax=len(a[:,0])#number of angles
  ftype=[('angle',float),('sigma',float),('sturn',float)]
  minturnang=np.ndarray(angmax,dtype=ftype)
  #initialize to 0
  for i in range(len(minturnang)):
    minturnang[i]=(0,0,0)
  for ang in set(a[:,0]):
  #save in minturnang
    tang=t[a==ang]
    sang=s[a==ang]
    if(any(tang[tang<it])):
      sangit=np.amin(sang[tang<it])
      argminit=np.amin(np.where(sang==sangit)[0])#get index of smallest amplitude with sturn<it - amplitudes are ordered ascending
#      print(argminit)
      minturnang[ang_to_i(ang,angmax)]=(ang,sang[argminit-1],tang[argminit-1])#last stable amplitude -> index argminit-1
    else: 
      minturnang[ang_to_i(ang,angmax)]=(ang,np.amax(sang),np.amin(tang))
  return minturnang
def get_da_vs_turns(data,turnstep):
  """returns DAout with DAw,DAs,DAserr,DAserrang,DAserramp,nturn,tlossmin.
  DAs:       simple average over radius r, trapezoidal rule
             DAs = 2/pi*int_0^(2pi)[r(theta)]dtheta=<r(theta)>
                 = 2/pi*dtheta*sum(r(theta_i))
  DAw:       weighted average, integration with trapezoidal rule
             DAw = (int_0^(2pi)[(r(theta))^4*sin(2*theta)]dtheta)^1/4
                 = (dtheta*sum(r(theta_i)^4*sin(2*theta_i)))^1/4
  """
  s,a,t=data['sigma'],data['angle'],data['sturn']
  tmax=np.max(t[s>0])#maximum number of turns
#  print tmax
  #set the 0 in t to tmax*100 in order to use t.any()<it later
  t[s==0]=tmax*100
  angmax=len(a[:,0])#number of angles
  angstep=np.pi/(2*(angmax+1))#step in angle in rad
  ampstep=np.abs((s[s>0][1])-(s[s>0][0]))
#  print(ampstep)
  ftype=[('DAw',float),('DAs',float),('DAserr',float),('DAserrang',float),('DAserramp',float),('nturn',float),('tlossmin',float)]
  DAout=np.ndarray(len(np.arange(turnstep,tmax,turnstep)),dtype=ftype)
  for i in range(len(DAout)):
    DAout[i]=(0,0,0,0,0,0,0)
  dacount=0
  currentDAw=0
  currenttlossmin=0
  for it in np.arange(turnstep,tmax,turnstep):
    minturnang=get_min_turn_ang(s,t,a,it)
    minturnang['angle']=minturnang['angle']*np.pi/180#convert to rad
    #MF: should add factor 3/2 for first and last angle
    DAw=(np.sum(minturnang['sigma']**4*np.sin(2*minturnang['angle']))*angstep)**(1/4.)
    DAs=(2./np.pi)*np.sum(minturnang['sigma'])*angstep
    DAserrang=np.sum(np.abs(np.diff(minturnang['sigma'])))/(2*angmax)
    DAserramp=ampstep/2
    DAserr=np.sqrt(DAserrang**2+DAserramp**2)
    tlossmin=np.min(minturnang['sturn'])
    if(DAw!=currentDAw and it-turnstep > 0 and tlossmin!=currenttlossmin):
      DAout[dacount]=(DAw,DAs,DAserr,DAserrang,DAserramp,it-turnstep,tlossmin)
      dacount=dacount+1
    currentDAw     =DAw
    currenttlossmin=tlossmin
  return DAout[DAout['DAw']>0]#delete 0 from errors

def save_daout(data,path):
  np.savetxt(path+'/DA.out',data,fmt='%.8f %.8f %.8f %.8f %.8f %d %d')
def reload_daout(path):
  ftype=[('DAw',float),('DAs',float),('DAserr',float),('DAserrang',float),('DAserramp',float),('nturn',float),('tlossmin',float)]
  return np.loadtxt(glob.glob(path+'/DA.out*')[0],dtype=ftype,delimiter=' ')
def save_dasurv(data,path):
  np.savetxt(path+'/DAsurv.out',np.reshape(data,-1),fmt='%.8f %.8f %d')
def reload_dasurv(path):
  ftype=[('angle', '<f8'), ('sigma', '<f8'), ('sturn', '<f8')]
  data=np.loadtxt(glob.glob(path+'/DAsurv.out*')[0],dtype=ftype,delimiter=' ')
  angles=len(set(data['angle']))
  return data.reshape(angles,-1)

def plot_surv_2d(data,seed,ampmax=14):
  """survival plot, blue=all particles, red=stable particles"""
  pl.close('seed '+seed)
  pl.figure('seed '+seed,figsize=(6,6))
  s,a,t=data['sigma'],data['angle'],data['sturn']
  s,a,t=s[s>0],a[s>0],t[s>0]#delete 0 values
  tmax=np.max(t)
  sx=s*np.cos(a*np.pi/180) 
  sy=s*np.sin(a*np.pi/180) 
  sxstab=s[t==tmax]*np.cos(a[t==tmax]*np.pi/180) 
  systab=s[t==tmax]*np.sin(a[t==tmax]*np.pi/180) 
  pl.scatter(sx,sy,20*t/tmax,marker='o',color='b',edgecolor='none')
  pl.scatter(sxstab,systab,4,marker='o',color='r',edgecolor='none')
  pl.title('seed '+seed,fontsize=12)
  pl.xlim([0,ampmax])
  pl.ylim([0,ampmax])
  pl.xlabel(r'Horizontal amplitude [$\sigma$]',labelpad=10,fontsize=12)
  pl.ylabel(r'Vertical amplitude [$\sigma$]',labelpad=10,fontsize=12)
def plot_da_vs_turns(data,seed,ampmin=2,ampmax=14,tmax=1.e6,slog=False):
  """dynamic aperture vs number of turns, blue=simple average, red=weighted average"""
  pl.close('seed '+seed)
  pl.figure('seed '+seed,figsize=(6,6))
  pl.errorbar(data['DAs'],data['tlossmin'],xerr=data['DAserr'],fmt='bo',markersize=2,label='simple average')
  pl.plot(data['DAw'],data['tlossmin'],'ro',markersize=3,label='weighted average')
  pl.title('seed '+seed)
  pl.xlim([ampmin,ampmax])
  pl.xlabel(r'Dynamic aperture [$\sigma$]',labelpad=10,fontsize=12)
  pl.ylabel(r'Number of turns',labelpad=15,fontsize=12)
#  pl.legend(loc='best',fontsize='12')
  pl.legend(loc='best')
  if(slog):
    pl.ylim([5.e3,tmax])
    pl.yscale('log')
  else:
    pl.ylim([0,tmax])
    pl.ticklabel_format(style='sci',axis='y',scilimits=(0,0))
def plot_da_vs_turns_comp(data,lbldata,datacomp,lbldatacomp,seed,ampmin=2,ampmax=14,tmax=1.e6,slog=False):
  """dynamic aperture vs number of turns, blue/green=simple average, red/orange=weighted average"""
  pl.close('seed '+seed)
  pl.figure('seed '+seed,figsize=(6,6))
  pl.errorbar(data['DAs'],data['tlossmin'],xerr=data['DAserr'],fmt='bo',markersize=2,label='simple average'+lbldata)
  pl.plot(data['DAw'],data['tlossmin'],'ro',markersize=3,label='weighted average'+lbldata)
  pl.errorbar(datacomp['DAs'],datacomp['tlossmin'],xerr=datacomp['DAserr'],fmt='go',markersize=2,label='simple average'+lbldatacomp)
  pl.plot(datacomp['DAw'],datacomp['tlossmin'],'o',color='orange',markersize=3,label='weighted average'+lbldatacomp)
  pl.title('seed '+seed)
  pl.xlim([ampmin,ampmax])
  pl.xlabel(r'Dynamic aperture [$\sigma$]',labelpad=10,fontsize=12)
  pl.ylabel(r'Number of turns',labelpad=15,fontsize=12)
  pl.legend(loc='best')
#  pl.legend(loc='best',fontsize=12)
  if(slog):
    pl.ylim([5.e3,tmax])
    pl.yscale('log')
  else:
    pl.ylim([0,tmax])
    pl.ticklabel_format(style='sci',axis='y',scilimits=(0,0))

# main analysis - putting the pieces together
def RunDaVsTurns(dbname,createdaout,turnstep,tmax,ampmaxsurv,ampmindavst,ampmaxdavst,plotlog=False,comp=False,compdirname='',lblname='',complblname=''):
'''Da vs turns analysis for study dbname'''
  db=SixDeskDB(dbname)
  study=db.orig_env_var['LHCDescrip']
  if(not glob.glob(study+'-analysis')):
    print('create new directory: '+study+'-analysis')
    os.mkdir(study+'-analysis')
  for seed in db.get_seeds():
    seed=int(seed)
    print('analyzing seed {0}').format(str(seed))
    dirname=study+'-analysis/'+str(seed)
    # case: create DA.out files
    if(createdaout):
      if(not glob.glob(dirname)):
        os.mkdir(dirname)
      #remove all old files
      count=0
      for filename in glob.glob(dirname+'/*'):
        os.remove(filename)
        if(count==0):
          print('remove old files in '+dirname)
        count=count+1
      #load and save the data
      print('get the data ...')
      DAsurv=db.get_surv(seed)
      save_dasurv(DAsurv,dirname)
      DAout=get_da_vs_turns(DAsurv,turnstep)
      save_daout(DAout,dirname)
    # case: reload DA.out files
    else:
      print('reload the data ...')
      DAout = reload_daout(dirname)
      DAsurv= reload_dasurv(dirname)
    print('create the plots ...')
    pl.close('all')
    plot_surv_2d(DAsurv,str(seed),ampmaxsurv)
    pl.savefig(dirname+'/DA.png')
    plot_da_vs_turns(DAout,str(seed),ampmindavst,ampmaxdavst,tmax,plotlog)
    if(plotlog==True):
      pl.savefig(dirname+'/DAsurv_log.png')
    else:
      pl.savefig(dirname+'/DAsurv.png')
    if(comp==True):
      compdirnameseed=compdirname+'/'+str(seed)
      DAoutcomp=reload_daout(compdirnameseed)
      plot_da_vs_turns_comp(DAout,lblname,DAoutcomp,complblname,str(seed),ampmindavst,ampmaxdavst,tmax,plotlog)
      if(plotlog==True):
        pl.savefig(dirname+'/DAsurv_comp_log.png')
      else:
        pl.savefig(dirname+'/DAsurv_comp.png')
  
