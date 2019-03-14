# da vs turns module
import numpy as np
from scipy import optimize
import matplotlib.pyplot as pl
import glob, sys, os, time
from .deskdb import SixDeskDB,tune_dir,mk_dir
import matplotlib
# ------------- basic functions -----------
def get_divisors(n):
  """finds the divisors of an integer number"""
  large_divisors = []
  for i in range(1, int(np.sqrt(n) + 1)):
    if n % i is 0:
      yield i
      if i is not n / i:
        large_divisors.insert(0, n / i)
  for divisor in large_divisors:
    yield divisor

def linear_fit(datx,daty,daterr):
  '''Linear model fit with f(x)=p0+p1*x
  (datx,daty): data, daterr: measurement error
  return values (res,p0,p0err,p1,p1err):
      - res: sum of residuals^2 normalized with the measurment error
      - p0,p1: fit paramaeters
      - p0err, p1err: error of fit parameters'''
  fitfunc = lambda p,x: p[0]+p[1]*x#p[0]=Dinf, p[1]=b0
  errfunc = lambda p,x,y,err: (y-fitfunc(p,x))/err
  pinit = [0.1, 0.1]
  #minimize
  outfit=optimize.leastsq(errfunc, pinit,args=(datx,daty,daterr),full_output=1)
  (p0,p1)=outfit[0]#(p[0],p[1])
  var    =outfit[1]#variance matrix
  p0err  =np.sqrt(var[0,0])#err p[0]
  p1err  =np.sqrt(var[1,1])#err p[1]
#  res=sum((daty-fitfunc((p0,p1),datx))**2)/len(datx-2)    #not weighted with error
  res=sum((errfunc((p0,p1),datx,daty,daterr))**2)/len(datx)#weighted with error
  return (res,p0,p0err,p1,p1err)

# ----------- functions necessary for the analysis -----------

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
  print(nang)
  if((nang not in list(get_divisors(angmax+1))) or ((angmax+1)/nang-1<3)):
    print(('%s is not a divisor of %s or two large (((angmax+1)/nang)-1<3)')%(nang,angmax+1))
    sys.exit(0)
  #define variables for only selection of angles
  s,a,t=data['sigma'][nang::nang+1],data['angle'][nang::nang+1],data['sturn'][nang::nang+1]
  ftype=[('angle',float),('sigma',float),('sturn',float)]
  dataang=np.ndarray(np.shape(a),dtype=ftype)
  dataang['sigma'],dataang['angle'],dataang['sturn']=s,a,t
  return dataang

#import pdb
def compute_da_ue(data, emittances_x, emittances_y, turnstep, regemi, 
                  seed, tunex, tuney, turnsl,
                  simple=True, method='simpson', verbose=True):
    '''
    New version to compute DA for unequal emittances (MT). 
    Code and some comments taken over
    from mk_da_vst function, but output/input structure
    overworked for the purpose of improving. Old code kept for compatibility/checks. 
    Currently only implemented for simpson rule.
    - all unecessary checks and calculations of outdated quantities removed.
 
    INPUT
    -----
    data: see mk_da_vst. Depends on seed & turns.
    emittance_x/y: list of emittances to be computed
    turnstep: step length of turns at which we perform the DA computation
    regemi: Regular emittance which was used in the simulation.
    simple: True: compute DA via simplified formula, False: use 4D formula.
    method: Method of integration. Either 'simpson' or 'trapezoid'.
    '''

    if verbose:
       print ('... calculate da vs turns')
       # print (' --- emittances ---\n x: {}\n y: {}'.format(emittances_x, emittances_y))

    mtime = time.time()
    s, a, t = data['sigma'], data['angle'], data['sturn']
    tmax = np.max(t[s > 0]) # maximum number of turns
    # set the 0 in t to tmax*100 in order to check if turnnumber<it (any(tang[tang<it])<it in get_min_turn_ang)
    t[s==0] = tmax*100
    angmax = len(a[:,0]) # number of angles
    angstep = np.pi/(2*(angmax + 1)) # step in angle in rad

    # Prepare calculations according to user input. This is outsourced from the original big loop in particular
    # to avoid calling (*) and other stuff repeatedly in a loop over the emittances.
    sta = {} # sigma-angle-integration information for all steps.
    for it in np.arange(turnstep, tmax + turnstep, turnstep):
        mta = get_min_turn_ang(s, t, a, it)  # (*)
        mta_angle = mta['angle']*np.pi/180
        l_mta_angle = len(mta_angle)
        mta_sigma = mta['sigma']
        if method == 'simpson':
            if l_mta_angle > 6:
                # define coefficients for simpson rule (simp)
                # ajsimp = [55/24, -1/6, 11/8, 1, .... 1, 11/8, -1/6, 55/24]
                aj_simp_s = np.array([55/24., -1/6., 11/8.]) # Simpson rule
                aj_simp_e = np.array([11/8., -1/6., 55/24.])
                aj = np.concatenate((aj_simp_s, np.ones(l_mta_angle - 6), aj_simp_e))
            else:
                print('ERROR: compute_da_ue - You need at least 7 angles to calculate the da vs turns with the simpson rule! Aborting!!!')
                sys.exit(0)
        if method == 'trapezoid':
            if l_mta_angle > 2:
                # define coefficients for trapezoid rule
                # ajtrap = [3/2, 1, ... 1, 3/2]
                aj_trap_s = np.array([3/2.])
                aj_trap_e = np.array([3/2.])
                aj = np.concatenate((aj_trap_s, np.ones(l_mta_angle - 2), aj_trap_e))
            else:
                print('ERROR: compute_da_ue - You need at least 3 angles to calculate the da vs turns! Aborting!!!')
                sys.exit(0)

        sta[it] = {'angles': mta_angle, 'sigmas': mta_sigma, 'aj': aj, 'tlossmin': np.min(mta['sturn'])}

    ###############################################
    # Initialize sql output. These lists are required due to the connection 
    # of sixdb with SQL databases. I dropped all quantities which are not supported by the unequal emittance
    # option. Furthermore, the various ways of how the DA is computed must be managed by the user via the input/output and will
    # not all be computed at once at it was done in the past, in order to improve calculation speed. Therefore this
    # routine will return a single column called 'da' (and later perhaps 'da_err').
    # This is also for the purpose of avoiding the original glibberish variable names. To be checked later with fitting method.
    ftype = [('seed',int), 
               ('tunex',float), 
               ('tuney',float), 
               ('emitx', float),
               ('emity', float),
               ('turn_max',int),
               ('da',float),
               ('nturn',float),
               ('tlossmin',float), 
               ('nturnavg',float) , 
               ('mtime',float)]
    #l_turnstep = len(np.arange(turnstep, tmax, turnstep))
    #daout = np.ndarray(l_turnstep, dtype=ftype)

    ll = len(sta.keys())*len(emittances_x)*len(emittances_y)
    daout = np.ndarray(ll, dtype=ftype)
    
    for nm in daout.dtype.names:
        #daout[nm] = np.zeros(l_turnstep)
        daout[nm] = np.zeros(ll)
    dawsimp, dassimp, dawsimperr, dassimperr = np.zeros(4)
    dawtrap, dawsimp, dassimp, dawtraperr, dastraperr = np.zeros(5)
    dawsimperr, dassimperr, dastraperrep = np.zeros(3)
    dastraperrepang, dastraperrepamp = np.zeros(2)
    ###############################################

    # iterate over emittances and turn steps
    dacount = 0
    currentdastrap = 0
    currenttlossmin = 0
    #daout = {(emitx, emity): [] for emitx, emity in itertools.product(emittances_x, emittances_y)}
    for it in np.arange(turnstep, tmax + turnstep, turnstep):
        mta_angle = sta[it]['angles']
        mta_sigma = sta[it]['sigmas']
        aj = sta[it]['aj']

        tlossmin = sta[it]['tlossmin']
        nturnavg = (it - turnstep + tlossmin)/2.

        if tlossmin != currenttlossmin: # ??? effect of this condition to be checked - MT
            for emitx, emity in itertools.product(emittances_x, emittances_y): #daout.keys():
                # prepare the arrays for unequal emittances; currently only
                # simple formula implemented.
                # Reference [1]: "Comment on the computation of DA in case of unequal sigmas", M. Giovanozzi, May 4 2017.
                mta_angle_ue = np.arctan((emitx/emity)**0.5*np.tan(mta_angle)) # angle for unequal emittances, Eq. (6) in [1].
                angstep_ue = angstep*(emitx/emity)**0.5*(np.cos(mta_angle_ue)/np.cos(mta_angle))**2 # angular step for unequal emittances, Eq. (7) in [1].
                eR = (emitx/regemi*np.cos(mta_angle_ue)**2 + emity/regemi*np.sin(mta_angle_ue)**2)**0.5 # square root under rsigma in Eq. (8) in [1].
                mta_sigma_ue = mta_sigma/eR # rsigma for unequal emittances, see also Eq. (8) in [1].

                # get the integration steps (difference in angle) in the new coordinate system
                dtheta = np.diff(mta_angle_ue) # get the angular distance of the data points [integration constant]
                dtheta = np.insert(dtheta, 0, mta_angle_ue[0]) # add the first point (distance from x-axis)
                dtheta = np.append(dtheta, (np.pi/2 - mta_angle_ue[-1])) # add the last point  (distance from y-axis)

                # currently only trapezoid formula implemented
                #
                # TO BE CHECKED LATER
                ###dastrap = 2/np.pi*(aj*mta_sigma_ue*angstep_ue).sum()

                # ORIGINAL VERSION
                #### PH: calculate dastrap with a generalized integration rule
                #        baseline is the open formula (4.1.15) in Press et al. NUMERICAL RECIPES in Fortran 77 [second edition]
                #        we generalize the formula for uneven distances between the sampling points
                dastrap_ue = 0
                dastrap_ue += dtheta[0]*mta_sigma_ue[0] # corner element from left  [open formula for trapezoidal rule]
                dastrap_ue += dtheta[-1]*mta_sigma_ue[-1] # corner element from right [open formula for trapezoidal rule]
                for i in range(1, len(dtheta) - 1):
                    dastrap_ue += (0.5)*( mta_sigma_ue[i] + mta_sigma_ue[i-1] )*dtheta[i]
                dastrap_ue = (2./np.pi)*dastrap_ue
                dastrap = dastrap_ue # preliminary, change dastrap variable before merging with main branch#
                ##daout[(emitx, emity)].append(dastrap)

                if (dastrap != currentdastrap or it == tmax) and dastrap > 0:  # ??? effect of first condition to be checked - MT
                    daout[dacount] = (seed, tunex, tuney, emitx, emity, turnsl, dastrap, 
                                      it - turnstep, tlossmin, nturnavg, mtime)
                    dacount += 1
                currentdastrap = dastrap
        currenttlossmin = tlossmin

    return daout[daout['da'] > 0]
    #return daout


#@profile
def mk_da_vst(data, seed, tune, turnsl, turnstep, emitx, emity, regemi, 
              verbose=True):
  """returns 'seed','tunex','tuney','dawtrap','dastrap','dawsimp','dassimp',
             'dawtraperr','dastraperr','dastraperrep','dastraperrepang',
             'dastraperrepamp','dawsimperr','dassimperr','nturn','tlossmin',
             'mtime'
  the da is in steps of turnstep
  das:       integral over radius
             das = 2/pi*int_0^(2pi)[r(theta)]dtheta=<r(theta)>
                 = 2/pi*dtheta*sum(a_i*r(theta_i))
  daw:       integral over phase space
             daw = (int_0^(2pi)[(r(theta))^4*sin(2*theta)]dtheta)^1/4
                 = (dtheta*sum(a_i*r(theta_i)^4*sin(2*theta_i)))^1/4
  trapezoidal rule (trap):  a_i=(3/2,1, ... ,1,3/2)
  simpson rule     (simp):  a_i=(55/24.,-1/6.,11/8.,1, ... 1,11/8.,-1/6.,55/24.)
                            numerical recipes open formulas 4.1.15 and 4.1.18
  """
  if verbose:
    print('... calculate da vs turns')

  mtime = time.time()
  (tunex, tuney) = tune
  s, a, t = data['sigma'], data['angle'], data['sturn']
  tmax = np.max(t[s > 0]) # maximum number of turns
  # set the 0 in t to tmax*100 in order to check if turnnumber<it (any(tang[tang<it])<it in get_min_turn_ang)
  t[s==0] = tmax*100
  angmax = len(a[:,0]) # number of angles
  angstep = np.pi/(2*(angmax + 1)) # step in angle in rad
  ampstep = np.abs((s[s > 0][1]) - (s[s > 0][0]))
  ftype = [('seed',int),('tunex',float),('tuney',float),('turn_max',int),('dawtrap',float),('dastrap',float),
    ('dawsimp',float),('dassimp',float),('dawtraperr',float),('dastraperr',float),('dastraperrep',float),
    ('dastraperrepang',float),('dastraperrepamp',float),('dawsimperr',float),('dassimperr',float),
    ('nturn',float),('tlossmin',float),('nturnavg',float),('mtime',float)]

  l_turnstep = len(np.arange(turnstep, tmax, turnstep))
  daout = np.ndarray(l_turnstep, dtype=ftype)
  for nm in daout.dtype.names:
    daout[nm] = np.zeros(l_turnstep)

  dacount = 0
  currentdastrap = 0
  currenttlossmin = 0
  # define integration coefficients at beginning and end which are unequal to 1
  ajtrap_s = np.array([3/2.]) # Trapezoid rule
  ajtrap_e = np.array([3/2.])
  ajsimp_s = np.array([55/24., -1/6., 11/8.]) # Simpson rule
  ajsimp_e = np.array([11/8., -1/6., 55/24.])
  warnsimp = True

  for it in np.arange(turnstep, tmax + turnstep, turnstep):
    mta = get_min_turn_ang(s, t, a, it)
    mta_angle = mta['angle']*np.pi/180 # convert to rad
    l_mta_angle = len(mta_angle)
    mta_sigma = mta['sigma']
    if(l_mta_angle > 2):
      # define coefficients for trapezoid rule
      # ajtrap = [3/2, 1, ... 1, 3/2]
      ajtrap = np.concatenate((ajtrap_s, np.ones(l_mta_angle - 2), ajtrap_e))
    else:
      print('WARNING! mk_da_vst - You need at least 3 angles to calculate the da vs turns! Aborting!!!')
      sys.exit(0)
    if(l_mta_angle > 6):
      # define coefficients for simpson rule (simp)
      # ajsimp = [55/24, -1/6, 11/8, 1, .... 1, 11/8, -1/6, 55/24]
      ajsimp = np.concatenate((ajsimp_s, np.ones(l_mta_angle - 6), ajsimp_e))
      calcsimp = True
    else:
      if(warnsimp):
        print('WARNING! mk_da_vst - You need at least 7 angles to calculate the da vs turns with the simpson rule! da*simp* will be set to 0.')
        warnsimp = False
      calcsimp = False

    # prepare the arrays for unequal emittances
    # Reference [1]: "Comment on the computation of DA in case of unequal sigmas", M. Giovanozzi, May 4 2017.
    mta_angle_ue = np.arctan((emitx/emity)**0.5*np.tan(mta_angle)) # angle for unequal emittances, Eq. (6) in [1].
    angstep_ue = angstep*(emitx/emity)**0.5*(np.cos(mta_angle_ue)/np.cos(mta_angle))**2 # angular step for unequal emittances, Eq. (7) in [1].
    eR = (emitx/regemi*np.cos(mta_angle_ue)**2 + emity/regemi*np.sin(mta_angle_ue)**2)**0.5 # square root under rsigma in Eq. (8) in [1].
    mta_sigma_ue = mta_sigma/eR # rsigma for unequal emittances, see also Eq. (8) in [1].

    # the next quantity is not used and therefore commented out - MT
    ## ampstep_ue = eR*ampstep - (emitx - emity)/regemi*np.cos(mta_angle_ue)*np.sin(mta_angle_ue)/eR**2*mta_sigma_ue*angstep_ue # amplitude step with unequal emittances
    # ampstep_ue = ampstep/eR + (emitx - emity)/regemi*np.cos(mta_angle_ue)*np.sin(mta_angle_ue)/eR**2*mta_sigma_ue*angstep_ue # bugfix of previous line - MT

    # get the integration steps (difference in angle) in the new coordinate system
    dtheta = np.diff(mta_angle_ue) # get the angular distance of the data points [integration constant]
    dtheta = np.insert(dtheta, 0, mta_angle_ue[0]) # add the first point (distance from x-axis)
    dtheta = np.append(dtheta, (np.pi/2 - mta_angle_ue[-1])) # add the last point  (distance from y-axis)

    # ---- trapezoidal rule (trap)
    # integral
    dawtrapint = ((ajtrap*mta_sigma**4*np.sin(2*mta_angle)).sum())*angstep # old
    dawtrap = (dawtrapint)**(1/4.) # old

    #### PH: calculate dastrap with a generalized integration rule
    #        baseline is the open formula (4.1.15) in Press et al. NUMERICAL RECIPES in Fortran 77 [second edition]
    #        we generalize the formula for uneven distances between the sampling points
    dastrap_ue = 0
    dastrap_ue += dtheta[0]*mta_sigma_ue[0]   # corner element from left  [open formula for trapezoidal rule]
    dastrap_ue += dtheta[-1]*mta_sigma_ue[-1] # corner element from right [open formula for trapezoidal rule]
    for i in range(1, len(dtheta) - 1):
        dastrap_ue += (0.5)*( mta_sigma_ue[i] + mta_sigma_ue[i-1] )*dtheta[i]
    dastrap_ue = (2./np.pi)*dastrap_ue
    dastrap = dastrap_ue # preliminary, change dastrap variable before merging with main branch

    # error
    dawtraperrint = np.abs(((ajtrap*(2*(mta_sigma**3)*np.sin(2*mta_angle))).sum())*angstep*ampstep) # old
    dawtraperr = np.abs(1/4.*dawtrapint**(-3/4.))*dawtraperrint

    dastraperr = ampstep/2 # old
    dastraperrepang = ((np.abs(np.diff(mta_sigma))).sum())/(2*(angmax+1)) # PH: bugfix angmax -> angmax+1

    # dastraperrepamp...
    # da: dynamic aperture
    # s: simple
    # trap: trapezoid
    # err: error
    # ep: ???
    # ang: angular
    # --> these variable names should be fixed - MT
    dastraperrepamp = ampstep/2
    dastraperrep = np.sqrt(dastraperrepang**2 + dastraperrepamp**2)
    # ---- simpson rule (simp)
    if(calcsimp):
      # int
      dawsimpint = (ajsimp*((mta_sigma**4)*np.sin(2*mta_angle))).sum()*angstep # old
      dawsimp = (dawsimpint)**(1/4.)
      dassimpint = (ajsimp*mta_sigma).sum()*angstep
      dassimp = (2./np.pi)*dassimpint

      # error
      dawsimperrint = (ajsimp*(2*(mta_sigma**3)*np.sin(2*mta_angle))).sum()*angstep*ampstep # old
      dawsimperr = np.abs(1/4.*dawsimpint**(-3/4.))*dawsimperrint
      dassimperr = ampstep/2 # simplified
      # print "DAWSIMPERR, DASSIMPERR, OLD", dawsimperr, dassimperr

    else:
      (dawsimp, dassimp, dawsimperr, dassimperr) = np.zeros(4)
    tlossmin = np.min(mta['sturn'])
    nturnavg = (it-turnstep + tlossmin)/2.
    if (dastrap != currentdastrap and it-turnstep >= 0 and tlossmin != currenttlossmin) or (it == tmax):  # conditions? to be checked - MT
      if emitx != regemi or emity != regemi:
        # dawtrap = 0.
        dawtrap, dawsimp, dassimp, dawtraperr, dastraperr = np.zeros(5)
        dawsimperr, dassimperr = np.zeros(2)

      daout[dacount] = (seed, tunex, tuney, turnsl, dawtrap, dastrap, dawsimp, dassimp,
                        dawtraperr, dastraperr, dastraperrep, dastraperrepang, dastraperrepamp, 
                        dawsimperr, dassimperr, it-turnstep, tlossmin, nturnavg, mtime)
      dacount += 1
    currentdastrap = dastrap
    currenttlossmin = tlossmin
  if emitx != regemi or emity != regemi:
    return daout[daout['dastrap'] > 0]
  else:
    return daout[daout['dawtrap'] > 0] # delete 0 from errors

# ----------- functions to calculat the fit -----------
def get_fit_data(data, fitdat, fitdaterr, fitndrop, fitkap, b1):
    '''linearize data for da vs turns fit according to model:
          D(N) = Dinf + b0/(log(N^(exp(-b1))))^kappa'''
    datx = 1/(np.log(data['tlossmin'][fitndrop::]**np.exp(-b1))**fitkap)
    #  print (fitdat,fitdaterr)
    daty = data[fitdat][fitndrop::]
    if fitdaterr == 'none': # case of no errors
        daterr = np.ones(len(datx))
    else:
        daterr = data[fitdaterr][fitndrop::]
    return datx, daty, daterr

def get_b1mean(db,tune,fitdat,fitdaterr,fitndrop,fitskap,fitekap,fitdkap, verbose=True):
  '''returns (mean(b1),errmean(b1),std(b1)) over the seeds
  with b1 being the fit parameter in:
        D(N) = Dinf+b0/(log(N^(exp(-b1))))^kappa
  and a linear relation is assumed between:
        log(|b|)=log(|b0|)+b1*kappa <=> b=b0*exp(b1*kappa)
  with b being the fit paramter in:
        D(N) = Dinf+b/(log(N))^kappa
  fitndrop=do not include first fitndrop data points
  fitkap=kappa'''
  if verbose:
    print('calculating b1mean ...')

  if(not db.check_seeds()):
    print('!!! Seeds are missing in database !!!')
  ftype=[('seed',int),('res',float),('logb0',float),('logb0err',float),('b1',float),('b1err',float)]
  lklog=np.zeros(len(db.get_db_seeds()),dtype=ftype)
  ftype=[('kappa',float),('res',float),('dinf',float),('dinferr',float),('b',float),('berr',float)]
  lkap=np.zeros(len(np.arange(fitskap,fitekap+fitdkap,fitdkap))-1,dtype=ftype)
  ccs=0
  for seed in db.get_db_seeds():
    data=db.get_da_vst(seed, tune)
    #start: scan over kappa
    cck=0
    for kap in np.arange(fitskap,fitekap+fitdkap,fitdkap):
      if(abs(kap)>1.e-6):#for kappa=0: D(N)=Dinf+b/(log(N)^kappa)=D(N)=Dinf+b -> fit does not make sense
        datx,daty,daterr=get_fit_data(data,fitdat,fitdaterr,fitndrop,kap,0)#fit D(N)=Dinf+b/(log(N)^kappa
        lkap[cck]=(kap,)+linear_fit(datx,daty,daterr)
        cck+=1
    lklog[ccs]=(seed,)+linear_fit(lkap['kappa'],np.log(np.abs(lkap['b'])),1)#linear fit log(|b|)=log(|b0|)+b1*kappa for each seed
    ccs+=1
  return (np.mean(lklog['b1']),np.sqrt(np.mean(lklog['b1err']**2)),np.std(lklog['b1']))#error of mean value = sqrt(sum_i((1/n)*sigma_i**2))

def mk_da_vst_fit(db,tune,fitdat,fitdaterr,fitndrop,fitskap,fitekap,fitdkap, verbose=True):
  '''1) a) fit D(N)=Dinf+b/(log(N))^kappa for all seeds and
           scan range (skap,ekap,dkap)
        b) assume linear dependence of b on kappa:
             log(|b|)=log(|b0|)+b1*kappa
           -> b1 for all seeds
        c) calculate avg(b1) over all seeds
     2) a) fit D(N)=Dinf+b0/(log(N)^(exp(-b1)))^kappa
           for fixed b1=b1mean (obtained in 1))
           and scan range (skap,ekap,dkap)
        b) use (b0,kappa) with minimum residual'''
  if verbose:
    print('fit da vs turns for tune {0} ...'.format(str(tune)))

  turnsl = db.env_var['turnsl']
  mtime = time.time()
  (tunex, tuney) = tune
  (b1mean, b1meanerr, b1std) = get_b1mean(db, tune, fitdat, fitdaterr, fitndrop, fitskap, 
                                          fitekap, fitdkap, verbose=verbose)

  if verbose:
    print(('average over %s seeds: b1mean=%s, b1meanerr=%s, b1std=%s'%(round(len(db.get_db_seeds())), 
          round(b1mean,3), round(b1meanerr, 3), round(b1std, 3))))
    print(('start scan over kappa for fixed b1=%s to find kappa with minimum residual ...'%b1mean))
  ftype = [('kappa',float), ('dkappa',float), ('res',float), ('dinf',float), ('dinferr',float), ('b0',float), ('b0err',float)]
  lkap = np.zeros(len(np.arange(fitskap, fitekap + fitdkap, fitdkap)) - 1, dtype=ftype) # -1 as kappa=0 is not used
  ftype = [('seed',float),('tunex',float),('tuney',float),('turn_max',int),('fitdat',np.str_, 30),
  ('fitdaterr',np.str_, 30),('fitndrop',float),('kappa',float),('dkappa',float),('res',float),
  ('dinf',float),('dinferr',float),('b0',float),('b0err',float),('b1mean',float),('b1meanerr',float),
  ('b1std',float),('mtime',float)]

  seeds = db.get_db_seeds()
  minkap = np.zeros(len(seeds), dtype=ftype)
  ccs = 0
  for seed in seeds:
    data = db.get_da_vst(seed, tune)
    # start: scan over kappa
    cck = 0
    for kap in np.arange(fitskap, fitekap + fitdkap, fitdkap):
      if(abs(kap) > 1.e-6): # for kappa=0: D(N)=Dinf+b/(log(N)^kappa)=D(N)=Dinf+b -> fit does not make sense
        datx, daty, daterr = get_fit_data(data, fitdat, fitdaterr, fitndrop, kap, b1mean)
        lkap[cck] = (kap, fitdkap,) + linear_fit(datx, daty, daterr)
        cck += 1
    iminkap = np.argmin(lkap['res'])
    minkap[ccs] = (seed, tunex, tuney, turnsl, fitdat, fitdaterr, fitndrop,) + \
                   tuple(lkap[iminkap]) + (b1mean, b1meanerr, b1std, mtime,)
    ccs += 1
  if verbose:
    print('... scan over kappa is finished!')
  return minkap

# ----------- functions to reload and create DA.out files for previous scripts -----------
def save_daout_old(data, filename, verbose=True):
    if verbose:
        print('... save da vs turns (old data format) data in {0}'.format(filename))
    daoutold = data[['dawtrap', 'dastrap', 'dastraperrep', 'dastraperrepang', 'dastraperrepamp', 'nturn', 'tlossmin']]
    np.savetxt(filename, daoutold, fmt='%.6f %.6f %.6f %.6f %.6f %d %d')

def reload_daout_old(filename):
    ftype=[('dawtrap',float),('dastrap',float),('dastraperrep',float),('dastraperrepang',float),('dastraperrepamp',float),('nturn',float),('tlossmin',float)]
    return np.loadtxt(filename,dtype=ftype,delimiter=' ')

def save_daout(data, filename, verbose=True):
    if verbose:
        print('... saving da vs turns data in {0}'.format(filename))
    daout = data[['seed', 'tunex', 'tuney', 'turn_max', 'dawtrap', 'dastrap',
                'dawsimp', 'dassimp', 'dawtraperr', 'dastraperr', 'dastraperrep',
                'dastraperrepang', 'dastraperrepamp', 'dawsimperr', 'dassimperr',
                'nturn', 'tlossmin']]
    np.savetxt(filename, daout, fmt='%d %.6f %.6f %d %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %.6f %d %d')

def save_davst_fit(data, filename, verbose=True):
    if verbose:
        print('... saving da vs turns fit data in {0}'.format(filename))
    fitdata = data[['seed','tunex','tuney','turn_max','fitdat','fitdaterr','fitndrop','kappa','dkappa','res','dinf','dinferr','b0','b0err','b1mean','b1meanerr','b1std']]
    np.savetxt(filename, fitdata, fmt='%d %.5f %.5f %d %s %s %d %.5f %.5f %.5f %.5f %.5f %.5f %.5f %.5f %.5f %.5f')

def reload_daout(filename):
    ftype=[('seed',int),('tunex',float),('tuney',float),('turn_max',int),('dawtrap',float),('dastrap',float),('dawsimp',float),('dassimp',float),('dawtraperr',float),('dastraperr',float),('dastraperrep',float),('dastraperrepang',float),('dastraperrepamp',float),('dawsimperr',float),('dassimperr',float),('nturn',float),('tlossmin',float),('mtime',float)]
    return np.loadtxt(filename,dtype=ftype,delimiter=' ')

def save_dasurv(data, filename, verbose=True):
    if verbose:
        print('... save survival data in {0}'.format(filename))
    np.savetxt(filename, np.reshape(data, -1), fmt='%.8f %.8f %d')

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
def plot_comp_da_vst(db,dbcomp,ldat,ldaterr,lblname,complblname,seed,tune,ampmin,ampmax,tmax,slog,sfit,fitndrop):
  """plot dynamic aperture vs number of turns,
  blue/green=simple average, red/orange=weighted average"""
  pl.close('all')
  pl.figure(figsize=(6,6))
  for dbbb in [db,dbcomp]:
    data=dbbb.get_da_vst(seed,tune)
    if(dbbb.LHCDescrip==db.LHCDescrip):
      lbl   = lblname
      fmtpl = 'bo'
      fmtfit= 'b-'
    if(dbbb.LHCDescrip==dbcomp.LHCDescrip):
      lbl    = complblname
      fmtpl  = 'ro'
      fmtfit = 'r-'
#    pl.errorbar(data[ldat[0]],data['tlossmin'],xerr=data[ldaterr[0]],fmt=fmtpl,markersize=2,label='%s %s'%(ldat[0],lbl))
    pl.errorbar(data[ldat[0]],data['tlossmin'],xerr=data[ldaterr[0]],fmt=fmtpl,markersize=2,label='%s'%(lbl))
    if(sfit):
      fitdata=dbbb.get_da_vst_fit(seed,tune)
      fitdata=fitdata[fitdata['fitdat']==ldat[0]]
      fitdata=fitdata[fitdata['fitdaterr']==ldaterr[0]]
      fitdata=fitdata[np.abs(fitdata['fitndrop']-float(fitndrop))<1.e-6]
      if(len(fitdata)==1):
        pl.plot(fitdata['dinf']+fitdata['b0']/(np.log(data['tlossmin']**np.exp(-fitdata['b1mean']))**fitdata['kappa']),data['tlossmin'],fmtfit)
      else:
        print('Warning: no fit data available or data ambigious!')
  pl.title('seed '+str(seed),fontsize=16)
  pl.xlim([ampmin,ampmax])
  pl.xlabel(r'Dynamic aperture [$\sigma$]',labelpad=10,fontsize=16)
  pl.ylabel(r'Number of turns',labelpad=15,fontsize=16)
  plleg=pl.gca().legend(loc='best',fontsize=16)
  for label in plleg.get_texts():
    label.set_fontsize(12)
  if(slog):
    pl.ylim([5.e3,tmax])
    pl.yscale('log')
  else:
    pl.ylim([0,tmax])
    pl.gca().ticklabel_format(style='sci',axis='y',scilimits=(0,0))

def clean_dir_da_vst(db, files, verbose=True):
  '''create directory structure and delete old files of da_vs_turns analysis'''
  if(len(files) > 0) and verbose:
    print(('remove old {0} ... files in ' + db.LHCDescrip).format(files))

  for seed in db.get_seeds():
    for tune in db.get_db_tunes():
      pp = db.mk_analysis_dir(seed, tune) # create directory
      if(len(files) > 0): # delete old plots and files
        for filename in files:
          ppf = os.path.join(pp, filename)
          if(os.path.exists(ppf)): os.remove(ppf)


# for error analysis - data is not saved in database but output files are generated
def RunDaVsTurnsAng(db,seed,tune,turnstep):
  """Da vs turns -- calculate da vs turns for divisors of angmax,
     e.g. for angmax=29+1 for divisors [1, 2, 3, 5, 6] - last 2 [10,15] are omitted as the number of angles has to be larger than 3"""
  # start analysis
  try:
    turnstep=int(float(turnstep))
  except [ValueError,NameError,TypeError]:
    print('Error in RunDaVsTurns: turnstep must be integer values!')
    sys.exit(0)
  if(seed not in db.get_db_seeds()):
    print(('WARNING: Seed %s is missing in database !!!'%seed))
    sys.exit(0)
  if(tune not in db.get_db_tunes()):
    print(('WARNING: tune %s is missing in database !!!'%tune))
    sys.exit(0)
  turnsl=db.env_var['turnsl']#get turnsl for outputfile names
  seed=int(seed)
  print(('analyzing seed {0} and tune {1}...').format(str(seed),str(tune)))
  dirname=db.mk_analysis_dir(seed,tune)#directory struct already created in clean_dir_da_vst, only get dir name (string) here
  print('... get survival data')
  dasurvtot= db.get_surv(seed,tune)
  a=dasurvtot['angle']
  angmax=len(a[:,0])#number of angles
  #use only divisors nang with (angmax+1)/nang-1>=3 = minimum number of angles for trapezoidal rule
  divsall=np.array(list(get_divisors(angmax+1)))
  divs=divsall[(angmax+1)/divsall-1>2]
  print(('... number of angles: %s, divisors: %s'%(angmax,str(divs))))
  for nang in divs:
    dirnameang='%s/%s'%(dirname,nang)
    mk_dir(dirnameang)
    dasurv=select_ang_surv(dasurvtot,seed,nang)
    print('... calculate da vs turns')
    daout=mk_da_vst(dasurv,seed,tune,turnsl,turnstep)
    save_daout(daout,dirnameang)
    print(('... save da vs turns data in {0}/DA.out').format(dirnameang))


import itertools

import pdb

# in analysis - putting the pieces together
def RunDaVsTurns(db,force,outfile,outfileold,turnstep,davstfit,fitdat,
                 fitdaterr,fitndrop,fitskap,fitekap,fitdkap,outfilefit,
                 emitx=None, emity=None,
                 verbose=True, method=0):
    '''Da vs turns -- calculate da vs turns for study dbname, if davstfit=True also fit the data'''

    try:
        turnstep = int(float(turnstep))
    except [ValueError,NameError,TypeError]:
        print('Error in RunDaVsTurns: turnstep must be integer!')
        sys.exit(0)
    if(not db.check_seeds()):
        print('!!! Seeds are missing in database !!!')
    turnsl = db.env_var['turnsl'] # get turnsl for outputfile names
    turnse = db.env_var['turnse']
    regemi = db.env_var['emit'] # get the emittance at which the simulation was carried out

    if emitx is None: # if the unequal emittance option is not used
        emitx, emity = regemi, regemi

    ## # filenames in case they have to be updated
    ## files0 = ['DA.out', 'DAsurv.out', 'DA.png', 'DAsurv.png',
    ##          'DAsurv_log.png', 'DAsurv_comp.png', 'DAsurv_comp_log.png']
    ## files = files0[:]
    ## for f in files0:
    ##   files.append(f[:-4] + '.{}'.format(turnse) + f[-4:])

    if verbose:
        print('REGEMI  =', regemi)
        print('EMITX   =', emitx)
        print('EMITY   =', emity)
        if (emitx != regemi or emity != regemi):
            print("WARNING: you are using unequal emittances! So far this option is only computing"\
                   + " the correct value of dastrap. Take this into account in your analysis!")

    seeds = db.get_db_seeds()
    tunes = db.get_db_tunes()
    if isinstance(emitx, float):
        emitx = [emitx]
    if isinstance(emity, float):
        emity = [emity]

    if method == 0:
        daout = {(ex, ey): [] for ex, ey in itertools.product(emitx, emity)}
    if method == 1: 
        daout = []

    #seeds = seeds[0:4]
    for seed in seeds:
        seed = int(seed)
        if verbose:
            print('analyzing seed {0} ...'.format(seed))

        for tune in tunes:
            if verbose:
                print('analyzing tune {0} ...'.format(tune))
            dasurv = db.get_surv(seed, tune, verbose=verbose)

            if dasurv is None:
                if verbose:
                    print("ERROR: survival data could not be retrieved due to "+
                          "and error in the database or tracking data. Skipping "
                          "this case: (seed, tune) = ({}, {})".format(seed, tune))
                break

            # now compute DA depending on user-given method
            if verbose:
                start = time.time()

            if method == 0:
                for ex, ey in itertools.product(emitx, emity):
                    if verbose:
                        print ('calculating da vs turns for (ex, ey) = ({}, {})'.format(ex, ey))

                    daout[(ex, ey)].append(mk_da_vst(dasurv, seed, tune, turnsl, turnstep, ex, ey, regemi, verbose=False))

            # loop inside new DA method
            if method == 1:
                #daout = compute_da_ue(dasurv, emitx, emity, turnstep, regemi, verbose=verbose)
                daout.append(compute_da_ue(dasurv, emitx, emity, turnstep, regemi, 
                                           seed, tune[0], tune[1], turnsl,
                                           verbose=verbose))

            if verbose:
                end = time.time()
                print ('T: {:.8f} [s]'.format(end - start))



    if verbose:
        print ('flattening {} cases & storing results to SQL database ...'.format(len(daout)))
        

    if method == 0:
        for exey in daout.keys():
            column_name = 'da_vst_{}_{}'.format(exey[0], exey[1]).replace('.', '')  # n.b. sql does not like '.' in column names.
            db.store_to_sql_database(np.hstack(daout[exey]), name=column_name, recreate=True, verbose=verbose)
    if method == 1:
        db.store_to_sql_database(np.hstack(daout), name='da_vst', recreate=True, verbose=verbose)
    
        #pdb.set_trace()
        #db.store_to_sql_database(np.array(daout[exey], dtype=[(column_name, float)]), name=column_name, 
        #                                  recreate=True, keys=[column_name], verbose=verbose)
 

'''
    if outfile or outfileold: # create dasurv.out and da.out files
                dirname = db.mk_analysis_dir(seed, tune) # directory struct already created in clean_dir_da_vst, only get dir name (string) here
                daout_seed_tune = daout[(ex, ey)]
                if outfile:
                    save_dasurv(data=dasurv, filename='%s/DAsurv.%s.out'%(dirname, turnse), verbose=verbose)        
                    save_daout(daout_seed_tune, filename='%s/DA.%s.out'%(dirname, turnse), verbose=verbose)
                if outfileold:
                    save_daout_old(daout_seed_tune, filename='%s/DAold.%s.out'%(dirname, turnse), verbose=verbose)

        # MT: ToDo: check the original cleaning part in this script

    recreate = False
    db.store_to_sql_database(daout_flat, name='da_vst', recreate=recreate, verbose=verbose, fit=True)
        
    if(davstfit):
        # --- fit the data ---
        valid_fit_keys = ['dawtrap', 'dastrap', 'dawsimp', 'dassimp']
        valid_fit_error_keys = ['none', 'dawtraperr', 'dastraperr', 'dastraperrep', 
                                'dastraperrepang', 'dastraperrepamp', 'dawsimperr', 
                                'dassimperr']
        # --- check user input ---
        if fitdat not in valid_fit_keys:
            print("Error in -fitopt: <data> has to be in {} - Aborting!".format(valid_fit_keys))
            sys.exit(0)
        if fitdaterr not in valid_fit_error_keys:
            print("Error in -fitopt: <dataerr> has to be in {} - Aborting!".format(valid_fit_error_keys))
            sys.exit(0)
        try:
            fitndrop = int(float(fitndrop))
        except [ValueError, NameError, TypeError]:
            print('Error in RunDaVsTurns: fitndrop must be integer! - Aborting!')
            sys.exit(0)
        try:
            fitskap = float(fitskap)
            fitekap = float(fitekap)
            fitdkap = float(fitdkap)
        except [ValueError, NameError, TypeError]:
            print('Error in RunDaVsTurns: fitskap, fitekap and fitdkap must be float! - Aborting!')
            sys.exit(0)
        # ------------------------

        if np.arange(fitskap, fitekap + fitdkap, fitdkap).any():
            for tune in db.get_db_tunes():
                fitdaout = mk_da_vst_fit(db, tune, fitdat, fitdaterr, fitndrop,
                                 fitskap, fitekap, fitdkap, verbose=verbose)
                db.store_to_sql_database(fitdaout, name='da_vst_fit', recreate=False, verbose=verbose)
            if(outfilefit):
                (tunex, tuney) = tune
                sixdesktunes = "%g_%g"%(tunex, tuney)
                fndot = '%s/DAfit.%s.%s.%s.%s.%s.plot'%(db.mk_analysis_dir(), db.LHCDescrip, sixdesktunes,
                                                      turnse, fitdat, fitdaterr)
                save_davst_fit(fitdaout, fndot, verbose=verbose)
        else:
            if verbose:
                print('Error in RunDaVsTurns: empty scan range for fitkap!')
'''


def PlotFMA(db,args=[]):
  for tunes in db.get_tunes():
    for seed in db.get_seeds():
      turnse='e'+ str(db.env_var['turnse'])
      method_inputfile = db.get_db_fma_inputfile_method()
      dirname=db.mk_analysis_dir(seed,tunes)
      pl.close('all')
      ##### If only one inputfile is detected or defined by user 
      if ( (len(method_inputfile)==1) or (len(args) == 2) ) :
        if  (len(method_inputfile)==1):
          method = method_inputfile[0][1]
          db.plot_fma_footprint(seed,tunes,turnse,method_inputfile[0][0],method_inputfile[0][1])
        else:
          method = args[1]
          db.plot_fma_footprint(seed,tunes,turnse, args[0],args[1])
        print(('... saving plot %s/Footprint.%s.%s.png'%(dirname,turnse,method)))
        pl.savefig('%s/Footprint.%s.%s.png'%(dirname,turnse,method))
      else:
        ##### If inputfiles have been defined manually 
        if (len(args) == 4):
          it = iter(args)
          n_args = list(zip(it, it))
          ##### Find other files with the same method
          results = [t for t in n_args if t[1] == n_args[0][1]]
        ##### If inputfiles have been detected from fma_sixtrack.gz
        elif not args:
          results = [t for t in method_inputfile if t[1] == method_inputfile[0][1]]
        if (len(results)==1):
          print("Only one file with %s method has been found!You need at least two files with the same method to compute tune diffusion."%(results[0][1]))
          return -1
        method = results[0][1]
        db.plot_fma_scatter(seed,tunes,turnse,results)
        print(('... saving plot %s/FMA_config.%s.%s.png'%(dirname,turnse,method)))
        pl.savefig('%s/FMA_config.%s.%s.png'%(dirname,turnse,method))
        pl.close('all')
        db.plot_fma_scatter(seed,tunes,turnse,results, var1 ='q1' ,var2 ='q2' ,dqlim=5e-3)
        db.plot_res_upto_order(15)
        print(('... saving plot %s/FMA.%s.%s.png'%(dirname,turnse,method)))
        pl.savefig('%s/FMA.%s.%s.png'%(dirname,turnse,method))

def PlotGrid(db,args=[]):
  for tunes in db.get_tunes():
    for seed in db.get_seeds():
      turnse='e'+ str(db.env_var['turnse'])
      method_inputfile = db.get_db_fma_inputfile_method()
      dirname=db.mk_analysis_dir(seed,tunes)
      pl.close('all')
      ##### If one inputfile is detected or defined by user 
      if ( (len(method_inputfile)==1) or (len(args) == 2) ) :
        if  (len(method_inputfile)==1):
          method = method_inputfile[0][1]
          db.plot_fma_footprint(seed,tunes,turnse,method_inputfile[0][0],method_inputfile[0][1],grid=True)
        else:
          method = args[1]
          db.plot_fma_footprint(seed,tunes,turnse, args[0],args[1], grid=True)
        print(('... saving plot %s/Grid.%s.%s.png'%(dirname,turnse,method)))
        pl.savefig('%s/Grid.%s.%s.png'%(dirname,turnse,method))
      elif (len(args)<2):
        print("More than 1 inputfiles have been detected. The first one will be used")
        method = method_inputfile[0][1]
        db.plot_fma_footprint(seed,tunes,turnse,method_inputfile[0][0],method_inputfile[0][1],grid=True)
        print(('... saving plot %s/Grid.%s.%s.png'%(dirname,turnse,method)))
        pl.savefig('%s/Grid.%s.%s.png'%(dirname,turnse,method))
      else:  
        print("You have manually defined more than one file.Please choose one method/inputfile or the first method/inputfile found will be used automatically")
        return -1

def PlotDaVsTurns(db,ldat,ldaterr,ampmaxsurv,ampmindavst,ampmaxdavst,tmax,plotlog,plotfit,fitndrop):
  '''plot survival plots and da vs turns for list of data ldat and associated error ldaterr'''
  turnsl=db.env_var['turnsl']
  turnse=db.env_var['turnse']
  print('Da vs turns -- create survival and da vs turns plots')
  try:
    ampmaxsurv =float(ampmaxsurv)
    ampmindavst=float(ampmindavst)
    ampmaxdavst=float(ampmaxdavst)
  except [ValueError,NameError,TypeError]:
    print('Error in PlotDaVsTurns: ampmaxsurv and amprangedavst must be float!')
    sys.exit(0)
  #remove all files
  if(plotlog):
    files=('DA_log.png DAsurv.png DA_log.%s.png DAsurv.%s.png'%(turnse,turnse)).split()
  else:
    files=('DA.png DAsurv.png DA.%s.png DAsurv.%s.png'%(turnse,turnse)).split()
  clean_dir_da_vst(db,files)# create directory structure and delete old files if force=true
  if(not db.check_seeds()):
    print('!!! Seeds are missing in database !!!')
  for seed in db.get_db_seeds():
    seed=int(seed)
    for tune in db.get_db_tunes():
      dirname=db.mk_analysis_dir(seed,tune)#directory struct already created in clean_dir_da_vst, only get dir name (string) here
      pl.close('all')
      pl.figure(figsize=(6,6))
      db.plot_surv_2d(seed,tune,ampmaxsurv)#suvival plot
      pl.savefig('%s/DAsurv.%s.png'%(dirname,turnse))
      print(('... saving plot %s/DAsurv.%s.png'%(dirname,turnse)))
      db.plot_da_vst(seed,tune,ldat,ldaterr,ampmindavst,ampmaxdavst,tmax,plotlog,plotfit,fitndrop)#da vs turns plot
      if(plotlog==True):
        pl.savefig('%s/DA_log.%s.png'%(dirname,turnse))
        print(('... saving plot %s/DA_log.%s.png'%(dirname,turnse)))
      else:
        pl.savefig('%s/DA.%s.png'%(dirname,turnse))
        print(('... saving plot %s/DA.%s.png'%(dirname,turnse)))

def PlotCompDaVsTurns(db,dbcomp,ldat,ldaterr,lblname,complblname,ampmaxsurv,ampmindavst,ampmaxdavst,tmax,plotlog,plotfit,fitndrop):
  '''Comparison of two studies: survival plots (area of stable particles) and Da vs turns plots'''
  matplotlib.rcParams.update({'font.size': 16})
  turnsldb    =db.env_var['turnsl']
  turnsedb    =db.env_var['turnse']
  turnsldbcomp=dbcomp.env_var['turnsl']
  turnsedbcomp=dbcomp.env_var['turnse']
  if(not turnsldb==turnsldbcomp):
    print(('Warning! Maximum turn number turn_max of %s and %s differ!'%(db.LHCDescrip,dbcomp.LHCDescrip)))
  try:
    ampmaxsurv=float(ampmaxsurv)
    ampmindavst=float(ampmindavst)
    ampmaxdavst=float(ampmaxdavst)
    tmax=int(float(tmax))
  except ValueError as NameError:
    print('Error in PlotCompDaVsTurns: ampmaxsurv and amprangedavst must be float values and tmax an integer value!')
    sys.exit(0)
  #remove all files
  if(plotlog):
    files=('DA_comp_log.png DAsurv_comp.png DA_comp_log.%s.png DAsurv_comp.%s.png'%(turnsedb,turnsedb)).split()
  else:
    files=('DA_comp.png DAsurv_comp.png DA_comp.%s.png DAsurv_comp.%s.png'%(turnsedb,turnsedb)).split()
  clean_dir_da_vst(db,files)# create directory structure and delete old files if force=true
# start analysis
  if(not db.check_seeds()):
    print('Seeds are missing in database!')
  for seed in db.get_db_seeds():
    seed=int(seed)
    for tune in db.get_db_tunes():
      if(seed in dbcomp.get_db_seeds() and tune in db.get_db_tunes()):
        dirname=db.mk_analysis_dir(seed,tune)#directories already created with
        pl.close('all')
        plot_surv_2d_comp(db,dbcomp,lblname,complblname,seed,tune,ampmaxsurv)
        pl.savefig('%s/DAsurv_comp.%s.png'%(dirname,turnsedb))
        print(('... saving plot %s/DAsurv_comp.%s.png'%(dirname,turnsedb)))
        plot_comp_da_vst(db,dbcomp,ldat,ldaterr,lblname,complblname,seed,tune,ampmindavst,ampmaxdavst,tmax,plotlog,plotfit,fitndrop)
        if(plotlog==True):
          pl.savefig('%s/DA_comp_log.%s.png'%(dirname,turnsedb),bbox_inches='tight')
          print(('... saving plot %s/DA_comp_log.%s.png'%(dirname,turnsedb)))
        else:
          pl.savefig('%s/DA_comp.%s.png'%(dirname,turnsedb),bbox_inches='tight')
          print(('... saving plot %s/DA_comp.%s.png'%(dirname,turnsedb)))
