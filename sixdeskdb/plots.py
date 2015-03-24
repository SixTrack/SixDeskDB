import numpy as np
import matplotlib.pyplot as plt
from datafromFort import *

postpr_plots = 'averem distance kvar maxslope smear survival'.split()

def plot_averem(db, seed, angle, a0, a1, nturns, path=None):

    f22 = Fort(22, db, seed, angle)
    f23 = Fort(23, db, seed, angle)
    f24 = Fort(24, db, seed, angle)

    fig = plt.figure()
    ax  = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
    ax.plot(f22['rad'], f22['(rad1*sigxminnld)'], marker='+', label = "Minimum")
    ax.plot(f23['rad'], f23['(rad1*sigxavgnld)'], marker='x', label = "Mean")
    ax.plot(f24['rad'], f24['(rad1*sigxmaxnld)'], marker='*', label = "Maximum")
    ax.plot((a0, a1), (a0,a1), marker='s', label="No errors")
    ax.legend(loc='best')
    ax.set_title('Averaged Amplitude(6d), %s turns' %nturns)
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Averaged Amplitude [sigma]')
    ax.set_xlim(a0,a1)
    if path:
        fn = "%s/averem.%s.%s.png"%(path, nturns, angle+1)
        print fn
        plt.savefig(fn)
    else:
        plt.show()

def plot_distance(db, seed, angle, a0, a1, nturns, path=None):

    f13 = Fort(13, db, seed, angle)
    f26 = Fort(26, db, seed, angle)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
    
    ax.plot(f13['rad'], f13['dist'], marker='+')
    ax.plot(f26['c1'], f26['c2'], marker='x', label = "Range from Chaos to Loss")
    ax.legend(loc="best")
    ax.set_title('Distance(6d), %s turns' %nturns)
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Distance in Phase Space of 2 initially close-by Particles')
    #ax.set_yscale("log", nonposy="clip")
    # ax.semilogy(f13['dist'], np.exp(-f13['dist']/5.0))
    ax.set_xlim(a0,a1)
    if path:
        fn = "%s/distance.%s.%s.png"%(path, nturns, angle+1)
        print fn
        plt.savefig(fn)
    else:
        plt.show()

def plot_kvar(db, seed, angle, a0, a1, nturns, exponent, path=None):

    f40 = Fort(40, db, seed)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
  
    if(exponent>=7):
        ax.plot(f40['angle'], f40['al']['arr'][:,0], marker='x', ls='-',label = '10 Million Turn Loss')
    if(exponent>=6):
        ax.plot(f40['angle'], f40['al']['arr'][:,1], marker='x', ls=':',label = "1 Million Turn Loss")
    if(exponent>=5):
        ax.plot(f40['angle'], f40['al']['arr'][:,2], marker='s', ls='-.',label = "100'000 Turn Loss")
    if(exponent>=4):
        ax.plot(f40['angle'], f40['al']['arr'][:,3], marker='*', ls=':',label = "10'000 Turn Loss")

    ax.plot(f40['angle'], f40['al']['arr'][:,4], marker='x', ls='--',label = "1'000 Turn Loss")
    
    ax.legend(loc="best")
    ax.set_title('D.A. vs K (6d), %s turns' %nturns)
    ax.set_xlabel('K = ATAN( SQRT( Ez/Ex )) in [Degree]')
    ax.set_ylabel('Dynamic Aperture in [sigma]')
    ax.set_xlim(0,90)
    if path:
        fn = "%s/kvar.%s.%s.png"%(path, nturns, angle+1)
        print fn
        plt.savefig(fn)
    else:
        plt.show()

def plot_maxslope(db, seed, angle, a0, a1, nturns, path=None):
    
    f12 = Fort(12, db, seed, angle)
    f26 = Fort(26, db, seed, angle)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
  
    
    ax.plot(f12['rad'], f12['distp'], marker='+')
    ax.plot(f26['c1'], f26['c2'], marker='x', ls='--',label = "Range from Chaos to Loss")
    ax.legend(loc="best")
    ax.set_title('Slope(6d), %s turns' %nturns)
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Maximum Slope of Distance in Phase Space')
    ax.set_xlim(a0,a1)
    if path:
        fn = "%s/maxslope.%s.%s.png"%(path, nturns, angle+1)
        print fn
        plt.savefig(fn)
    else:
        plt.show()

def plot_smear(db, seed, angle, a0, a1, nturns, path=None):

    f18 = Fort(18, db, seed, angle)
    f19 = Fort(19, db, seed, angle)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)

    ax.plot(f18['rad'], f18['smearx'], marker='+', label="Horizontal" )
    ax.plot(f19['rad'], f19['smeary'], marker='x', ls='--', label = "Vertical")
    ax.legend(loc="best")
    ax.set_title('Smear(6d), %s turns' %nturns)
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Smear [%]')
    ax.set_xlim(a0,a1)
    if path:
        fn = "%s/smear.%s.%s.png"%(path, nturns, angle+1)
        print fn
        plt.savefig(fn)
    else:
        plt.show()

def plot_survival(db, seed, angle, a0, a1, nturns, path=None):
   
    f15 = Fort(15, db, seed, angle)
    f14 = Fort(14, db, seed, angle)
    fig = plt.figure()
    ax  = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)

    ax.plot(f15['rad'], f15['sturns'], marker='+')
    ax.plot(f14['achaos'], f14['c2'], marker='x', ls='--', label = "Chaotic Border")
    ax.legend(loc="best")
    ax.set_title('Survival(6d), %s turns' %nturns)
    # ax.yaxis.set_scale('log')
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Survival Time')
    # ax.set_yscale("log", nonposy="clip")
    ax.set_xlim(a0, a1)
    
    if path:
        fn = "%s/survival.%s.%s.png"%(path, nturns, angle+1)
        print fn
        plt.savefig(fn)
    else:
        plt.show()

# def clean_dir_plot(db,files):
#   '''create directory structure and if force=true delete old files of plots'''
#   for seed in db.get_seeds():
#     for tune in db.get_db_tunes():
#       pp=db.mk_analysis_dir(seed,tune)# create directory
#       if(len(files)>0):#delete old plots and files
#         for filename in files:
#           ppf=os.path.join(pp,filename)
#           if(os.path.exists(ppf)): os.remove(ppf)
#   if(len(files)>0):
#     print('remove old {0} ... files in '+db.LHCDescrip).format(files)

def plotPostProcessingPlots(db):
  '''plot survival plots and da vs turns for list of data ldat and associated error ldaterr'''
  turnsl=db.env_var['turnsl']
  turnse=db.env_var['turnse']
  a0 = db.env_var['ns1l']
  a1 = db.env_var['ns2l']
  print('Post processing plots -- generating the post processing plots plots')
  
  #files = postpr_plot_files = [p+'%s.%s.png'%(turnse,angle) for p in postpr_plots]
  # clean_dir_da_vst(db,files)# create directory structure and delete old files if force=true
  if(not db.check_seeds()):
    print('!!! Seeds are missing in database !!!')
  for seed in db.get_seeds():
    seed=int(seed)
    for tune in db.get_db_tunes():
      dirname=db.mk_analysis_dir(seed,tune)#directory struct already created in clean_dir_da_vst, only get dir name (string) here
      for angle in xrange(len(db.get_db_angles())):
          plt.close('all')
          plt.figure(figsize=(6,6))
          plot_averem(db, seed, angle, a0, a1, turnsl, path=dirname)
          plot_distance(db, seed, angle, a0, a1, turnsl, path=dirname)
          plot_kvar(db, seed, angle, a0, a1, turnsl, turnse, path=dirname)
          plot_maxslope(db, seed, angle, a0, a1, turnsl, path=dirname)
          plot_smear(db, seed, angle, a0, a1, turnsl, path=dirname)
          plot_survival(db, seed, angle, a0, a1, turnsl, path=dirname)


#short
# def plot_tunedp(db, seed, angle, a0, a1, nturns, path=None):
#     f16=Fort(16, db)
#     f17=Fort(17, db)
#     fig = plt.figure()
#     ax = fig.add_subplot(111)
#     fig.subplots_adjust(top=0.85)

#     ax.plot(f15[2], f15[3], marker='+', label = "Horizontal")
#     ax.plot(f14[2], f14[3], marker='x', ls='--', label = "Vertical")
#     ax.legend(loc="best")
#     ax.set_title('Chromaticity ("$iqs"),  - %s Turn' %nturns)
#     ax.yaxis.set_scale('log')
#     ax.set_xlabel('delta')
#     ax.set_ylabel('Detuning')
#     ax.set_xlim(-0.002,0.002)
    
#     if path:
    #     plt.savefig("%stunedp.png"%path)
    # else:
    #     plt.show()