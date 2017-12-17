import numpy as np
import matplotlib.pyplot as plt
from datafromFort import Fort
import sys 

postpr_plots = 'averem distance kvar maxslope smear survival'.split()


class Post_Plot:

    def __init__(self, db, name, seed=None, angle=None, tune=None):
        self.db = db
        self.nturns = self.db.env_var['turnsl']
        self.exponent = self.db.env_var['turnse']
        self.a0 = self.db.env_var['ns1l']
        self.a1 = self.db.env_var['ns2l']
        self.names = ["all", "averem", "distance", "kvar", "maxslope", "smear", "survival"]
        plot = self.process_args(name, seed, angle)
        plot(seed, angle, tune)

    def process_args(self, name, seed, angle):
        try:
          plot = getattr(self, 'plot_{name}'.format(name=name))
        except Exception, e:
          print
          print "Wrong plot option: %s"%name
          sys.exit(1)

        if seed and (seed < 1 or seed > len(self.db.get_seeds())):
          print('Error in plot: incorrect seed!')
          sys.exit(1)
        if angle and (angle < 0 or angle >= len(self.db.get_angles())):
          print('Error in da_vs_turns: incorrect angle!')
          sys.exit(1)
        return plot

    def plot_averem(self, seed, angle, tune, show=False):

        f22 = Fort(22, self.db, seed, angle, tune)
        f23 = Fort(23, self.db, seed, angle, tune)
        f24 = Fort(24, self.db, seed, angle, tune)

        fig = plt.figure()
        ax  = fig.add_subplot(111)
        fig.subplots_adjust(top=0.85)
        ax.plot(f22['rad'], f22['(rad1*sigxminnld)'], marker='+', label = "Minimum")
        ax.plot(f23['rad'], f23['(rad1*sigxavgnld)'], marker='x', label = "Mean")
        ax.plot(f24['rad'], f24['(rad1*sigxmaxnld)'], marker='*', label = "Maximum")
        ax.plot((self.a0, self.a1), (self.a0,self.a1), marker='s', label="No errors")
        ax.legend(loc='best')
        ax.set_title('Averaged Amplitude(6d), %s turns' %self.nturns)
        ax.set_xlabel('Initial Amplitude [sigma]')
        ax.set_ylabel('Averaged Amplitude [sigma]')
        ax.set_xlim(self.a0,self.a1)
        if show:
            plt.show()
        else:
            dirname=self.db.mk_analysis_dir(seed,tune)
            fn = "%s/averem.%s.%s.png"%(dirname, self.nturns, angle+1)
            print fn
            plt.savefig(fn)


    def plot_distance(self, seed, angle, tune, show=False):

        f13 = Fort(13, self.db, seed, angle, tune)
        f26 = Fort(26, self.db, seed, angle, tune)

        fig = plt.figure()
        ax = fig.add_subplot(111)
        fig.subplots_adjust(top=0.85)
        
        ax.plot(f13['rad'], f13['dist'], marker='+')
        ax.plot(f26['c1'], f26['c2'], marker='x', label = "Range from Chaos to Loss")
        ax.legend(loc="best")
        ax.set_title('Distance(6d), %s turns' %self.nturns)
        ax.set_xlabel('Initial Amplitude [sigma]')
        ax.set_ylabel('Distance in Phase Space of 2 initially close-by Particles')
        #ax.set_yscale("log", nonposy="clip")
        # ax.semilogy(f13['dist'], np.exp(-f13['dist']/5.0))
        ax.set_xlim(self.a0,self.a1)
        
        if show:
            plt.show()
        else:
            dirname=self.db.mk_analysis_dir(seed,tune)
            fn = "%s/distance.%s.%s.png"%(dirname, self.nturns, angle+1)
            print fn
            plt.savefig(fn)


    def plot_kvar(self, seed, angle, tune, show=False):

        f40 = Fort(40, self.db, seed, tunes=tune)

        fig = plt.figure()
        ax = fig.add_subplot(111)
        fig.subplots_adjust(top=0.85)
      
        if(self.exponent>=7):
            ax.plot(f40['angle'], f40['al']['arr'][:,0], marker='x', ls='-',label = '10 Million Turn Loss')
        if(self.exponent>=6):
            ax.plot(f40['angle'], f40['al']['arr'][:,1], marker='x', ls=':',label = "1 Million Turn Loss")
        if(self.exponent>=5):
            ax.plot(f40['angle'], f40['al']['arr'][:,2], marker='s', ls='-.',label = "100'000 Turn Loss")
        if(self.exponent>=4):
            ax.plot(f40['angle'], f40['al']['arr'][:,3], marker='*', ls=':',label = "10'000 Turn Loss")

        ax.plot(f40['angle'], f40['al']['arr'][:,4], marker='x', ls='--',label = "1'000 Turn Loss")
        
        ax.legend(loc="best")
        ax.set_title('D.A. vs K (6d), %s turns' %self.nturns)
        ax.set_xlabel('K = ATAN( SQRT( Ez/Ex )) in [Degree]')
        ax.set_ylabel('Dynamic Aperture in [sigma]')
        ax.set_xlim(0,90)
        
        if show:
            plt.show()
        else:
            dirname=self.db.mk_analysis_dir(seed,tune)
            fn = "%s/kvar.%s.%s.png"%(dirname, self.nturns, angle+1)
            print fn
            plt.savefig(fn)


    def plot_maxslope(self, seed, angle, tune, show=False):
        
        f12 = Fort(12, self.db, seed, angle, tune)
        f26 = Fort(26, self.db, seed, angle, tune)
        fig = plt.figure()
        ax = fig.add_subplot(111)
        fig.subplots_adjust(top=0.85)
      
        
        ax.plot(f12['rad'], f12['distp'], marker='+')
        ax.plot(f26['c1'], f26['c2'], marker='x', ls='--',label = "Range from Chaos to Loss")
        ax.legend(loc="best")
        ax.set_title('Slope(6d), %s turns' %self.nturns)
        ax.set_xlabel('Initial Amplitude [sigma]')
        ax.set_ylabel('Maximum Slope of Distance in Phase Space')
        ax.set_xlim(self.a0,self.a1)

        if show:
            plt.show()
        else:
            dirname=self.db.mk_analysis_dir(seed,tune)
            fn = "%s/maxslope.%s.%s.png"%(dirname, self.nturns, angle+1)
            print fn
            plt.savefig(fn)


    def plot_smear(self, seed, angle, tune, show=False):

        f18 = Fort(18, self.db, seed, angle, tune)
        f19 = Fort(19, self.db, seed, angle, tune)
        fig = plt.figure()
        ax = fig.add_subplot(111)
        fig.subplots_adjust(top=0.85)

        ax.plot(f18['rad'], f18['smearx'], marker='+', label="Horizontal" )
        ax.plot(f19['rad'], f19['smeary'], marker='x', ls='--', label = "Vertical")
        ax.legend(loc="best")
        ax.set_title('Smear(6d), %s turns' %self.nturns)
        ax.set_xlabel('Initial Amplitude [sigma]')
        ax.set_ylabel('Smear [%]')
        ax.set_xlim(self.a0,self.a1)

        if show:
            plt.show()
        else:
            dirname=self.db.mk_analysis_dir(seed,tune)
            fn = "%s/smear.%s.%s.png"%(dirname, self.nturns, angle+1)
            print fn
            plt.savefig(fn)

    def plot_survival(self, seed, angle, tune, show=False):
       
        f15 = Fort(15, self.db, seed, angle, tune)
        f14 = Fort(14, self.db, seed, angle, tune)
        fig = plt.figure()
        ax  = fig.add_subplot(111)
        fig.subplots_adjust(top=0.85)

        ax.plot(f15['rad'], f15['sturns'], marker='+')
        ax.plot(f14['achaos'], f14['c2'], marker='x', ls='--', label = "Chaotic Border")
        ax.legend(loc="best")
        ax.set_title('Survival(6d), %s turns' %self.nturns)
        # ax.yaxis.set_scale('log')
        ax.set_xlabel('Initial Amplitude [sigma]')
        ax.set_ylabel('Survival Time')
        # ax.set_yscale("log", nonposy="clip")
        ax.set_xlim(self.a0, self.a1)
        
        if show:
            plt.show()
        else:
            dirname=self.db.mk_analysis_dir(seed,tune)
            fn = "%s/survival.%s.%s.png"%(dirname, self.nturns, angle+1)
            print fn
            plt.savefig(fn)

    def plot_all(self, seed = None, angle= None, tune=None):
      '''plot survival plots and da vs turns for list of data ldat and associated error ldaterr'''

      print('Post processing plots -- generating the post processing plots plots')
      
      if(not self.db.check_seeds()):
        print('!!! Seeds are missing in database !!!')
      for seed in self.db.get_seeds():
        seed=int(seed)
        for tune in self.db.get_db_tunes():
          dirname=self.db.mk_analysis_dir(seed,tune)
          for angle in xrange(len(self.db.get_db_angles())):
              plt.close('all')
              plt.figure(figsize=(6,6))
              self.plot_averem(seed, angle, tune)
              self.plot_distance(seed, angle, tune)
              self.plot_kvar(seed, angle, tune)
              self.plot_maxslope(seed, angle, tune)
              self.plot_smear(seed, angle, tune)
              self.plot_survival(seed, angle, tune)





# short runs
# def plot_tunedp(self, seed, angle, tune, show=False):
#     f16=Fort(16, self.db)
#     f17=Fort(17, self.db)
#     fig = plt.figure()
#     ax = fig.add_subplot(111)
#     fig.subplots_adjust(top=0.85)

#     ax.plot(f15[2], f15[3], marker='+', label = "Horizontal")
#     ax.plot(f14[2], f14[3], marker='x', ls='--', label = "Vertical")
#     ax.legend(loc="best")
#     ax.set_title('Chromaticity ("$iqs"),  - %s Turn' %self.nturns)
#     ax.yaxis.set_scale('log')
#     ax.set_xlabel('delta')
#     ax.set_ylabel('Detuning')
#     ax.set_xlim(-0.002,0.002)
