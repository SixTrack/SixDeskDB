import numpy as np
import matplotlib.pyplot as plt
from datafromFort import *

def plot_averem(sd, seed, angle, a0, a1, nturns, path=None):

    f22 = Fort(22, sd)[seed, angle]
    f23 = Fort(23, sd)[seed, angle]
    f24 = Fort(24, sd)[seed, angle]

    fig = plt.figure()
    ax  = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
    print a0
    print a1
    print f22['rad']
    print f23['rad']
    print f24['rad']
    print f23['(rad1*sigxavgnld)']
    ax.plot(f22['rad'], f22['(rad1*sigxminnld)'], marker='+', label = "Minimum")
    ax.plot(f23['rad'], f23['(rad1*sigxavgnld)'], marker='x', label = "Mean")
    ax.plot(f24['rad'], f24['(rad1*sigxmaxnld)'], marker='*', label = "Maximum")
    ax.plot((a0, a1), (a0,a1), marker='s', label="No errors")
    ax.legend(loc='best')
    ax.set_title('Averaged Amplitude(6d), %s turns' %nturns)
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Averaged Amplitude [sigma]')
    ax.set_xlim(a0,a1)
    # if path:
    #     plt.savefig("%saverem.png"%path)
    # else:
    #     plt.show()
    plt.show()

def plot_distance(sd, seed, angle, a0, a1, nturns, path=None):

    f13 = Fort(13, sd)[seed, angle]
    f26 = Fort(26, sd)[seed, angle]

    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
    
    ax.plot(f13['rad'], f13['dist'], marker='+')
    ax.plot(f26['c1'], f26['c2'], marker='x', label = "Range from Chaos to Loss")
    ax.legend(loc="best")
    ax.set_title('Averaged Amplitude(6d), %s turns' %nturns)
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Distance in Phase Space of 2 initially close-by Particles')
    #ax.set_yscale("log", nonposy="clip")
    # ax.semilogy(f13['dist'], np.exp(-f13['dist']/5.0))
    ax.set_xlim(a0,a1)
    if path:
        plt.savefig("%sdistance.png"%path)
    else:
        plt.show()

def plot_kvar(sd, seed, angle, a0, a1, nturns, exponent, path=None):
    f40 = Fort(40, sd)[seed]

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
        plt.savefig("%skvar.png"%path)
    else:
        plt.show()

def plot_maxslope(sd, seed, angle, a0, a1, nturns, path=None):
    
    f12 = Fort(12, sd)[seed, angle]
    f26 = Fort(26, sd)[seed, angle]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
  
    
    ax.plot(f12['rad'], f12['distp'], marker='+')
    ax.plot(f26['c1'], f26['c2'], marker='x', ls='--',label = "Range from Chaos to Loss")
    ax.legend(loc="best")
    ax.set_title('Averaged Amplitude(6d), %s turns' %nturns)
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Maximum Slope of Distance in Phase Space')
    ax.set_xlim(a0,a1)
    if path:
        plt.savefig("%smaxslope.png"%path)
    else:
        plt.show()

def plot_smear(sd, seed, angle, a0, a1, nturns, path=None):

    f18 = Fort(18, sd)[seed, angle]
    f19 = Fort(19, sd)[seed, angle]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)

    ax.plot(f18['rad'], f18['smearx'], marker='+', label="Horizontal" )
    ax.plot(f19['rad'], f19['smeary'], marker='x', ls='--', label = "Vertical")
    ax.legend(loc="best")
    ax.set_title('Averaged Amplitude(6d), %s turns' %nturns)
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Smear [%]')
    ax.set_xlim(a0,a1)
    if path:
        plt.savefig("%ssmear.png"%path)
    else:
        plt.show()

def plot_survival(sd, seed, angle, a0, a1, nturns, path=None):
   
    f15 = Fort(15, sd)[seed, angle]
    f14 = Fort(14, sd)[seed, angle]
    fig = plt.figure()
    ax  = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)

    ax.plot(f15['rad'], f15['sturns'], marker='+')
    ax.plot(f14['achaos'], f14['c2'], marker='x', ls='--', label = "Chaotic Border")
    ax.legend(loc="best")
    ax.set_title('Averaged Amplitude(6d), %s turns' %nturns)
    # ax.yaxis.set_scale('log')
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Survival Time')
    # ax.set_yscale("log", nonposy="clip")
    ax.set_xlim(a0, a1)
    
    if path:
        plt.savefig("%ssurvival.png"%path)
    else:
        plt.show()

#short
# def plot_tunedp(sd, seed, angle, a0, a1, nturns, path=None):
#     f16=Fort(16, sd)
#     f17=Fort(17, sd)
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