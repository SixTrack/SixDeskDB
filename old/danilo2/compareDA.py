# from sixdesk import *
import numpy as np
import math
import os
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib.backends.backend_pdf import PdfPages


colori=['black','red','blue','green','cyan','magenta','yellow']
pp=PdfPages('DAplots.pdf')
DAresults='/afs/cern.ch/user/d/dbanfi/SixTrack_NEW/'

suff=['_6D1','_6D_err']  #nothing or _6D,_6D1,_noBB_err...to select different samples ['6D','']

mylegend={'_6D_err':'6D bb lens + multip error','_6D1':'6D bb lens','':'4D bb lens','_noBB_err':'no BB, multip error'}
dt2=np.dtype([('suff','S100'),('binte','float'),('xangle','float'),('angle','int'),('beta','float'),
                ('DA1','float'),('DA2','float'),('DA3','float'),
                ('DA4','float'),('in_amp','float'),('fin_amp','float')])

# tmp2=np.zeros(len(tmp),dtype=dt2)
# tmp2=np.zeros(2000,dtype=dt2)
cont=0
betaarray=[15] #[7.5,10,15]                           #full range: modify in case needed
xingarray=range(390,940,50)
# xingarray=[590]                    #full range: modify in case needed
intearray=[2.2] #[1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8]    #full range: modify in case needed
anglearray=range(1,18)
beta_plot=15     #for plot at nominal value
inte_plot=2.2    #for plot at nominal value
xangle_plot=590  #for plot at nominal value
angle_plot=9     #for plot at nominal value
fignum=1

fact={7.5 : 0.932, 10 : 1.22 ,15 : 1}

for beta in betaarray:
    for stu in suff:
        DAres='DA_st%s%s.txt'%(beta,stu)
        if os.path.isfile(DAresults+DAres):
            fin=file(DAresults+DAres)
            dt=np.dtype([('binte','float'),('xangle','float'),('angle','int'),('DA1','float'),('DA2','float'),('DA3','float'),('DA4','float'),('in_amp','float'),('fin_amp','float')])
            tmp=np.genfromtxt(fin,dtype=dt)
            tmp2=np.zeros(len(tmp),dtype=dt2)
            tmp2['suff']=stu
            tmp2['beta']=beta
            tmp2['xangle']=tmp['xangle']
            tmp2['binte']=tmp['binte']
            tmp2['angle']=tmp['angle']
            tmp2['DA1']=tmp['DA1']
            tmp2['DA2']=tmp['DA2']
            tmp2['DA3']=tmp['DA3']
            tmp2['DA4']=tmp['DA4']
            tmp2['in_amp']=tmp['in_amp']
            tmp2['fin_amp']=tmp['fin_amp']
            if cont==0:
                out=np.copy(tmp2)
                cont+=1
            else:
                out=np.append(out,tmp2,axis=0)
        else:
            print "ERROR: file  %s does not exists!" %(DAresults+DAres)



for beta in betaarray:
    for xangle in xingarray:
        for binte in intearray:
            plt.figure(fignum)
            plt.xlabel('angle (deg)')
            plt.ylabel('DA ($\sigma$)')
            plt.title('beta = %s BeamInt = %s Xangle = %s' %(beta,binte,xangle))
            for stu in suff:    
                mask=(out["beta"]==beta)&(out["xangle"]==xangle)&(out["binte"]==binte)&(out["suff"]==stu)
                x2=out['angle'][mask]
                y2=out['DA3'][mask]
                mylab='X-angle = %s'%xangle
                plt.plot(x2,y2,linestyle='-',marker='+',color=colori[suff.index(stu)], label=mylegend[stu])
            plt.legend(loc='upper left',fontsize=12)
            pp.savefig()
            fignum+=1


if len(intearray) > 1:
    for beta in betaarray:
        for xangle in xingarray:
            fignum+=1
            plt.figure(fignum)
            plt.title('beta = %s Xangle = %s' %(beta,xangle))
            plt.xlabel('Beam Intensity ($10^{11}$)')
            plt.ylabel('DA ($\sigma$)')
            for stu in suff:
                y=[np.min(out["DA3"][(out["beta"]==beta)&(out["binte"]==x)&(out["xangle"]==xangle)&(out["suff"]==stu)]) for x in intearray]
                x=[np.min(out["binte"][(out["beta"]==beta)&(out["binte"]==x)&(out["xangle"]==xangle)&(out["suff"]==stu)]) for x in intearray]
                plt.plot(x,y,linestyle='-',marker='+',color=colori[suff.index(stu)], label=mylegend[stu])
            plt.legend(loc='upper left',fontsize=12)
            pp.savefig()

if len(xingarray) > 1:
    for beta in betaarray:
        for binte in intearray:
            fignum+=1
            plt.figure(fignum)
            plt.title('beta = %s Beam Intensity = %s' %(beta,binte))
            plt.xlabel('Xing angle ($\mu$rad)')
            plt.ylabel('DA ($\sigma$)')
            for stu in suff:
                y=[np.min(out["DA3"][(out["beta"]==beta)&(out["binte"]==binte)&(out["xangle"]==x)&(out["suff"]==stu)]) for x in xingarray]  ####only if is >0!!!!!!!!!
                x=[np.min(out["xangle"][(out["beta"]==beta)&(out["binte"]==binte)&(out["xangle"]==x)&(out["suff"]==stu)]) for x in xingarray]
                plt.plot(x,y,linestyle='-',marker='+',color=colori[suff.index(stu)], label=mylegend[stu])
            plt.legend(loc='upper left',fontsize=12)
            pp.savefig()
            
if len(betaarray) > 1:
    for binte in intearray:
        for xangle in xingarray:
            fignum+=1
            plt.figure(fignum)
            plt.title('Beam Intensity = %s Xangle = %s' %(binte,xangle))
            plt.xlabel('$\beta$ (cm)')
            plt.ylabel('DA ($\sigma$)')
            for stu in suff:
                y=[np.min(out["DA3"][(out["beta"]==x)&(out["binte"]==binte)&(out["xangle"]==xangle)&(out["suff"]==stu)]) for x in betaarray]
                x=[np.min(out["beta"][(out["beta"]==x)&(out["binte"]==binte)&(out["xangle"]==xangle)&(out["suff"]==stu)]) for x in betaarray]
                plt.plot(x,y,linestyle='-',marker='+',color=colori[suff.index(stu)], label=mylegend[stu])
            plt.legend(loc='upper left',fontsize=12)
            pp.savefig()
pp.close()
plt.show()
# 



