#!/usr/bin/python

# python re-implementation of readplotb.f + read10b.f
# NOTA: please use python version >=2.6   

import sys
import getopt
from SixdeskDB import *
import numpy as np
import matplotlib.pyplot as plt
import os




# PART TO BE EDITED ========================================================================
Elhc=2.5                    #normalized emittance as in "general input"
Einj=7460.5                 #gamma as in "general input"
#workarea='/afs/cern.ch/user/d/dbanfi/SixTrack_NEW'  #where input db is, and where output will be written
# DO NOT EDIT BEYOND HERE IF YOU'RE NOT REALLY SURE  =======================================    

rectype=[('seed','int'),('qx','float'),('qy','float'),('betx','float'),('bety','float'),('sigx1','float'),('sigy1','float'),('deltap','float'),('emitx','float'),('emity','float'),
        ('sigxminnld', 'float'),('sigxavgnld' ,'float') ,('sigxmaxnld', 'float'),('sigyminnld', 'float'),('sigyavgnld' ,'float'),
        ('sigymaxnld', 'float'),('betx2','float'),('bety2','float'),('distp','float'),('dist','float'),('qx_det','float'),('qy_det','float'),('sturns1' ,'int'),
        ('sturns2','int'),('turn_max','int'),('amp1','float'),('amp2','float'),('angle','float'),('smearx','float'),('smeary','float')]
names='seed,qx,qy,betx,bety,sigx1,sigy1,deltap,emitx,emity,sigxminnld,sigxavgnld,sigxmaxnld,sigyminnld,sigyavgnld,sigymaxnld,betx2,bety2,distp,dist,qx_det,qy_det,sturns1,sturns2,turn_max,amp1,amp2,angle,smearx,smeary'
outtype=[('study','S100'),('seed','int'),('angle','float'),('achaos','float'),('achaos1','float'),('alost1','float'),('alost2','float'),('Amin','float'),('Amax','float')]

def plot_averem(path, nturns, a0, a1):

    f22=np.loadtxt('fort.22')
    f23=np.loadtxt('fort.23')
    f24=np.loadtxt('fort.24')
    
    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
    
    ax.plot(f22[:,0], f22[:,1], marker='+', label = "Minimum")
    ax.plot(f23[:,0], f23[:,1], marker='x', label = "Mean")
    ax.plot(f24[:,0], f24[:,1], marker='*', label = "Maximum")
    ax.legend(loc="upper left")
    ax.set_title('%s Averaged Amplitude(6d), %s' %(path,nturns))
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Averaged Amplitude [sigma]')
    ax.set_xlim(a0,a1)
    plt.show()

def plot_distance(path, nturns, a0, a1):

    f13=np.loadtxt('fort.13')
    f26=np.loadtxt('fort.26')

    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
    
    ax.plot(f13[:,0], f13[:,1], marker='+')
    ax.plot(f26[:,0], f26[:,1], marker='x', label = "Range from Chaos to Loss")
    ax.legend(loc="upper left")
    ax.set_title('%s Averaged Amplitude(6d), %s' %(path,nturns))
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Distance in Phase Space of 2 initially close-by Particles')
    #ax.set_yscale("log", nonposy="clip")
    ax.set_xlim(a0,a1)
    plt.show()

def plot_kvar(path, nturns, a0, a1):
    return

def plot_maxslope(path, nturns, a0, a1):
    
    f12=np.loadtxt('fort.12')
    f26=np.loadtxt('fort.26')
    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
  
    
    ax.plot(f12[:,0], f12[:,1], marker='+')
    ax.plot(f26[:,0], f26[:,1], marker='x', label = "Range from Chaos to Loss")
    ax.legend(loc="upper left")
    ax.set_title('%s Averaged Amplitude(6d), %s' %(path,nturns))
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Maximum Slope of Distance in Phase Space')
    ax.set_xlim(a0,a1)
    plt.show()

def plot_smear(path, nturns, a0, a1):

    f18=np.loadtxt('fort.18')
    f19=np.loadtxt('fort.19')
    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)
    
    ax.plot(f18[:,0], f18[:,1], marker='+', label="Horizontal" )
    ax.plot(f19[:,0], f19[:,1], marker='x', label = "Vertical")
    ax.legend(loc="upper left")
    ax.set_title('%s Averaged Amplitude(6d), %s' %(path,nturns))
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Smear [%]')
    ax.set_xlim(a0,a1)
    plt.show()

def plot_survival(path, nturns, a0, a1):
   
    f15=np.loadtxt('fort.15')
    f14=np.loadtxt('fort.14')
    fig = plt.figure()
    ax = fig.add_subplot(111)
    fig.subplots_adjust(top=0.85)

    ax.plot(f15[:,0], f15[:,1], marker='+')
    ax.plot(f14[:,0], f14[:,1], marker='x', label = "Chaotic Border")
    ax.legend(loc="upper left")
    ax.set_title('%s Averaged Amplitude(6d), %s' %(path,nturns))
    ax.set_xlabel('Initial Amplitude [sigma]')
    ax.set_ylabel('Survival Time')
    #ax.set_yscale("log", nonposy="clip")
    ax.set_xlim(a0,a1)
    
    plt.show()


def main2(studyName):
    database='%s.db'%(studyName)
    if os.path.isfile(database):
        sd=SixDeskDB(studyName)
    else:
        print "ERROR: file  %s does not exists!" %(database)
        sys.exit()

    
    nPlotSeeds = sd.env_var["iend"]

    f2 = open('DA_%s.txt'%studyName, 'w')
    LHCDesName=sd.env_var['LHCDesName']
    turnse=sd.env_var['turnse']
    sixdesktunes='%s_%s'%(sd.env_var['tunex'], sd.env_var['tuney'])
    ns1l=sd.env_var['ns1l']
    ns2l=sd.env_var['ns2l']

    tmp = np.array(sd.execute('SELECT DISTINCT %s FROM six_results,six_input where id=six_input_id'%names),dtype=rectype)
    Elhc,Einj = sd.execute('SELECT emitn,gamma from six_beta LIMIT 1')[0]
    anumber = 1
    
    ment=1000
    epsilon = 1e-38
    ntlint = 4
    ntlmax = 12

    iin  = -999
    iend = -999

    angles = sd.get_angles()
    seeds  = sd.get_seeds()

    for angle in angles:      
        f = open('DAres_%s.%s.%s.%d'%(LHCDesName,sixdesktunes,turnse,anumber), 'w')
        angleRes=[]

        for seed in seeds:
            countic=0
            nSeed = 1
            seedRes=[]        
            tl = np.zeros(ntlmax*ntlint+1)
            al = np.zeros(ntlmax*ntlint+1)
            ichl =np.zeros(ntlmax*ntlint+1)
            for i in range(1, ntlmax):
              for j in range(0,ntlint):
                    tl[(i-1)*ntlint+j] = int(round(10**((i-1)+(j-1)/float(ntlint))))
                    al[(i-1)*ntlint+j] = 0
                    ichl[(i-1)*ntlint+j] = 0

            tl[ntlmax*ntlint]=int(round(10**(float(ntlmax))))
            achaos = 0
            achaos1 = 0
            alost1 = 0.
            alost2 = 0.
            ilost=0

            if(np.abs(Einj)< epsilon):
                    print "ERROR: Injection energy too small"
                    sys.exit()

            fac=2.0
            fac1=2.0
            fac2=0.1
            fac3=0.0
            fac4=1.1
            fac5=0.9
            itest=1

            ich1 = 0
            ich2 = 0
            ich3 = 0
            icount = 1.


            mask=[(tmp['betx']> epsilon) & (tmp['emitx']>epsilon) & (tmp['bety']>epsilon) & (tmp['emity']>epsilon) & (tmp['angle']==angle) & (tmp['seed']==seed)]
            inp=tmp[mask]
            if inp.size<2 : 
                print 'not enought data for angle = %s' %angle
                break
            zero = 1e-10
            
            iel=itest-1
            inp['sigx1'] = np.sqrt(inp['betx']*inp['emitx'])
            inp['sigy1'] = np.sqrt(inp['bety']*inp['emity'])
            itest = sum(inp['betx']>zero)    
            iel=itest-1
            rat=0
            if abs(inp['sigx1'][0]) < epsilon and abs(inp['sigx1'][0])>epsilon and inp['bety'] > epsilon:  
                rat=pow(inp['sigy1'][0],2)*inp['betx'][0]/(pow(inp['sigx1'][0],2)*inp['bety'][0])
            if abs(inp['emity'][0]) > abs(inp['emitx'][0]) or rat > 1e-10:
                rat=0
                dummy=np.copy(inp['betx'])
                inp['betx']=inp['bety']
                inp['bety']=dummy
                dummy=np.copy(inp['betx2'])
                inp['betx2']=inp['bety2']
                inp['bety2']=dummy
                dummy=np.copy(inp['sigxminnld'])
                inp['sigxminnld']=inp['sigyminnld']
                inp['sigyminnld']=dummy
                dummy=np.copy(inp['sigx1'])
                inp['sigx1']=inp['sigy1']
                inp['sigy1']=dummy
                dummy=np.copy(inp['sigxmaxnld'])
                inp['sigxmaxnld']=inp['sigymaxnld']
                inp['sigymaxnld']=dummy
                dummy=np.copy(inp['sigxavgnld'])
                inp['sigxavgnld']=inp['sigyavgnld']
                inp['sigyavgnld']=dummy
                dummy=np.copy(inp['emitx']) 
                inp['emitx']=inp['emity']
                inp['emity']=dummy

            sigma=np.sqrt(inp['betx'][0]*Elhc/Einj)
            if abs(inp['emity'][0])>0 and abs(inp['sigx1'][0])>0:
                if abs(inp['emitx'][0])< epsilon :
                    rad=np.sqrt(1+(pow(inp['sigy1'][0],2)*inp['betx'][0])/(pow(inp['sigx1'][0],2)*inp['bety'][0]))/sigma
                else:
                    rad=np.sqrt((inp['emitx'][0]+inp['emity'][0])/inp['emitx'][0])/sigma
            else:
                rad=1
            if abs(inp['sigxavgnld'][0])>zero and abs(inp['bety'][0])>zero and sigma > 0:
                if abs(inp['emitx'][0]) < zero :
                    rad1=np.sqrt(1+(pow(inp['sigyavgnld'][0],2)*inp['betx'][0])/(pow(inp['sigxavgnld'][0],2)*inp['bety'][0]))/sigma
                else:
                    rad1=(inp['sigyavgnld'][0]*np.sqrt(inp['betx'][0])-inp['sigxavgnld'][0]*np.sqrt(inp['bety2'][0]))/(inp['sigxavgnld'][0]*np.sqrt(inp['bety'][0])-inp['sigyavgnld'][0]*np.sqrt(inp['betx2'][0]))
                    rad1=np.sqrt(1+rad1*rad1)/sigma
            else:
                rad1 = 1

            amin=1/epsilon
            amax=zero   
                  
            for i in range(0,iel+1):

                if abs(inp['sigx1'][i]) > epsilon and inp['sigx1'][i]:
                        amin = inp['sigx1'][i]
                if abs(inp['sigx1'][i]) > epsilon and inp['sigx1'][i]:
                        amax=inp['sigx1'][i]
                if ich1 == 0 and (inp['distp'][i] > fac or inp['distp'][i]< 1./fac): 
                    ich1 = 1
                    achaos=rad*inp['sigx1'][i]
                    iin=i
                if ich3 == 0 and inp['dist'][i] > fac3 :
                    ich3=1
                    iend=i
                    achaos1=rad*inp['sigx1'][i]
                if ich2 == 0 and  (inp['sturns1'][i]<inp['turn_max'][i] or inp['sturns2'][i]<inp['turn_max'][i]):
                    ich2 = 1
                    alost2 = rad*inp['sigx1'][i]
                for j in range(0, ntlmax*ntlint+1):
                  if ichl[j] == 0 and  int(round(inp['turn_max'][i])) >= tl[j] and (int(round(inp['sturns1'][i])) < tl[j] or int(round(inp['sturns2'][i])) < tl[j]):
                      ichl[j] = 1
                      al[j] = rad*inp['sigx1'][i]
            

            if iin != -999 and iend == -999 : iend=iel  
            if iin != -999 and iend > iin :    
                for i in range(iin,iend+1) :
                    if(abs(rad*inp['sigx1'][i])>zero):
                        alost1 += rad1 * inp['sigxavgnld'][i]/rad/inp['sigx1'][i]
                    if(i!=iend):
                        icount+=1.
                alost1 = alost1/icount
                if alost1 >= 1.1 or alost1 <= 0.9:  alost1= -1. * alost1
            else:
                alost1 = 1.0

            al = abs(alost1)* al
          
            alost1=alost1*alost2
            if amin == 1/epsilon:
                    amin = zero
            amin=amin*rad
            amax=amax*rad

            al[al==0]=amax

            alost3 = inp['turn_max'][1]

            inp['sturns1'][inp['sturns1']== zero] = 1
            inp['sturns2'][inp['sturns2']== zero] = 1
          
            alost3 = min(alost3, min(inp['sturns1']),min(inp['sturns2']))
           

            name2 = 'DAres.%s.%s.%s'%(studyName,sixdesktunes,turnse)
            name1 = '%s%ss%s%s-%s%s.%d'%(LHCDesName,seed,sixdesktunes,ns1l, ns2l, turnse,anumber)
            
            if(seed<10):
                name1+=" "
            if(anumber<10):
                name1+=" " 

            if achaos== 0:
                achaos=amin
            else:
                #plot_results['f14'] = [[achaos, alost3/fac], [achaos, inp['turn_max'][0]*fac]]
                f14 = open('fort.14.%d.%d' %(nSeed,anumber),'w')
                f14.write('%s %s\n'%(achaos,alost3/fac))
                f14.write('%s %s\n'%(achaos,inp['turn_max'][0]*fac))
                f14.close()
            if abs(alost1) < epsilon:
                alost1=amax
                ilost=1
            if nSeed != (nPlotSeeds +1):
                f11 = open('fort.11.%d.%d' %(nSeed,anumber),'w')
                f11.write('%s %s\n'%(achaos,1e-1))
                f11.write('%s %s\n'%(achaos,inp['turn_max'][0]*fac))
                f11.close()

                f26 = open('fort.26.%d.%d' %(nSeed,anumber),'w')
                f26.write('%s %s\n'%(achaos,1e-1))
                f26.write('%s %s\n'%((alost2,1e-1) if alost2 > epsilon else (amax, 1e-1)))
                f26.close()

                f27 = open('fort.27.%d.%d' %(nSeed,anumber),'w')
                al.tofile(f27, sep="\t", format="%s")
                f27.close()

                f12 = open('fort.12.%d.%d' %(nSeed,anumber),'w')
                f13 = open('fort.13.%d.%d' %(nSeed,anumber),'w')
                f15 = open('fort.15.%d.%d' %(nSeed,anumber),'w')
                f16 = open('fort.16.%d.%d' %(nSeed,anumber),'w')
                f17 = open('fort.17.%d.%d' %(nSeed,anumber),'w')
                f18 = open('fort.18.%d.%d' %(nSeed,anumber),'w')
                f19 = open('fort.19.%d.%d' %(nSeed,anumber),'w')
                f20 = open('fort.20.%d.%d' %(nSeed,anumber),'w')
                f21 = open('fort.21.%d.%d' %(nSeed,anumber),'w')
                f22 = open('fort.22.%d.%d' %(nSeed,anumber),'w')
                f23 = open('fort.23.%d.%d' %(nSeed,anumber),'w')
                f24 = open('fort.24.%d.%d' %(nSeed,anumber),'w')
                f25 = open('fort.25.%d.%d' %(nSeed,anumber),'w')

                for i in range(0, iel+1):

                    f12.write('%s %s\n'%(rad*inp['sigx1'][i], inp['distp'][i]))
                    
                    f13.write('%s %s\n'%(rad*inp['sigx1'][i], inp['dist'][i]))
                    
                    f15.write('%s %s\n'%(rad*inp['sigx1'][i], inp['sturns1'][i]))
                    f15.write('%s %s\n'%(rad*inp['sigx1'][i], inp['sturns2'][i]))
                    
                    if ilost ==1 or rad*inp['sigx1'][i] < alost2:
                        if inp['distp'][i] < fac1 and inp['dist'][i] < fac2:
                            iel2=(iel+1)/2
                            f16.write('%s %s\n' %(inp['deltap'][i],inp['qx'][i]-inp['qx'][iel2]))
                            f17.write('%s %s\n' %(inp['deltap'][i],inp['qy'][i]-inp['qy'][iel2]) )
                            f20.write('%s %s\n' %(rad*inp['sigx1'][i],inp['qx_det'][i]))
                            f21.write('%s %s\n' %(rad*inp['sigx1'][i],inp['qy_det'][i]))
                            f25.write('%s %s %d %s %s\n' %(inp['qx_det'][i]+inp['qx'][i], inp['qy_det'][i]+inp['qy'][i],i+1,inp['qx_det'][i],inp['qy_det'][i]))

                        f18.write('%s %s\n'%(rad*inp['sigx1'][i], inp['smearx'][i]))

                        f19.write('%s %s\n'%(rad*inp['sigx1'][i], inp['smeary'][i]))

                        f22.write('%s %s\n'%(rad*inp['sigx1'][i], rad1*inp['sigxminnld'][i]))

                        f23.write('%s %s\n'%(rad*inp['sigx1'][i], rad1*inp['sigxavgnld'][i]))

                        f24.write('%s %s  %s\n'%(rad*inp['sigx1'][i], rad1*inp['sigxmaxnld'][i],inp['sigxmaxnld'][i]))

                f12.close()
                f13.close()
                f14.close()
                f15.close()
                f16.close()
                f17.close()
                f18.close()
                f19.close()
                f20.close()
                f21.close()
                f22.close()
                f23.close()                  
                f24.close()
                f25.close()
                f26.close()
                f27.close()

            f.write(' %s         %6f    %6f    %6f    %6f    %6f   %6f\n'%( name1,achaos,achaos1,alost1,alost2,rad*inp['sigx1'][0],rad*inp['sigx1'][iel]))
            f2.write('%s %s %s %s %s %s %s %s %s \n'%( name2, seed,angle,achaos,achaos1,alost1,alost2,rad*inp['sigx1'][0],rad*inp['sigx1'][iel]))
            nSeed +=1
        anumber+=1
        f.close()
    f2.close()
    #nSeed +=1
    print nSeed
    fhtxt = open('DA_%s.txt'%studyName, 'r')
    final=np.genfromtxt(fhtxt,dtype=outtype)
    fhtxt.close()

    fnplot='DAres.%s.%s.%s.plot'%(LHCDesName,sixdesktunes,turnse)
    fhplot = open(fnplot, 'w')
    fn=0

    for angle in np.unique(final['angle']):
        fn+=1
        study= final['study'][0]
        idxangle=final['angle']==angle
        idx     =idxangle&(final['alost1']!=0)
        idxneg  =idxangle&(final['alost1']<0)
        mini, smini = np.min(np.abs(final['alost1'][idx])), np.argmin(np.abs(final['alost1'][idx]))
        maxi, smaxi = np.max(np.abs(final['alost1'][idx])), np.argmax(np.abs(final['alost1'][idx]))
        toAvg = np.abs(final['alost1'][idx])
        i = len(toAvg)
        mean = np.mean(toAvg)
        idxneg = (final['angle']==angle)&(final['alost1']<0)
        eqaper = np.where(final['alost2'] == final['Amin'])[0]
        nega = len(final['alost1'][idxneg])
        Amin = np.min(final['Amin'][idxangle])
        Amax = np.max(final['Amax'][idxangle])

        for k in eqaper:
          print "Seed #:  %d Dynamic Aperture below:  %.2f Sigma\n" %( k, final['Amin'][k])

        if i == 0:
          mini  = -Amax
          maxi  = -Amax
          mean  = -Amax
        else:
          if i < int(sd.env_var['iend']):
            maxi = -Amax
          elif len(eqaper)>0:
            mini = -Amin
          # print "Minimum:  %.2f  Sigma at Seed #: %d\n" %(mini, smini)
          # print "Maximum:  %.2f  Sigma at Seed #: %d\n" %(maxi, smaxi)
          # print "Average: %.2f Sigma\n " %(mean)
        
        #print "# of (Aav-A0)/A0 >10%%:  %d\n"  %nega        
        fhplot.write('%s %d %.2f %.2f %.2f %d %.2f %.2f\n'%(name2, fn, mini, mean, maxi, nega, Amin, Amax))
    fhplot.close()

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    for o, a in opts:
        if o in ("-h", "--help"):
            print "use: DA_FullStat_public <study_name>"
            sys.exit(0)
    if len(args)<1 :
        print "too few options: please provide <study_name>"
        sys.exit()
    if len(args)>1 :
        print "too many options: please provide only <study_name>"
        sys.exit()
    main2(sys.argv[1])
    path='job_tracking/1/simul/62.31_60.32/6-14/e5/.1'
    nturns=100000
    a0 = 6
    a1 = 14
    #plot_averem( '%s/fort10.tgz'%path, nturns, a0, a1)
    #plot_distance( '%s/fort10.tgz'%path, nturns, a0, a1)
    #plot_maxslope('%s/fort10.tgz'%path, nturns, a0, a1)
    #plot_smear('%s/fort10.tgz'%path, nturns, a0, a1)
    #plot_survival('%s/fort10.tgz'%path, nturns, a0, a1)