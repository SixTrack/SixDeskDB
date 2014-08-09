#!/usr/bin/python

# python re-implementation of read10b.f  done by Danilo Banfi (danilo.banfi@cern.ch)
# This compute DA starting from the local .db produced by CreateDB.py
# Below are indicated thing that need to be edited by hand. 
# You only have to provide the name of the study <study_name> like 
# python CreateDB.py <write_your_fancy_study_name_here>
# DA result will be written in file DA_<study_name>.txt with the usual meaning
# 
# NOTA: please use python version >=2.6   

import sys
import getopt
from SixdeskDB import *
import numpy as np
import math
import matplotlib.pyplot as plt
import os

# PART TO BE EDITED ========================================================================
# anglearray = range(5,90,5)  #xy angle simulated: change accordingly with your simulations settings
Elhc=2.5                    #normalized emittance as in "general input"
Einj=7460.5                 #gamma as in "general input"
workarea='.'  #where input db is, and where output will be written
# DO NOT EDIT BEYOND HERE IF YOU'RE NOT REALLY SURE  =======================================    

rectype=[('betx'    ,'float'),('bety'    ,'float'),('sigx1'   ,'float'),('sigy1'   ,'float'),('emitx'   ,'float'),('emity'   ,'float'),
        ('sigxavg' ,'float') ,('sigyavg' ,'float'),('betx2'   ,'float'),('bety2'   ,'float'),('distp'   ,'float'),('dist'    ,'float'),
        ('sturns1' ,'int')   ,('sturns2' ,'int')  ,('turn_max','int')  ,('amp1'    ,'float'),('amp2'    ,'float'),('angle'   ,'float')]
names='betx,bety,sigx1,sigy1,emitx,emity,sigxavg,sigyavg,betx2,bety2,distp,dist,sturns1,sturns2,turn_max,amp1,amp2,angle'

def main1(studyName):
    database='%s.db'%(studyName)
    if os.path.isfile(database):
        sd=SixDeskDB(studyName)   
    else:
        print "ERROR: file  %s does not exists!" %(database)
        return
    f = open('DA_%s.txt'%studyName, 'w')
    
    tmp=np.array(sd.execute('SELECT DISTINCT %s FROM six_results,six_input where id=six_input_id'%names),dtype=rectype)
    Elhc,Einj = sd.execute('SELECT emitn,gamma from six_beta LIMIT 1')[0]
    print Elhc,Einj
    #print tmp.size
    for angle in np.unique(tmp['angle']):   
    # for angle in anglearray:
        
        ich1 = 0
        ich2 = 0
        ich3 = 0
        icount = 1.
        itest = 0
        iin  = -999
        iend = -999
        alost1 = 0.
        alost2 = 0.
        achaos = 0
        achaos1 = 0
        mask=[(tmp['betx']>0) & (tmp['emitx']>0) & (tmp['bety']>0) & (tmp['emity']>0) & (tmp['angle']==angle)]
        inp=tmp[mask]
        #print inp.size
        if inp.size<2 : 
            print 'not enought data for angle = %s' %angle
            break

        zero = 1e-10
        for itest in range(0,inp.size):
            if inp['betx'][itest]>zero and inp['emitx'][itest]>zero : 
                inp['sigx1'][itest] =  math.sqrt(inp['betx'][itest]*inp['emitx'][itest]) 
            if inp['bety'][itest]>zero and inp['emity'][itest]>zero : 
                inp['sigy1'][itest] =  math.sqrt(inp['bety'][itest]*inp['emity'][itest]) 
            if inp['betx'][itest]>zero and inp['emitx'][itest]>zero and inp['bety'][itest]>zero and inp['emity'][itest]>zero: 
                itest+=1

        iel=inp.size-1
        rat=0

        if inp['sigx1'][0]>0:  
            rat=pow(inp['sigy1'][0],2)*inp['betx'][0]/(pow(inp['sigx1'][0],2)*inp['bety'][0])
        if pow(inp['sigx1'][0],2)*inp['bety'][0]<pow(inp['sigy1'][0],2)*inp['betx'][0]:
            rat=2        
        if inp['emity'][0]>inp['emitx'][0]:
            rat=0
            dummy=np.copy(inp['betx'])
            inp['betx']=inp['bety']
            inp['bety']=dummy
            dummy=np.copy(inp['betx2'])
            inp['betx2']=inp['bety2']
            inp['bety2']=dummy
            dummy=np.copy(inp['sigx1'])
            inp['sigx1']=inp['sigy1']
            inp['sigy1']=dummy
            dummy=np.copy(inp['sigxavg'])
            inp['sigxavg']=inp['sigyavg']
            inp['sigyavg']=dummy
            dummy=np.copy(inp['emitx']) 
            inp['emitx']=inp['emity']
            inp['emity']=dummy

        sigma=math.sqrt(inp['betx'][0]*Elhc/Einj)
        if abs(inp['emity'][0])>0 and abs(inp['sigx1'][0])>0:
            if abs(inp['emitx'][0])<zero :
                rad=math.sqrt(1+(pow(inp['sigy1'][0],2)*inp['betx'][0])/(pow(inp['sigx1'][0],2)*inp['bety'][0]))/sigma
            else:
                rad=math.sqrt((abs(inp['emitx'][0])+abs(inp['emity'][0]))/abs(inp['emitx'][0]))/sigma
        rad1=math.sqrt(1+pow((inp['sigyavg'][0]*math.sqrt(inp['betx'][0])-inp['sigxavg'][0]*math.sqrt(inp['bety2'][0]))/(inp['sigxavg'][0]*math.sqrt(inp['bety'][0])-inp['sigyavg'][0]*math.sqrt(inp['betx2'][0])),2))/sigma
        for i in range(0,iel+1):
            if ich1 == 0 and (inp['distp'][i] > 2. or inp['distp'][i]<=0.5):
                ich1 = 1
                achaos=rad*inp['sigx1'][i]
                iin=i
            if ich3 == 0 and inp['dist'][i] > 1e-2 :
                ich3=1
                iend=i
                achaos1=rad*inp['sigx1'][i]
            if ich2 == 0 and  (inp['sturns1'][i]<inp['turn_max'][i] or inp['sturns2'][i]<inp['turn_max'][i]):
                ich2 = 1
                alost2 = rad*inp['sigx1'][i]

        if iin != -999 and iend == -999 : 
            iend=iel  

        if iin != -999 and iend >= iin :    
            for i in range(iin,iend+1) :
                alost1 += (rad1/rad) * (inp['sigxavg'][i]/inp['sigx1'][i])
            alost1 = alost1/(float(iend)-iin+1)
        else:
            alost1 = 1.0
        if alost1 >= 1.1 or alost1 <= 0.9:
            alost1= -1. * alost1 
        else:
            alost1 = 1

        alost1=alost1*alost2
        print  "study=%s angle = %s achaos= %s achaos1= %s alost1= %s alost2= %s rad*sigx1[1]= %s rad*sigx1[iel]= %s" %(studyName,angle,achaos,achaos1,alost1,alost2,rad*inp['sigx1'][0],rad*inp['sigx1'][iel]) 
        f.write('%s %s %s %s %s %s %s %s \n'%(studyName,angle,achaos,achaos1,alost1,alost2,rad*inp['sigx1'][0],rad*inp['sigx1'][iel]))
    f.close()


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
    main1(sys.argv[1])











