from sixdesk import *
import numpy as np
import math
import matplotlib.pyplot as plt

beta='15'
kind='6D1'    #6D1 or 6D_err
name='st%s_%s' %(beta,kind)
sd=SixDeskDB('%s.db'%name)

f = open('DA_%s.txt'%name, 'w')

xingarray=range(390,940,50)
intearray  = [1.6,1.8,2.0,2.2,2.4,2.6,2.8,3.0]
anglearray = range(5,90,5)
# xingarray=[390]
# intearray  = [1.8]
# anglearray = range(5,90,5)
Elhc=2.5      #normalized emittance in "general input"
Einj=7460.5   #gamma in "general input"
rectype=[('study','S100'),('betx'    ,'float'),('bety'    ,'float'),('sigx1'   ,'float'),('sigy1'   ,'float'),('emitx'   ,'float'),('emity'   ,'float'),
        ('sigxavg' ,'float') ,('sigyavg' ,'float'),('betx2'   ,'float'),('bety2'   ,'float'),('distp'   ,'float'),('dist'    ,'float'),
        ('sturns1' ,'int')   ,('sturns2' ,'int')  ,('turn_max','int')  ,('amp1'    ,'float'),('amp2'    ,'float'),('angle'   ,'int')]

names='study,betx,bety,sigx1,sigy1,emitx,emity,sigxavg,sigyavg,betx2,bety2,distp,dist,sturns1,sturns2,turn_max,amp1,amp2,angle'
tmp=np.array(sd.execute('SELECT DISTINCT %s FROM results '%names),dtype=rectype)

for xing in xingarray :
    for binte in intearray:
        for angle in anglearray:
            ich1 = 0
            ich2 = 0
            ich3 = 0
            icount = 1.
            itest = 0
            iin  = 0
            iend = 0
            alost1 = 0.
            alost2 = 0.
            achaos = 0
            achaos1 = 0
            study='st%s_%s_%s_%s' %(beta,xing,binte,kind)
            mask=[(tmp['betx']>0) & (tmp['emitx']>0) & (tmp['bety']>0) & (tmp['emity']>0) & (tmp['angle']==angle)& (tmp['study']==study)]
            inp=tmp[mask]
            if inp.size<2 : 
                print 'not enought data for study = %s' %(study)
                break

            zero = 1e-10
            for itest in range(0,inp.size):
                if inp['betx'][itest]>zero and inp['emitx'][itest]>zero : inp['sigx1'][itest] =  math.sqrt(inp['betx'][itest]*inp['emitx'][itest]) 
                if inp['bety'][itest]>zero and inp['emity'][itest]>zero : inp['sigy1'][itest] =  math.sqrt(inp['bety'][itest]*inp['emity'][itest]) 
                if inp['betx'][itest]>zero and inp['emitx'][itest]>zero and inp['bety'][itest]>zero and inp['emity'][itest]>zero: itest+=1

            iel=inp.size-1
            # print "iel = %s"%iel
            rat=0

            if inp['sigx1'][0]>0:  
                rat=pow(inp['sigy1'][0],2)*inp['betx'][0]/(pow(inp['sigx1'][0],2)*inp['bety'][0])
            if pow(inp['sigx1'][0],2)*inp['bety'][0]<pow(inp['sigy1'][0],2)*inp['betx'][0]:
                rat=2        
            if inp['emity'][0]>inp['emitx'][0]:
                rat=0
                # print "SWAP X and Y axis!!"
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
            # print "rat = %s"%rat
    
            sigma=math.sqrt(inp['betx'][0]*Elhc/Einj)
            # print "sigma = %s"%sigma
            if abs(inp['emity'][0])>0 and abs(inp['sigx1'][0])>0:
                if abs(inp['emitx'][0])<0 :
                    rad=math.sqrt(1+(pow(inp['sigy1'][0],2)*inp['betx'][0])/(pow(inp['sigx1'][0],2)*inp['bety'][0]))/sigma
                else:
                    rad=math.sqrt((abs(inp['emitx'][0])+abs(inp['emity'][0]))/abs(inp['emitx'][0]))/sigma
            rad1=math.sqrt(1+pow((inp['sigyavg'][0]*math.sqrt(inp['betx'][0])-inp['sigxavg'][0]*math.sqrt(inp['bety2'][0]))/(inp['sigxavg'][0]*math.sqrt(inp['bety'][0])-inp['sigyavg'][0]*math.sqrt(inp['betx2'][0])),2))/sigma
            for i in range(1,iel+1):
                if ich1 == 0 and (inp['distp'][i] > 2. or inp['distp'][i]<=0.5):
                    ich1 = 1
                    achaos=rad*inp['sigx1'][i]
                    iin=i
                    # print "TRIGGERED CAOS 1!! Set iin = %s"%iin
                if ich3 == 0 and inp['dist'][i] > 1e-2 :
                    ich3=1
                    iend=i
                    achaos1=rad*inp['sigx1'][i]
                    # print "TRIGGERED CAOS 3!!  Set iend = %s"%iend
                if ich2 == 0 and  (inp['sturns1'][i]<inp['turn_max'][i] or inp['sturns2'][i]<inp['turn_max'][i]):
                    ich2 = 1
                    alost2 = rad*inp['sigx1'][i]
                    # print "TRIGGERED CAOS 2!!"

            if iin != 0 and iend == 0 : iend=iel  

            if iin != 0 and iend >= iin :    
                for i in range(iin,iend+1) :
                    alost1 += rad1/rad * inp['sigxavg'][i]/inp['sigx1'][i]
                alost1 = alost1/(float(iend)-iin+1)
                # print "final alost1 factor= %s"%alost1

            alost1=alost1*alost2
            print  "binte = %s xing = %s angle = %s achaos= %s achaos1= %s alost1= %s alost2= %s rad*sigx1[1]= %s rad*sigx1[iel]= %s" %(binte,xing,angle,achaos,achaos1,alost1,alost2,rad*inp['sigx1'][1],rad*inp['sigx1'][iel]) 
            f.write('%s %s %s %s %s %s %s %s %s \n'%(binte,xing,angle,achaos,achaos1,alost1,alost2,rad*inp['sigx1'][1],rad*inp['sigx1'][iel]))
f.close()
