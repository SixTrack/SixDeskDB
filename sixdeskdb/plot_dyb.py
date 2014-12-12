#!/usr/bin/python

# python re-implementation of readplotb.f + read10b.f
# NOTA: please use python version >=2.6   

import sys
import getopt
from deskdb import *
import numpy as np
import os
from plots import *
import sqlite3



def readplotb(studyName):
    database='%s.db'%(studyName)
    if os.path.isfile(database):
        sd=SixDeskDB(studyName)
    else:
        print "ERROR: file  %s does not exists!" %(database)
        sys.exit()

    dirname=sd.mk_analysis_dir()
    rectype=[('six_input_id','int'), ('row_num','int'),
             ('seed','int'), ('qx','float'),('qy','float'),
             ('betx','float'),('bety','float'),('sigx1','float'),
             ('sigy1','float'),('deltap','float'),('emitx','float'),
             ('emity','float'),('sigxminnld', 'float'),
             ('sigxavgnld' ,'float') ,('sigxmaxnld', 'float'),
             ('sigyminnld', 'float'),('sigyavgnld' ,'float'),
             ('sigymaxnld', 'float'),('betx2','float'),
             ('bety2','float'),('distp','float'),('dist','float'),
             ('qx_det','float'),('qy_det','float'),('sturns1' ,'int'),
             ('sturns2','int'),('turn_max','int'),('amp1','float'),
             ('amp2','float'),('angle','float'),('smearx','float'),
             ('smeary','float'),('mtime','float')]

    names=','.join(zip(*rectype)[0])
    turnsl, turnse = sd.env_var['turnsl'], sd.env_var['turnse']
    tunex, tuney = float(sd.env_var['tunex']), float(sd.env_var['tuney'])
    ns1l, ns2l = sd.env_var['ns1l'], sd.env_var['ns2l']
    sql='SELECT %s FROM results ORDER BY tunex,tuney,seed,amp1,amp2,angle'%names
    Elhc,Einj=sd.execute('SELECT emitn,gamma from six_beta LIMIT 1')[0]
    anumber=1

    ment=1000
    epsilon = 1e-38
    zero = 1e-10
    ntlint, ntlmax = 4 , 12
    iin  = -999
    iend = -999

    seeds, angles= sd.get_seeds(), sd.get_angles()
    mtime=sd.execute('SELECT max(mtime) from results')[0][0]
    final=[]
    ftot = []
    sql1='SELECT %s FROM results WHERE betx>0 AND bety>0 AND emitx>0 AND emity>0 AND turn_max=%d '%(names,turnsl)
    nPlotSeeds = sd.env_var["iend"]

    for tunex,tuney in sd.get_db_tunes():
        sixdesktunes="%s_%s"%(tunex,tuney)
        sql1+=' AND tunex=%s AND tuney=%s '%(tunex,tuney)
        for angle in angles:      
            fndot='DAres.%s.%s.%s.%d'%(sd.LHCDescrip,sixdesktunes,turnse,anumber)
            fndot=os.path.join(dirname,fndot)
            fhdot = open(fndot, 'w')
            nSeed=1
            for seed in seeds:
                name2 = "DAres.%s.%s.%s"%(sd.LHCDescrip,sixdesktunes,turnse)
                name1= '%s%ss%s%s-%s%s.%d'%(sd.LHCDescrip,seed,sixdesktunes,ns1l, ns2l, turnse,anumber)
                ich1, ich2, ich3 = 0, 0, 0
                alost1, alost2 = 0., 0.
                achaos, achaos1 = 0, 0
                icount = 1.

                tl = np.zeros(ntlmax*ntlint+1)
                al = np.zeros(ntlmax*ntlint+1)
                ichl =np.zeros(ntlmax*ntlint+1)
                for i in range(1, ntlmax):
                  for j in range(0,ntlint):
                        tl[(i-1)*ntlint+j] = int(round(10**((i-1)+(j-1)/float(ntlint))))

                tl[ntlmax*ntlint]=int(round(10**(float(ntlmax))))
                achaos, achaos1 = 0, 0
                alost1, alost2 = 0., 0.
                ilost=0
                itest=1
                fac=2.0
                fac2=0.1
                fac3=0.01

                if(np.abs(Einj)< epsilon):
                        print "ERROR: Injection energy too small"
                        sys.exit()
                sql = sql1+'AND seed=%g AND angle=%g ORDER BY amp1'%(seed,angle)
                inp = np.array(sd.execute(sql),dtype=rectype)
                if len(inp)==0:
                    msg="all particle lost for angle = %s and seed = %s"
                    print msg%(angle,seed)
                    continue
                six_id = inp['six_input_id']
                row  = inp['row_num']
                qx   = inp['qx']
                qy   = inp['qy']
                betx = inp['betx']
                bety = inp['bety']
                dist = inp['dist']
                distp  = inp['distp']
                sigx1  = inp['sigx1']
                betx2  = inp['betx2']
                bety2  = inp['bety2']
                emitx  = inp['emitx']
                emity  = inp['emity']
                smeary = inp['smeary']
                smearx = inp['smearx']
                qx_det = inp['qx_det']
                qy_det = inp['qy_det']
                sigy1  = inp['sigy1']
                deltap = inp['deltap']
                sturns1    = inp['sturns1']
                sturns2    = inp['sturns2']
                turn_max   = inp['turn_max']
                sigxavgnld = inp['sigxavgnld']
                sigyavgnld = inp['sigyavgnld']
                sigxmaxnld = inp['sigxmaxnld']
                sigxminnld = inp['sigxminnld']
                sigymaxnld = inp['sigymaxnld']
                sigyminnld = inp['sigyminnld']

                if sigx1[0]>0:
                    rat=sigy1[0]**2*betx[0]/(sigx1[0]**2*bety[0])
                if sigx1[0]**2*bety[0]<sigy1[0]**2*betx[0]:
                    rat=2
                if emity[0]>emitx[0]:
                    rat=0
                    dummy=np.copy(betx)
                    betx=bety
                    bety=dummy
                    dummy=np.copy(betx2)
                    betx2=bety2
                    bety2=dummy
                    dummy=np.copy(sigx1)
                    sigx1=sigy1
                    sigy1=dummy
                    dummy=np.copy(sigxavgnld)
                    sigxavgnld=sigyavgnld
                    sigyavgnld=dummy
                    dummy=np.copy(emitx)
                    emitx=emity
                    emity=dummy

                sigma=np.sqrt(betx[0]*Elhc/Einj)

                
                xidx=(betx>zero) & (emitx>zero)
                yidx=(bety>zero) & (emity>zero)
                # xidx, yidx = len(betx), len(bety)
                sigx1[xidx]=np.sqrt(betx[xidx]*emitx[xidx])
                sigy1[yidx]=np.sqrt(bety[yidx]*emity[yidx])
                itest = sum(betx>zero)
                # itest = len(betx)
                iel=itest-1    
                rat=0
    #############################################
                # if sigx1[0]>0:
                #     rat=sigy1[0]**2*betx[0]/(sigx1[0]**2*bety[0])
                # if sigx1[0]**2*bety[0]<sigy1[0]**2*betx[0]:
                #     rat=2
    #############################################
                if abs(emitx[0]) < epsilon and abs(sigx1[0])>epsilon and bety > epsilon:  
                    rat=sigy1[0]**2*betx[0]/(sigx1[0]**2*bety[0])
                if abs(emity[0]) > abs(emitx[0]) or rat > 1e-10:
                    rat=0
                    dummy=np.copy(betx)
                    betx=bety
                    bety=dummy
                    dummy=np.copy(betx2)
                    betx2=bety2
                    bety2=dummy
                    dummy=np.copy(sigxminnld)
                    sigxminnld=np.copy(sigyminnld)
                    sigyminnld=dummy
                    dummy=np.copy(sigx1)
                    sigx1=sigy1
                    sigy1=dummy
                    dummy=np.copy(sigxmaxnld)
                    sigxmaxnld=np.copy(sigymaxnld)
                    sigymaxnld=dummy
                    dummy=np.copy(sigxavgnld)
                    sigxavgnld=sigyavgnld
                    sigyavgnld=dummy
                    dummy=np.copy(emitx) 
                    emitx=emity
                    emity=dummy


                sigma=np.sqrt(betx[0]*Elhc/Einj)
                if abs(emity[0])>0 and abs(sigx1[0])>0:
                    if abs(emitx[0])>= epsilon :
                        eex=emitx[0]
                        eey=emity[0]
                    else:
                        eey=sigy1[0]**2/bety[0]
                        eex=sigx1[0]**2/betx[0]
                    rad=np.sqrt(1+eey/eex)/sigma
                else:
                    rad=1
                if abs(sigxavgnld[0])>zero and abs(bety[0])>zero and sigma > 0:
                    if abs(emitx[0]) < zero :
                        rad1=np.sqrt(1+(sigyavgnld[0]**2*betx[0])/(sigxavgnld[0]**2*bety[0]))/sigma
                    else:
                        rad1=(sigyavgnld[0]*np.sqrt(betx[0])-sigxavgnld[0]*np.sqrt(bety2[0]))/(sigxavgnld[0]*np.sqrt(bety[0])-sigyavgnld[0]*np.sqrt(betx2[0]))
                        rad1=np.sqrt(1+rad1**2)/sigma
                else:
                    rad1 = 1

                amin, amax = 1/epsilon, zero
                achaosPlot, achaos1Plot = achaos, achaos1
                # f30 = open('fort.30.%d.%d' %(nSeed,anumber),'a')                
                for i in range(0,iel+1):
                    # if i==0:
                    #     achaos=rad*sigx1[i] #OJO, NOMES PER READ10B
                    #     achaos1 =achaos

                    if abs(sigx1[i]) > epsilon and sigx1[i]<amin:
                            amin = sigx1[i]
                    if abs(sigx1[i]) > epsilon and sigx1[i]>amax:
                            amax=sigx1[i]
                    if ich1 == 0 and (distp[i] > fac or distp[i] < 1./fac): 
                        ich1 = 1
                        achaos=rad*sigx1[i]
                        iin=i
                    if ich3 == 0 and dist[i] > fac3 :
                        ich3=1
                        iend=i
                        achaos1=rad*sigx1[i]
                    if ich2 == 0 and (sturns1[i]<turn_max[i] or sturns2[i]<turn_max[i]):
                        ich2 = 1
                        alost2 = rad*sigx1[i]
                    for j in range(0, ntlmax*ntlint+1):
                      if (ichl[j] == 0 and  int(round(turn_max[i])) >= tl[j]) and ((int(round(sturns1[i])) < tl[j] or int(round(sturns2[i])) < tl[j])):
                          ichl[j] = 1
                          al[j-1] = rad*sigx1[i]
                    if i>0:
                      achaosPlot, achaos1Plot = achaos, achaos1
                #     f30.write("%s\t%f %f %f %f %f\n"%( name1[:39],rad*sigx1[i],distp[i],achaosPlot,alost2,rad1*sigxavgnld[i]))
                # f30.close()

                if iin != -999 and iend == -999 : iend=iel  
                if iin != -999 and iend > iin :    
                    for i in range(iin,iend+1) :
                        if(abs(rad*sigx1[i])>zero):
                            alost1 += rad1 * sigxavgnld[i]/rad/sigx1[i]
                        if(i!=iend):
                            icount+=1.
                    alost1 = alost1/icount
                    if alost1 >= 1.1 or alost1 <= 0.9:  alost1= -1.*alost1
                else:
                    alost1 = 1.0

                al = abs(alost1)*al
                alost1 = alost1*alost2

                if amin == 1/epsilon: amin = zero
                amin=amin*rad
                amax=amax*rad

                al[al==0]=amax
                alost3 = turn_max[1]
                sturns1[sturns1==zero] = 1
                sturns2[sturns2==zero] = 1            
                alost3 = min(alost3, min(sturns1),min(sturns2))

                if(seed<10):
                    name1+=" "
                if(anumber<10):
                    name1+=" " 

                if achaos==0:
                    f14Flag = 0 
                    achaos=amin
                else:
                    f14Flag = 1
                    f14 = open('fort.14.%d.%d' %(nSeed,anumber),'w')
                    f14.write('%s %s\n'%(achaos,alost3/fac))
                    f14.write('%s %s\n'%(achaos,turn_max[0]*fac))
                    f14.close()

                if abs(alost1) < epsilon: alost1=amax

                if nSeed != (nPlotSeeds +1):
                    for i in range(0, iel+1):
                        tbl="six_results"
                        sql=("UPDATE {0} SET {1}={2}, {3}={4}, {5}={6}, {7}={8}, {9}={10},"+
                            " {11}={12}, {13}={14}, {15}={16}, {17}={18}, {19}={20}, {21}={22} " +
                            " WHERE six_input_id = {23} AND row_num = {24}").format(
                            tbl, "rad", (rad*sigx1[i]), "rad1", rad1, "alost1", alost1, 
                            "alost2", alost2, "alost3", alost3, "achaos", achaos, "achaos1", achaos1, 
                            "amin", amin,"amax", amax, 'f14', f14Flag, "al", '?',  six_id[i], row[i])
                        sd.conn.cursor().execute(sql, (sqlite3.Binary(al),))

                fmt=' %-39s  %10.6f  %10.6f  %10.6f  %10.6f  %10.6f  %10.6f\n'
                fhdot.write(fmt%( name1[:39],achaos,achaos1,alost1,alost2,rad*sigx1[0],rad*sigx1[iel]))
                final.append([name2, turnsl, tunex, tuney, int(seed),
                               angle,achaos,achaos1,alost1,alost2,
                               rad*sigx1[0],rad*sigx1[iel],mtime])
                
                nSeed +=1
            anumber+=1
            fhdot.close()
    cols=SQLTable.cols_from_fields(tables.Da_Post.fields)
    # datab=SQLTable(sd.conn,'da_post',cols,tables.Da_Post.key,recreate=True)
    datab=SQLTable(sd.conn,'da_post',cols)
    datab.insertl(final)

def mk_da(studyName,force=False,nostd=False):
    database='%s.db'%(studyName)
    if os.path.isfile(database):
        sd=SixDeskDB(studyName)
    else:
        print "ERROR: file  %s does not exists!" %(database)
        sys.exit()

    dirname=sd.mk_analysis_dir()
    cols=SQLTable.cols_from_fields(tables.Da_Post.fields)
    datab=SQLTable(sd.conn,'da_post',cols)
    final=datab.select(orderby='angle,seed')
    turnse=sd.env_var['turnse']
    tunex=float(sd.env_var['tunex'])
    tuney=float(sd.env_var['tuney'])
    sixdesktunes="%g_%g"%(tunex,tuney)
    ns1l=sd.env_var['ns1l']
    ns2l=sd.env_var['ns2l']
    if len(final)>0:
        an_mtime=final['mtime'].min()
        res_mtime=sd.execute('SELECT max(mtime) FROM six_results')[0][0]
        if res_mtime>an_mtime or force is True:
            readplotb(studyName)
            final=datab.select(orderby='angle,seed')
    else:
      readplotb(studyName)
      final=datab.select(orderby='angle,seed')

    fnplot='DAres.%s.%s.%s.plot'%(sd.LHCDescrip,sixdesktunes,turnse)
    fnplot= os.path.join(dirname,fnplot)
    fhplot = open(fnplot, 'w')
    fn=0

    for angle in np.unique(final['angle']):
        fn+=1
        study= final['name'][0]
        idxangle=final['angle']==angle
        idx     =idxangle&(final['alost1']!=0)
        idxneg  =idxangle&(final['alost1']<0)
        mini, smini = np.min(np.abs(final['alost1'][idx])), np.argmin(np.abs(final['alost1'][idx]))
        maxi, smaxi = np.max(np.abs(final['alost1'][idx])), np.argmax(np.abs(final['alost1'][idx]))
        toAvg = np.abs(final['alost1'][idx])
        i = len(toAvg)
        mean = np.mean(toAvg)
        std = np.sqrt(np.mean(toAvg*toAvg)-mean**2)
        idxneg = (final['angle']==angle)&(final['alost1']<0)
        eqaper = np.where((final['alost2'] == final['Amin']))[0]
        nega = len(final['alost1'][idxneg])
        Amin = np.min(final['Amin'][idxangle])
        Amax = np.max(final['Amax'][idxangle])

        #for k in eqaper:
        #  msg="Angle %d, Seed %d: Dynamic Aperture below:  %.2f Sigma"
        #  print msg %( final['angle'][k],final['seed'][k], final['Amin'][k])

        if i == 0:
          mini  = -Amax
          maxi  = -Amax
          mean  = -Amax
        else:
          if i < int(sd.env_var['iend']):
            maxi = -Amax
          elif len(eqaper)>0:
            mini = -Amin
          print "Minimum:  %.2f  Sigma at Seed #: %d" %(mini, smini)
          print "Maximum:  %.2f  Sigma at Seed #: %d" %(maxi, smaxi)
          print "Average: %.2f Sigma" %(mean)
        print "# of (Aav-A0)/A0 >10%%:  %d" %nega
        name2 = "DAres.%s.%s.%s"%(sd.LHCDescrip,sixdesktunes,turnse)
        if nostd:
          fhplot.write('%s %d %.2f %.2f %.2f %d %.2f %.2f\n'%(name2, fn, mini, mean, maxi, nega, Amin, Amax))
        else:
          fhplot.write('%s %d %.2f %.2f %.2f %d %.2f %.2f %.2f\n'%(name2, fn, mini, mean, maxi, nega, Amin, Amax, std))
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
    #mk_da(sys.argv[1], force=True)
    readplotb(sys.argv[1])
    path='job_tracking/1/simul/62.31_60.32/6-14/e5/.1'
    nturns=100000
    a0 = 6
    a1 = 14
    seed=3
    angle=19
    # plot_averem( '%s/fort10.tgz'%path,seed,angle,nturns,a0,a1)
    # plot_distance( '%s/fort10.tgz'%path,seed,angle,nturns,a0,a1)
    # plot_maxslope('%s/fort10.tgz'%path,seed,angle,nturns,a0,a1)
    # plot_smear('%s/fort10.tgz'%path,seed,angle,nturns,a0,a1)
    # plot_survival('%s/fort10.tgz'%path,seed,angle,nturns,a0,a1)