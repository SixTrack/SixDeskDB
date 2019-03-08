from .postPlot import *
import os
import shutil
import gzip

def createStructuresAndPlots(nSeeds, nAngles, a0, a1, nturns, exp):
    
    directory="job_tracking_plot/%s/simul/62.31_60.32/6-14/e5/.%s/"
    
    for seed in range(1, nSeeds+1):
        for angle in range(1, nAngles+1): 
            path = directory%(seed,angle)
            if not os.path.exists(path):
                os.makedirs(path)
            # plot_averem(seed,angle,nturns,a0,a1, path)
            # plot_kvar(path,seed,nturns,a0,a1,exp)
            # plot_distance(seed,angle,nturns,a0,a1, path)
            # plot_maxslope(seed,angle,nturns,a0,a1, path)
            # plot_smear(seed,angle,nturns,a0,a1, path)
            # plot_survival(seed,angle,nturns,a0,a1, path)
            ## plot short
            plot_tunedp(seed,angle,nturns,a0,a1, path)
            # shutil.copy2("fort.40.%s"%seed, "%sfort.40"%(path))
            # shutil.copy2("fort.28.%s"%seed, "%sfort.28"%(path))
            # # shutil.copy2("fort.15.%s.%s"%(seed,angle), "%sfort.15"%(path))
            # shutil.copy2("fort.11.%s.%s"%(seed,angle), "%sfort.11"%(path))

            # f15 = open("fort.15.%s.%s"%(seed,angle), 'rb')
            # f15_out = gzip.open("%sfort.15"%(path), 'wb')
            # f15_out.writelines(f15)
            # f15_out.close()
            # f15.close()

            # f30 = open("fort.15.%s.%s"%(seed,angle), 'rb')
            # f30_out = gzip.open("%sfort.30"%(path), 'wb')
            # f30_out.writelines(f30)
            # f30_out.close()
            # f30.close()
            
if __name__ == '__main__':
    sd=SixDeskDB("job_tracking.db")
    nSeeds=3
    nAngles=19
    nturns=100000
    a0 = 6
    a1 = 14
    tune="62.31_60.32"
    exp = 5
    expDir="e%d" %exp
    #createStructuresAndPlots(sd, nSeeds, nAngles, a0, a1, nturns,exp)
    print(a1)
    # plot_averem(sd, 1, 0, a0, a1, nturns)
    # plot_kvar(sd, 1, 0, a0, a1, nturns, exp)
    plot_distance(sd, 1, 0, a0, a1, nturns)
    # plot_maxslope(sd, 1, 0, a0, a1, nturns)
    # plot_smear(sd, 1, 0, a0, a1, nturns)
    # plot_survival(sd, 1, 0, a0, a1, nturns)
