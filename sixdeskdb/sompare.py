import numpy as np 

# a2 = np.loadtxt(dir1%(fort,i))
# b2 = np.loadtxt(dir2%(fort,i))
# a2 = np.round(a2,3)
# b2 = np.round(b2,3)
# a2!=b2
# np.sum(a2!=b2)
dir1 = '/afs/cern.ch/user/x/xvallspl/gsoc/xavi/SixDesk/sixdeskdb/fort.%s.3.%s'
dir2 = '/afs/cern.ch/user/x/xvallspl/w1/sixjobs/plot/fort.%s.1.%s'
forts=[12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]

def compareFile(fort, nAngle, prec, diff=True):
    try:
        a2 = np.loadtxt(dir1%(fort,angle))
    except:
        return
    b2 = np.loadtxt(dir2%(fort,angle))
    a2 = np.round(a2,prec)
    b2 = np.round(b2,prec)
    if np.sum(a2!=b2):
        print 'fort.%s.3.%s: %s' %(fort, nAngle, np.sum(a2!=b2))
    if diff:
        vec=np.column_stack([a2[a2!=b2], b2[a2!=b2]])
        return vec

def compareFort(fort, prec, diff=False):
    for nAngle in range(1,19+1):
            compareFile(fort, nAngle, prec, diff)

def compareAll(prec):
    for fort in forts:
        compareFort(fort, prec, False)
        

		
