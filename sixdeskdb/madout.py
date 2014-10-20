import numpy as np


def check_mad_out(data,resname):
  out={}
  seeds=[]
  for seed,fh in data:
    seeds.append(seed)
    for l in fh:
      if l.startswith('closest'):
        nclosest=int(l.split('closest')[1][0])
        vclosest=float(l.split('=')[1].split(';')[0])
        out.setdefault('closest%d'%nclosest,[]).append(vclosest)
      elif 'max*100' in l:
        name,val=extract_kmax(l)
        out.setdefault(name,[]).append(val)
  for k,vals in sorted(out.items()):
    print "%-10s: %s"%(k,minmaxavg(vals))
  keys,table=zip(*sorted(out.items()))
  table=zip(*table)
  fh=open(resname,'w')
  fh.write('seed,%s\n'%(','.join(keys)))
  for seed,row in zip(seeds,table):
     fh.write('%d,%s\n'%(seed,','.join(map(str,row))))
  fh.close()


def extract_kmax(l):
  name,val=l.split('=')
  name=name.split('/')[0]
  val=float(val.split(';')[0])
  return name,val

def minmaxavg(l,fmt="%13e"):
  if len(l)>0:
      l=np.array(l)
      mi=l.min()
      ma=l.max()
      av=l.mean()
      tmp="min %s avg %s max %s"%(fmt,fmt,fmt)
      return tmp%(mi,av,ma)
  else:
      return "no data to find min and max"



