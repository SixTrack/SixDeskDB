from  StringIO import StringIO
import re

import numpy as np

def check_mad_out(data,resname):
  out={}
  seeds=[]
  for seed,mad_out in data:
    seeds.append(seed)
    for l in StringIO(mad_out):
      if l.startswith('closest'):
        nclosest=int(l.split('closest')[1][0])
        vclosest=float(l.split('=')[1].split(';')[0])
        out.setdefault('closest%d'%nclosest,[]).append(vclosest)
      elif 'max*100' in l or 'max1*100' in l or 'max2*100' in l:
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

def extract_mad_out(fh):
  out={}
  for l in fh:
    if l.startswith('closest'):
      nclosest=int(l.split('closest')[1][0])
      vclosest=float(l.split('=')[1].split(';')[0])
      out['closest%d'%nclosest]=vclosest
    elif 'max*100' in l or 'max1*100' in l or 'max2*100' in l:
      name,val=extract_kmax(l)
      out[name]=val
    elif 'nom1 =' in l or 'nom2 =' in l or 'nom5 =' in l or 'nom8 =' in l:
      name,eq,val,sm=l.split()
      out.setdefault(name,[]).append(float(val))
    elif 'err =  ' in l or 'qx =  ' in l or 'qy =  ' in l:
      name,eq,val,sm=l.split()
      out[name]=float(val)
      #out.setdefault(name,[]).append(float(val))
      #out.setdefault(name,[]).append(float(val))
    elif l.startswith('acb'):
      name,valf,vali,lima,limb=l.split()
      valf,vali,lima,limb=map(float,(valf,vali,lima,limb))
      out[name]=[abs(valf-vali),valf,vali,limb]
  return out

