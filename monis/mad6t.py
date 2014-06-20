import os
from sixdeskdir import extract_kmax 
class Mad6tOut(object):
  def __init__(self,**opt):
    self.basedir=opt['sixtrack_input']
    self.LHCDescrip=opt['LHCDescrip']
    self.ista=opt['ista']
    self.iend=opt['iend']
    print "Mad6tOut basedir: %s"%self.basedir
  def get_outdirnames(self):
    out=[]
    for l in os.listdir(self.basedir):
      if l.startswith('mad.'):
        print "Mad6tOut rundir found: %s" % l
        fdir=os.path.join(self.basedir,l)
        ftime=os.path.getmtime(fdir)
        out.append((ftime,fdir))
    return [l[1] for l in sorted(out)]
  def get_outfnames(self):
    self.missing_seed=[]
    out=[]
    try:
      basedir=self.get_outdirnames()[-1]
    except IndexError:
      raise ValueError, "Mad6tOut no mad_run directory found"
    print "Mad6tOut rundir used: %s" % basedir
    for i in range(int(self.ista),int(self.iend)+1):
      out_fn=os.path.join(basedir,"%s.out.%d"%(self.LHCDescrip,i))
      if os.path.exists(out_fn):
        out.append(out_fn)
      else:
        print "Mad6tOut Error: outfn '%s' does not exists" %out_fn
        self.missing_seed.append(i)
    print "Mad6tOut found %d out file names"%len(out)
    return out
  def get_jobname(self,seed):
    return "%s_%s_mad6t_%d"%(self.workspace,self.LHCDescrip,seed)
  def check_out(self):
    self.closest2=[]
    self.closest1=[]
    self.closest0=[]
    self.kqs={}
    self.kqt={}
    for fn in self.get_outfnames():
      for l in open(fn):
        l=l.strip()
        if l.startswith('closest'):
          if l.startswith('closest2 =  '):
            self.closest2.append(float(l.split('=')[1].split(';')[0]))
          elif l.startswith('closest1 =  '):
            self.closest1.append(float(l.split('=')[1].split(';')[0]))
          elif l.startswith('closest0 =  '):
            self.closest0.append(float(l.split('=')[1].split(';')[0]))
        elif 'kmqsmax*100' in l:
          name,val=extract_kmax(l)
          self.kqs.setdefault(name,[]).append(val)
        elif 'kmqtmax*100' in l:
          name,val=extract_kmax(l)
          self.kqt.setdefault(name,[]).append(val)
    print "Mad6tOut clo0: %s"%minmaxavg(self.closest0)
    print "Mad6tOut clo1: %s"%minmaxavg(self.closest1)
    print "Mad6tOut clo2: %s"%minmaxavg(self.closest2)
    kqsmax=[max(abs(m) for m in l) for l in zip(*self.kqs.values())]
    kqtmax=[max(abs(m) for m in l) for l in zip(*self.kqt.values())]
    print "Mad6tOut kqt : %s"%minmaxavg(kqtmax)
    print "Mad6tOut kqs : %s"%minmaxavg(kqsmax)
  def get_forts_filenames(self):
    out=[]
    for fort in [2,16,8]:
      for seed in range(self.ista,self.iend+1):
        yield 'fort.%d_%d.gz'%(fort,seed)
  def check_forts(self):
    for fn in self.get_forts_filenames():
      ffn=os.path.join(self.basedir,fn)
      if os.path.exists(ffn):
        if os.path.getsize(ffn)<10:
          print "Mad6tOut %s too small"
      else:
          print "Mad6tOut %s does not exists"
  def check_all(self):
    self.check_out()
    self.check_forts()



