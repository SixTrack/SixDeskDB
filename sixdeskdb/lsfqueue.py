import os
import time

def parse_bjobs():
  cmd='/usr/bin/bjobs'
  out={}
  fh=os.popen('%s -W -a' %cmd)
  fh.readline()
  for l in fh:
    job=LSFJob(l.split())
    out[job.job_name]=job
  return out

def jobs_stats(jobs):
  pending=[v for _,v in list(jobs.items()) if v.stat=='PEND']
  running=[v for _,v in list(jobs.items()) if v.stat=='RUN']
  done=[v for _,v in list(jobs.items()) if v.stat=='DONE']
  print("Jobs running : %d" % len(running))
  print("Jobs pending : %d" % len(pending))
  print("Jobs just done: %d" % len(done))


class LSFJob(tuple):
  __slots__=()
  jobid          =property(lambda x: x[ 0])
  user           =property(lambda x: x[ 1])
  stat           =property(lambda x: x[ 2])
  queue          =property(lambda x: x[ 3])
  from_host      =property(lambda x: x[ 4])
  exec_host      =property(lambda x: x[ 5])
  job_name       =property(lambda x: x[ 6])
  submit_time    =property(lambda x: x[ 7])
  proj_name      =property(lambda x: x[ 8])
  cpu_used       =property(lambda x: x[ 9])
  mem            =property(lambda x: x[10])
  swap           =property(lambda x: x[11])
  pids           =property(lambda x: x[12])
  start_time     =property(lambda x: x[13])
  finish_time    =property(lambda x: x[14])
  def run_since(self):
    newdate=time.strftime('%Y/')+self.start_time
    t=time.mktime(time.strptime(newdate,'%Y/%m/%d-%H:%M:%S'))
    return time.time()-t



