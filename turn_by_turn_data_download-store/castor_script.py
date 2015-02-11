# --------------------------------------------------------------------------------------------------------------
# TURN BY TURN TRACKING DATA DOWNLOADER
# GIOVANNA CAMPOGIANI
# LAST MODFIIED: 16/09/2014
# This script downloads turn by turn tracking data files from CASTOR repository
# --------------------------------------------------------------------------------------------------------------

'''castor_script help: to properly work the script needs to be fed with the following information:
studio=string with the study name
seeds=vector containing the madX seeds
ampls=vector containing strings of amp_amp+step for amp within (nsi,nsf)
angles=vector containing the values of the angles tracked
'''

import os
from read_fortbin import read_allfortbin
import subprocess
import time
import string
from deskdb import amp_dir,ang_dir

#direct_track/job_fma_bb_2_2e4/2/simul/62.31_60.32/6_8/e4/85.5/job_fma_bb_2_2e4%2%s%62.31_60.32%6_8%4%85.5.tar

def pre_stage_castor(CASTOR_HOME,studio, seeds, ampls, angles, tunes, exp_turns):
  '''prestages the files to be downloaded from CASTOR'''
  staged_files=''
  POOL=os.environ.get('STAGE_SVCCLASS')
  fn_list='to_download.files'
  file_list=open(fn_list,'w+') 
  for mad_seed in seeds:
    for amp in ampls:
      for angle in angles:
        subDir=CASTOR_HOME+studio+'/'+str(int(mad_seed))+'/simul/'+tunes+'/'+amp+'/e'+str(exp_turns)+'/'+angle
        filename=studio+'%'+str(int(mad_seed))+'%s%'+tunes+'%'+amp+'%'+str(exp_turns)+'%'+angle+'.tar'
        file_list.write('%s/%s\n'%(subDir,filename)) 
#        os.system('echo %s/%s | awk \'{print $NF}\' >> %s' %(subDir,filename,fn_list))#write list of files to be downloaded in to_download.files
  file_list.close()
  print('executing: stager_get -f %s -S %s' %(fn_list,POOL))
  os.system('stager_get -f %s -S %s' %(fn_list,POOL))
  return fn_list
  
def query_castor(file_list):
  '''checks if one or more files are in the STAGEIN status and reports it'''
  POOL=os.environ.get('STAGE_SVCCLASS')

  proc = subprocess.Popen(['stager_qry -f %s -S %s' %(file_list,POOL)], stdout=subprocess.PIPE, shell=True)
  (QUERY_STATUS, err) = proc.communicate()
  print QUERY_STATUS
  not_ready = 'STAGEIN' in QUERY_STATUS
  
  return not_ready
  
def delayer(file_list):
  '''delays the data download in case not all the files are ready from CASTOR'''
  not_ready=query_castor(file_list)
  while (not_ready==True):
    print ('The data is being retrieved from CASTOR')
    print ('Going on sleep mode for the next: 30\'')
    time.sleep(1800)
    query_status=query_castor(file_list)
  print ('The data is ready to be downloaded')
        
def create_destination_folder(studio, mad_seed, amp, angle, tunes, exp_turns):
  '''returns the path where the donwloaded data will be stored'''
  dest_path=studio+'/'+str(int(mad_seed))+'/'+amp+'/'+angle
  os.system('mkdir -p %s' %dest_path)
  return dest_path

def download_data_here(CASTOR_HOME,studio, mad_seed, amp, angle, tunes, exp_turns):
  '''downloads the .tar compressed data from CASTOR'''
  subDir=CASTOR_HOME+studio+'/'+str(int(mad_seed))+'/simul/'+tunes+'/'+amp+'/e'+str(exp_turns)+'/'+angle
  filename=studio+'%'+str(int(mad_seed))+'%s%'+tunes+'%'+amp+'%'+str(exp_turns)+'%'+angle+'.tar'
  status=os.system('rfcp %s/%s .' %(subDir,filename))

def unpack_data_here():
  '''unpacks the .tar archive downloaded from CASTOR'''
  os.system('tar -xf *.tar')
  os.system('gunzip *.gz')
  print ('Data unpacked')
  
def get_data_from_castor(CASTOR_HOME,studio, mad_seed, amp, angle, tunes, exp_turns):
  '''creates a local destination folder for the data from castor and unpacks the .tar inside'''
  HOME=os.environ.get('PWD')
  
  dest_path=create_destination_folder(studio, mad_seed, amp, angle, tunes, exp_turns)
  
  os.chdir(dest_path)
  os.system('echo \'Now downloading files in\' | pwd')
  download_data_here(CASTOR_HOME,studio, mad_seed, amp, angle, tunes, exp_turns)
  os.system('ls -l')
  unpack_data_here()
  
  os.chdir(HOME)
  
  return dest_path

def remove_data(dest_path):
  os.system('rm -r %s' %dest_path)
  print ('folder %s removed' %dest_path)

def set_env_castor(default=False):
  '''set environment variables for castor
  if default=true the environment variables are reset to the
  default values even if set'''
  if((os.environ.get('STAGE_HOST'))==None or default==True):
    os.environ['STAGE_HOST']='castorpublic'
    print('export STAGE_HOST=%s'%(os.environ.get('STAGE_HOST')))
  if((os.environ.get('STAGE_SVCCLASS'))==None or default==True):
    os.environ['STAGE_SVCCLASS']='default'
    print('export STAGE_SVCCLASS=%s'%(os.environ.get('STAGE_SVCCLASS')))
  if((os.environ.get('CASTOR_HOME'))==None or default==True):
    os.environ['CASTOR_HOME']='/castor'+(os.environ.get('HOME')[4:])
    print('export CASTOR_HOME=%s'%(os.environ.get('CASTOR_HOME')))

def check_env_castor():
  print('STAGE_HOST=%s'%(os.environ.get('STAGE_HOST')))
  print('STAGE_SVCCLASS=%s'%(os.environ.get('STAGE_SVCCLASS')))
  print('CASTOR_HOME=%s'%(os.environ.get('CASTOR_HOME')))
  
# THE FOLLOWING FUNCTION CALLS ALL OF THE ABOVE ONES
def downloader(studio, seeds, ampls, angles, tunes, exp_turns,np,setenv=True,deletedata=False): 
  '''routine that downloads all the data and stacks it up in a dedicated dictionary'''
  set_env_castor(setenv)#set environment variables for castor 
  CASTOR_HOME=os.environ.get('CASTOR_HOME')+'/direct_track/'

  file_list=pre_stage_castor(CASTOR_HOME,studio, seeds, ampls, angles, tunes, exp_turns)
  delayer(file_list)
  os.system('rm %s' %file_list)
  
  tbt_data={} #turn by turn data container
  part=0
  i=0
  
  for mad_seed in seeds:
    pID_bias=0
    bunch={}
    for amp in ampls:
      for angle in angles:
        dest_path=get_data_from_castor(CASTOR_HOME,studio, mad_seed, amp, angle, tunes, exp_turns)
        head,part=read_allfortbin(pID_bias,basedir='./%s' %dest_path)
        i=i+1
        pID_bias=np*i
        bunch.update(part)
        if(deletedata):
          remove_data(dest_path)
    tbt_data[mad_seed]=bunch
  
  return tbt_data
