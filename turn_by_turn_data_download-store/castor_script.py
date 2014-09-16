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

def	pre_stage_castor(CASTOR_HOME,studio, seeds, ampls, angles, tunes, exp_turns):
	'''prestages the files to be downloaded from CASTOR'''
	staged_files=''
	POOL=os.environ.get('STAGE_SVCCLASS')
	file_list='to_download.files'
	
	for mad_seed in seeds:
		for amp in ampls:
			for angle in angles:
				subDir=CASTOR_HOME+studio+'/'+str(int(mad_seed))+'/simul/'+tunes+'/'+amp+'/e'+str(exp_turns)+'/'+str(int(angle))
				filename=studio+'%'+str(int(mad_seed))+'%s%'+tunes+'%'+amp+'%'+str(exp_turns)+'%'+str(int(angle))+'.tar'
				os.system('echo %s/%s | awk \'{print $NF}\' >> %s' %(subDir,filename,file_list))

	os.system('stager_get -f %s -S %s' %(file_list,POOL))
	
	return file_list
	
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
	dest_path=studio+'/'+str(int(mad_seed))+'/'+amp+'/'+str(int(angle))
	os.system('mkdir -p %s' %dest_path)
	return dest_path

def download_data_here(CASTOR_HOME,studio, mad_seed, amp, angle, tunes, exp_turns):
	'''downloads the .tar compressed data from CASTOR'''
	subDir=CASTOR_HOME+studio+'/'+str(int(mad_seed))+'/simul/'+tunes+'/'+amp+'/e'+str(exp_turns)+'/'+str(int(angle))
	filename=studio+'%'+str(int(mad_seed))+'%s%'+tunes+'%'+amp+'%'+str(exp_turns)+'%'+str(int(angle))+'.tar'
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
	
# THE FOLLOWING FUNCTION CALLS ALL OF THE ABOVE ONES
def downloader(studio, seeds, ampls, angles, tunes, exp_turns,np): 
	'''routine that downloads all the data and stacks it up in a dedicated dictionary'''
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
				remove_data(dest_path)
		tbt_data[mad_seed]=bunch
	
	return tbt_data
