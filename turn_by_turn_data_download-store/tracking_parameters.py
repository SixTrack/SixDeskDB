# --------------------------------------------------------------------------------------------------------------
# TRACKING PARAMETERS CALCULATOR
# GIOVANNA CAMPOGIANI
# LAST MODFIIED: 16/09/2014
# This script returns some vectors containing information relative to the parameters set for a specific
# SixDesk study
# --------------------------------------------------------------------------------------------------------------

'''tracking_parameters help: utility to calculate the number of seeds, amplitudes and angles tracked by Sixdesk in a specific study,
starting from the input of the user.'''

from numpy import *


def seeds_calc(seedinit,seedend):
	'''input: istamad, iendmad
	output: array containing seeds values 
	'''
	seeds=arange(seedinit,seedend+1)
	return seeds

def amplitudes_calc(nsi,nstep,nsf):
	'''input: nsi,nstep,nsf (start, step, end amplitudes)
	output: array of amp0_amp1 strings (amplitude folders names)
	'''
	n=nsi
	ampls=[]

	while (n<nsf):
		ampls.append(str(int(n))+'_'+str(int(n+nstep)))
		n+=nstep
	
	return ampls

def angles_calc(ki,kmax,kend):
	'''input: ki,kmax,kend (start,max,end angles)
	output: array containing all the angles tracked (angles folders names)
	'''
	k=ki
	anglestep=float(90./(kmax+1.))
	dim=kmax
	angles=zeros(dim)
	i=0
	
	while (k<=kend):
		angles[i]=anglestep*k
		i+=1
		k+=1
	
	return angles
