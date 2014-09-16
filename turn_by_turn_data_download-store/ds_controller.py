# --------------------------------------------------------------------------------------------------------------
# TURN BY TURN TRACKING DATA DOWNLOAD AND STORE CONTROLLER
# GIOVANNA CAMPOGIANI
# LAST MODFIIED: 16/09/2014
# This script contains:
# - the user interface to inser the data relative to the studio whose data needs to be analysed
# - the calls to the routines that actually download, store and analyse the data
# --------------------------------------------------------------------------------------------------------------

'''
This is the controller interface of the software that is meant 
to analyse the turn by turn data of the particle tracking jobs
done by Sixtrack through the SixDesk platform.
It uses the following utilities:
tracking_parameters, castor_script, data_to_db, distribution, plot_results_module
'''

from numpy import *
from tracking_parameters import seeds_calc,amplitudes_calc,angles_calc
from castor_script import downloader, remove_data
from data_to_db import create_db

'''The user should insert some a priori data (from sixdeskenv) about the simulation output that has to be analysed,
in particular the following values:
studio name, initial and end seeds for madx, normalised emittance, relative gamma factor,
energy, initial and end amplitudes in sigmas units, amplitude interval, initial angle, end angle, angle step,
relative momentum spread, particles number'''

# --------------------------------------------------------------------------------------------------------------
# USER INPUT BLOCK
# --------------------------------------------------------------------------------------------------------------

studio='4tev_long_withbb' 			# study name
tunes='62.31_60.32' 				# tunes 											[]
exp_turns=5 						# order of magnitude of the number of turns tracked []
seedinit=1. 						# initial seed for madx 							[]
seedend=10. 						# end seed for madx 								[]
emit=2.5 							# normalised emittance 								[mm-rad]
bunch=7.5 							# bunch length 										[cm]
gamma_rel=4263.156 					# relative gamma factor 							[]
nsi=4. 								# initial amplitude in units of sigma 				[]
nsf=12. 							# end amplitude in units of sigma 					[]
nstep=1 							# amplitude interval 								[]
ki=1. 								# initial angle 									[deg]
kmax=5. 							# end angle 										[deg]
kend=5. 							# angles number 									[]
delta0=0.0001 						# relative momentum deviation amplitude 			[]
np=30 								# particles number 									[]

# --------------------------------------------------------------------------------------------------------------
# END OF USER INPUT BLOCK
# --------------------------------------------------------------------------------------------------------------







# --------------------------------------------------------------------------------------------------------------
# DOWLOAD DATA BLOCK
# --------------------------------------------------------------------------------------------------------------

np=2*np

seeds=seeds_calc(seedinit,seedend)
ampls=amplitudes_calc(nsi,nstep,nsf)
angles=angles_calc(ki,kmax,kend)

tbt_data=downloader(studio, seeds, ampls, angles, tunes, exp_turns,np)
remove_data(studio)

print 'Download of the data from CASTOR completed'


# --------------------------------------------------------------------------------------------------------------
# STORE DATA BLOCK
# --------------------------------------------------------------------------------------------------------------

dbname=create_db(tbt_data,studio,seedinit,seedend,nsi,nsf,angles)
