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

studio= LHCDescrip 			# study name
tunes=  'tunex_tuney' 				    # tunes 											                      []
exp_turns= turnsle  						        # order of magnitude of the number of turns tracked []
seedinit=  istamad			        # initial seed for madx 							              []
seedend=  iendmad 						        # end seed for madx 								                []
emit= emit 							          # normalised emittance 								              [mm-rad]
gamma_rel= gamma 					    # relative gamma factor 							              []
nsi= ns1l 								          # initial amplitude in units of sigma 				      []
nsf= ns2l 							          # end amplitude in units of sigma 					        []
nstep= nsincl 							          # amplitude interval 								                []
ki= kini; 								          # initial angle 									                  [deg]
kmax= kmaxl 							          # end angle 										                    [deg]
kend= kendl 							          # angles number 									                  []
delta0= dpini 						      # relative momentum deviation amplitude 			      []
np= sixdeskpairs								          # particles number 									                []

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

print ('Download of the data from CASTOR completed')


# --------------------------------------------------------------------------------------------------------------
# STORE DATA BLOCK
# --------------------------------------------------------------------------------------------------------------

dbname=create_db(tbt_data,studio,seedinit,seedend,nsi,nsf,angles)
print ('Turn by turn tracking data successfully stored in %s.db' %dbname)
