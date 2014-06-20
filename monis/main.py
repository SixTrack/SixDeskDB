from SixDB import *
from SixDir import *
from DA_FullStat_v2 import *
from DA_FullStat_public import *
from mad6t import *

a = SixDB('./files/w7/sixjobs/') #SixDB.py
# a.st_control()
# a.st_mask()
# a.st_env()
# a.st_mad6t_run()
# a.st_mad6t_run2()
# a.st_mad6t_results()
# a.st_six_beta()
# a.st_six_input_results()
# a = SixDir('jobslhc31b_inj55_itv19') #SixDir.py
# a.load_extra()
# a.load_mad6t_run()
# a.load_mad6t_run2()
# a.load_mad6t_results()
# a.load_six_beta()
# a.load_six_input_results()
# a.join10()
# main1(a.env_var['LHCDescrip']) #DA_FullStat_public.py
# main1(a.env_var['LHCDescrip']) #DA_FullStat_v2.py
env_var = a.env_var
m = Mad6tOut(**env_var)
# print m.get_outdirnames()
# print m.get_outfnames()
m.check_all()