import argparse
from SixDB import *
from SixDir import *
from DA_FullStat_v2 import *
from DA_FullStat_public import *
from mad6t import *

parser = argparse.ArgumentParser(description="SixDesk Library")
group = parser.add_mutually_exclusive_group()
group.add_argument("-loaddir", "--loaddir",type=str,nargs='?',const='.',help="LOAD A COMPLETE OR UPDATED STUDY INTO DATABASE")
group.add_argument("-loaddb", "--loaddb", type=str,nargs='?',const='.',help="CREATE STUDY FROM DATABASE")
group.add_argument("-da", "--da", type=str, help="CREATE DARE FILES FOR STUDIES")
group.add_argument("-mad", "--mad", type=str, help="MAD RUN ANALYSIS AND CHECK")
group.add_argument("-join10", "--join10", type=str, help="PERFORM RUN_JOIN10 FOR STUDY PROVIDED")
group.add_argument("-q", "--quiet", action="store_true")
args = parser.parse_args()
# print args.loaddb
if args.loaddir:
	a = SixDB(args.loaddir)
	a.info()
	a.st_control()
	a.st_mask()
	a.st_env()
	a.st_mad6t_run()
	a.st_mad6t_run2()
	a.st_mad6t_results()
	a.set_variable([['name','monis'],['LHCDescrip','jobslhc31b_inj55_itv19']])
	a.st_six_beta()
	a.st_six_input()
	a.st_six_results()
elif args.loaddb:
	print args.loaddb
	exit(0)
	a = SixDir(args)
	a.load_extra()
	a.load_mad6t_run()
	a.load_mad6t_run2()
	a.load_mad6t_results()
	a.load_six_beta()
	a.load_six_input_results()
elif args.da:
	main2(args)
	main1(args)
elif args.mad:
	a = SixDir(args)
	m = Mad6tOut(**a.env_var)
	m.check_all()
elif args.join10:
	a = SixDir(args)
	if a.get_missing_fort10 == 0 and a.get_incomplete_fort10 == 0:
		a.join10
	else:
		print "fort.10 not complete or missing cannot continue"
		sys.exit(0)
else:
    print "see program usage with -h or --help"
    exit(0)