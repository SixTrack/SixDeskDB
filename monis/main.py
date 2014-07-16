#!/usr/bin/python

from SixDB import *
from SixDir import *
from DA_FullStat_v2 import *
from DA_FullStat_public import *
from mad6t import *
import os

if __name__ == "__main__":
  args = sys.argv[1:]
  # try:
  #   opts, args = getopt.getopt(sys.argv[1:], "h", ["help","loaddir","loaddb",/
  #   "DA","mad","join10"])
  #   # print 'opts',opts
  #   # print 'args',args
  # except getopt.error, msg:
  #   print msg
  #   print "for help use help"
  #   sys.exit(2)
  if args:
    try:
      if args[0] in ("h", "help"):
          print "use: main <option>" 
          print """loaddir <optional: studydir> LOAD A COMPLETE OR UPDATED STUDY 
          INTO DATABASE"""
          print "loaddb <studyname> CREATE STUDY FROM DATABASE"
          print "optional switch\nbasedir:\tSpecify dir for study creation\n"
          print "verbose:\tif used prints messages\n"
          print "dryrun:\tif used will not create files or directories"
          print "DA <studyname> CREATE DARE FILES FOR STUDIES"
          print "mad <studyname> MAD RUN ANALYSIS AND CHECK"
          print "join10 <studyname> PERFORM RUN_JOIN10 FOR STUDY PROVIDED"
          print "info <studyname> GET INFO FOR STUDY PROVIDED"
      if args[0] in ("loaddir","loaddir"):
        if len(args)==1:
          a = SixDB()
        if len(args)==2:
          a = SixDB(args[1])
          a.st_control()
          # a.st_mask()
          a.st_env()
          a.st_mad6t_run()
          a.st_mad6t_run2()
          a.st_mad6t_results()
          a.st_six_beta()
          a.st_six_input()
          a.st_six_results()
        else:
          print "too many arguments see help with h or help"
          sys.exit(0)
      elif args[0] in ("loaddb","loaddb"):
        dryrun = verbose = False
        if "-verbose" in args:
          verbose = True
          del args[args.index("-verbose")]
        if "-dryrun" in args:
          dryrun = True
          del args[args.index("-dryrun")]
        if len(args) == 2:
          a = SixDir(args[1],'.',verbose,dryrun)
        elif len(args) == 3:
          a = SixDir(args[1],args[2],verbose,dryrun)
        else:
          print "invalid see help with h or help"
          sys.exit(0)
        if a:
          a.load_extra()
          exit(0)
          a.load_mad6t_run()
          # a.load_mad6t_run2()
          a.load_mad6t_results()
          a.load_six_beta()
          a.load_six_input_results()
      elif args[0] in ("info"):
        if len(args)==2:
          a = SixDir(args[1])
          a.info()
        else:
          print "invalid see help with h or help"
          sys.exit(0)
      elif args[0] in ("DA","DA"):
        if len(args)==2:
          main2(args[1])
          main1(args[1])
        else:
          print "invalid see help with h or help"
          sys.exit(0)
      elif args[0] in ("mad","mad"):
        if len(args)==2:
          a = SixDir(args[1])
          m = Mad6tOut(**a.env_var)
          m.check_all()
        else:
          print "invalid see help with h or help"
          sys.exit(0)
      elif args[0] in ("join10","join10"):
        if len(args)==2:
          a = SixDir(args[1])
          if a.get_missing_fort10 == 0 and a.get_incomplete_fort10 == 0:
            a.join10
        else:
          print "invalid see help with h or help"
          sys.exit(0)
      else:
        print 'invalid for help use help or h'
    except Exception, e:
      print e.__doc__
      print e.message
  else:
      print "too few options: please see help with h or help"
      sys.exit()
# print opts
# print args
# a = SixDB('./files/w7/sixjobs/') #SixDB.py
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
# m = Mad6tOut(**a.env_var)
# print m.get_outdirnames()
# print m.get_outfnames()
# m.check_all()
