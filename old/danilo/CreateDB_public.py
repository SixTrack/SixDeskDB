#!/usr/bin/python

# this create a sqlite3 database for each study. You only have to provide the name of the study to analyse:
# python CreateDB.py <write_your_fancy_study_name_here>
# the only thing you have to EDIT is : 
# workarea: that should point where you run the script and where sixdesk module sit.
# path: that should point where studies directory is
# A db with the name of you study will be produced in the same workarea


import sys
import getopt
from sixdesk import *

# def main():
try:
    opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
except getopt.error, msg:
    print msg
    print "for help use --help"
    sys.exit(2)
for o, a in opts:
    if o in ("-h", "--help"):
        print "use: CreateDB_public <study_name>"
        sys.exit(0)
if len(args)<1 :
    print "too few options: please provide <study_name>"
    sys.exit()
if len(args)>1 :
    print "too many options: please provide only <study_name>"
    sys.exit()
    
# PART TO BE EDITED ========================================================================
# workarea='/afs/cern.ch/user/d/dbanfi/SixTrack_NEW/'   # PUT HERE WORKAREA (all results will be written here)
# workarea='/afs/cern.ch/user/d/dbanfi/SixTrack_NEW2'  
workarea='/afs/cern.ch/work/d/dbanfi/private/SixTrack_NEW/'
# path='/afs/cern.ch/user/d/dbanfi/SixTrack_3/sd/sixjobs/studies/%s/sixdeskenv'%args[0]    #PUT HERE path to studies directory
path='/afs/cern.ch/user/b/boinc/scratch0/boinc/sd_%s'%args[0]  #PUT HERE path to studies directory
# path='/afs/cern.ch/user/d/dbanfi/SixTrack_3_Slice/sd/sixjobs/studies/%s/sixdeskenv'%args[0]    #PUT HERE path to studies directory
# DO NOT EDIT BEYOND HERE IF YOU'RE NOT REALLY SURE  =======================================     

database='%s/%s.db'%(workarea,args[0])
sd=SixDeskDB(database)
print path
if os.path.isdir(path):
    st=SixDeskDir(path)
    # st.replace_scratch('/Volumes/TrekStor/WorkArea/Sixtrack_3')
    # sd.add_results(st.iter_results())
    sd.add_results(st.iter_results_boinc())
else:
    print "ERROR: file  %s does not exists!" %(path)


# if __name__ == "__main__":
    # main()



