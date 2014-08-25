from sixdeskdb import *
from sixdesk import *


sd=SixDeskDB('rnubbe4.db')
sd.add_results(SixDeskDir('/afs/cern.ch/user/r/rdemaria/w10/sixjobs/studies/nubbe4').iter_results())

