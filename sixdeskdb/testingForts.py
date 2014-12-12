import sys
import getopt
from deskdb import *
import numpy as np
import os
from plots import *
from datafromFort import *

studyName='job_tracking'
database='%s.db'%(studyName)
if os.path.isfile(database):
    sd=SixDeskDB(studyName)
else:
    print "ERROR: file  %s does not exists!" %(database)
    sys.exit()

f11 = Fort(11, sd)
f12 = Fort(12, sd)
f13 = Fort(13, sd)
f14 = Fort(14 ,sd)
f15 = Fort(15, sd)
f16 = Fort(16 ,sd)
f17 = Fort(17 ,sd)
f18 = Fort(18 ,sd)
f19 = Fort(19 ,sd)
f20 = Fort(20 ,sd)
f21 = Fort(21 ,sd)
f22 = Fort(22 ,sd)
f23 = Fort(23 ,sd)
f24 = Fort(24 ,sd)
f25 = Fort(25 ,sd)

f27 = Fort(27 ,sd)
f28 = Fort(28 ,sd)
f40 = Fort(40 ,sd)

# f12.write()
# f13.write()
# # f14.write()
# f15.write()
# f16.write()
# f17.write()
# f18.write()
# f19.write()
# f20.write()
# f21.write()
# f22.write()
# f23.write()
# f24.write()
# f25.write()


