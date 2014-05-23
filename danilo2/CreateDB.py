from sixdesk import *

for binte in [1.6,1.8,2.0,2.2,2.4,2.6,2.8]:
    for xangle in range(390,940,50):
        # database='st15_%s_%s_noBB_err.db'%(xangle,binte)
        # database='st15_%s_%s_6D1.db'%(xangle,binte)

        # database='st15.db'
        # path='/afs/cern.ch/user/d/dbanfi/SixTrack_2/sd/sixjobs/studies/st15_%s_%s/sixdeskenv'%(xangle,binte)

        # database='st15_6D1.db'
        # path='/afs/cern.ch/user/d/dbanfi/SixTrack_2/sd/sixjobs/studies/st15_%s_%s_6D1/sixdeskenv'%(xangle,binte)

        database='st15_6D_err.db'
        path='/afs/cern.ch/user/d/dbanfi/SixTrack_Err/sd/sixjobs/studies/st15_%s_%s_6D_err/sixdeskenv'%(xangle,binte)
        sd=SixDeskDB(database)
        print path
        st=SixDeskDir(path)
        sd.add_results(st.iter_results())









