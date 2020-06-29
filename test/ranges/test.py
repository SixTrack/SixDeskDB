from  sixdeskdb import SixDeskDB


db=SixDeskDB('job_hllhc10_test8.db')

def test():
 assert db.get_tunes()==[(62.31, 60.32)]

 db.env_var['deltax']=0.001
 db.env_var['deltay']=0.001
 db.env_var['tunex1']=62.31
 db.env_var['tuney1']=60.32
 assert db.get_tunes()==[(62.31, 60.32)]

 db.env_var['deltax']=0.000
 db.env_var['deltay']=0.000
 db.env_var['tunex1']=62.31
 db.env_var['tuney1']=60.32
 assert db.get_tunes()==[(62.31, 60.32)]

 db.env_var['deltax']=0.001
 db.env_var['deltay']=0.001
 db.env_var['tunex1']=62.312
 db.env_var['tuney1']=60.322
 print(db.get_tunes())
 assert db.get_tunes()==[(62.31, 60.32),(62.311, 60.321),(62.312, 60.322)]

 db.env_var['deltax']=-0.001
 db.env_var['deltay']=0.001
 db.env_var['tunex1']=62.309
 db.env_var['tuney1']=60.322
 print(db.get_tunes())
 assert db.get_tunes()==[(62.31, 60.32), (62.309, 60.321), (62.309, 60.322)]


test()

