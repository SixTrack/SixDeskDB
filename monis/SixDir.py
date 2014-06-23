import sqlite3,time
import psutil, os, re, gzip
from sys import platform as _platform
import sys
import sixdeskdir
import sixdeskdb

def load_dict(cur,table,idcol,idnum):
    sql='SELECT key,value from %s WHERE %s=%d'%(table,idcol,idnum)
    cur.execute(sql)
    a = cur.fetchall()
    dict = {}
    for row in a:
        dict[str(row[0])] = str(row[1]) 
    return dict 

class SixDir(object):
    def __init__(self,studyName):
        self.studyName = studyName
        db = studyName+".db"
        if not os.path.isfile(db):
            print "%s does'nt exist "%(db)
            return
        try:
            conn = sqlite3.connect(db,isolation_level="IMMEDIATE")
        except sqlite3.Error:
            print 'error'
            return
        print "Opened database successfully"
        self.conn = conn
        self.load_env_var()

    def execute(self,sql):
        cur= self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
        return list(cur)

    def load_env_var(self):
        conn = self.conn
        cur = conn.cursor()
        sql = """SELECT env_id from env where key='LHCDescrip' and value='%s'"""
        cur.execute(sql%self.studyName)
        temp = cur.fetchone()[0]
        if temp is not None:
            id = int(temp)
        else:   
            'studyname not found'
            return
        self.env_var = load_dict(cur,"env","env_id",id)
        self.id = id

    def load_extra(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        id = self.id
        cur.execute("begin IMMEDIATE transaction")
        sql = """SELECT path,content from files where env_id = ?"""
        cur.execute(sql,[id])
        files = cur.fetchall()
        #print len(files)
        for file in files:
            path = os.path.dirname(str(file[0]))
            if not os.path.exists(path):
                os.makedirs(path)
            if not 'gz' in str(file[0]):
                f = open(str(file[0]),'w')
            else:
                f = gzip.open(str(file[0]),'w')
            f.write(str(file[1]))
            f.close()

    def load_mad6t_run(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        id = self.id
        sql = """SELECT * from mad6t_run where env_id = ?"""
        cur.execute(sql,[id])
        files = cur.fetchall()
        for file in files:
            path = os.path.join(env_var['sixtrack_input'],str(file[1]))
            if not os.path.exists(path):
                    os.makedirs(path)
            mad_in,mad_out,mad_lsf = [str(file[i]) for i in range(3,6)]
            f = open(path+'/'+env_var['LHCDescrip']+'.'+str(file[2]),'w')
            f.write(mad_in)
            f = open(path+'/'+env_var['LHCDescrip']+'.out.'+str(file[2]),'w')
            f.write(mad_out)
            f = open(path+'/mad6t_'+str(file[2])+'.lsf','w')
            f.write(mad_lsf)
        f.close()

    def load_mad6t_run2(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        id = self.id
        sql = """SELECT * from mad6t_run2 where env_id = ?"""
        cur.execute(sql,[id])
        fort3 = cur.fetchone()
        aux,mad,m1,m2 = [str(fort3[i]) for i in range(1,5)]
        path = env_var['sixtrack_input']
        f = open(path+'/fort.3.aux','w')
        f.write(aux)
        f = open(path+'/fort.3.mad','w')
        f.write(mad)
        f = open(path+'/fort.3.mother1','w')
        f.write(m1)
        f = open(path+'/fort.3.mother2','w')
        f.write(m2)
        f.close()

    def load_mad6t_results(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        id = self.id
        cur.execute("begin IMMEDIATE transaction")
        sql = """SELECT * from mad6t_results where env_id = ?"""
        cur.execute(sql,[id])
        forts = cur.fetchall()
        path = env_var['sixtrack_input']
        for fort in forts:
            seed,f2,f8,f16 = [str(fort[i]) for i in range(1,5)]
            f = gzip.open(path+'/fort.2_'+seed+'.gz','w')
            f.write(f2)
            f = gzip.open(path+'/fort.8_'+seed+'.gz','w')
            f.write(f8)
            f = gzip.open(path+'/fort.16_'+seed+'.gz','w')
            f.write(f16)
        f.close()

    def load_six_beta(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        id = self.id
        path = env_var['sixdesktrack']
        cur.execute("begin IMMEDIATE transaction")
        sql = """SELECT * from six_beta where env_id = ?"""
        cur.execute(sql,[id])
        beta = cur.fetchall()
        for row in beta:
            sql = """SELECT simul from six_input where env_id=? and seed=? and 
                    tunex=? and tuney=?"""
            cur.execute(sql,row[0:4])
            simul = cur.fetchone()[0]
            path1 = os.path.join(
                path,str(row[1]),simul,str(row[2])+'_'+str(row[3])
                )
            if not os.path.exists(path1):
                os.makedirs(path1)
            stri = ' '.join([str(row[i]) for i in range(5,19)])
            f = open(path1+'/betavalues','w')
            f.write(stri)
            stri = str(row[11])+' '+str(row[12])
            f = open(path1+'/mychrom','w')
            f.write(stri)
            stri = str(row[20])+'\n'+str(row[21])+' '+str(row[22])+'\n'
            stri += str(row[23])+' '+str(row[24])
            f = open(path1+'/sixdesktunes','w')
            f.write(stri)
        f.close()

    def load_six_input_results(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        id = self.id
        path = env_var['sixdesktrack']
        cur.execute("begin IMMEDIATE transaction")
        sql = """SELECT * from six_input where env_id=?"""
        cur.execute(sql,[id])
        six = cur.fetchall()
        for row in six:
            path1 = os.path.join(
                path,str(row[2]),str(row[3]),str(row[4])+'_'+str(row[5]),
                str(int(float(row[6])))+'_'+str(int(float(row[7]))),
                str(row[8]),str(int(float(row[9])))
                )
            if not os.path.exists(path1):
                os.makedirs(path1)
            f = gzip.open(path1+'/fort.3.gz','w')
            f.write(str(row[10]))
            sql = """SELECT * from six_results where six_input_id=?"""
            cur.execute(sql,[row[0]])
            fort = cur.fetchall()
            stri = ""
            for col in fort:
                str1 = ""
                for i in xrange(60):
                    str1 += str(col[i+2]) + ' '
                stri += str1 + '\n'
            f = gzip.open(path1+'/fort.10.gz','w')
            f.write(stri)
            f.close()

    def get_missing_fort10(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        # newid = self.newid
        sql = """select seed,tunex,tuney,amp1,amp2,turns,angle from six_input
        where not exists(select 1 from six_results where id=six_input_id)"""
        a = self.execute(sql)
        if a:
            for rows in a:
                print 'fort.10 missing at','/'.join([str(i) for i in rows])
                print 
            return 1
        else:
            return 0

    def get_incomplete_fort10(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        # newid = self.newid
        sql = """select seed,tunex,tuney,amp1,amp2,turns,angle from six_input
        where not exists(select 1 from six_results where id=six_input_id and 
        row_num=30)"""
        a = self.execute(sql)
        if a:
            for rows in a:
                print 'fort.10 incomplete at','/'.join([str(i) for i in rows])
                print 
            return 1
        else:
            return 0

    def join10(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        id = self.id
        ista = int(env_var['ista'])
        iend = int(env_var['iend'])
        tuney = float(env_var['tuney'])
        tunex = float(env_var['tunex'])
        tune = str(tunex)+'_'+str(tuney)
        print env_var['short'],env_var['long']
        if env_var['short'] == 1:
            amp1 = int(env_var['ns1s'])
            amp2 = int(env_var['ns2s'])
            ampd = int(env_var['nss'])
        else:
            amp1 = int(env_var['ns1l'])
            amp2 = int(env_var['ns2l'])
            ampd = int(env_var['nsincl'])
        sql = """SELECT distinct turns,angle from six_input"""
        cur.execute(sql)
        val = cur.fetchall()
        for seed in range(ista,iend+1):
            workdir = os.path.join(
                env_var['sixdesktrack'],str(seed),'simul',tune
                )
            join = os.path.join(workdir,str(amp1)+'-'+str(amp2))
            #print join
            for amp in range(amp1,amp2,ampd):
                sql = """SELECT * from six_input,six_results 
                where six_input_id=id and seed=? and amp1=? and amp2=? 
                and env_id=?"""
                cur.execute(sql,[seed,amp,amp+2,id])
                data = cur.fetchall()
                while data:
                    path = os.path.join(join,str(data[0][8]),str(data[0][9]))
                    if not os.path.exists(path):
                        os.makedirs(path)
                    if amp == amp1:
                        f = gzip.open(os.path.join(path,'fort.10.gz'),'w')
                        #print os.path.join(path,'fort.10.gz')
                    else:
                        f = gzip.open(os.path.join(path,'fort.10.gz'),'a')
                    for j in xrange(30):
                        str1 = '\t'.join(
                            [str(data[0][i]) for i in range(13,73)]
                            )
                        str1 += '\n'
                        f.write(str1)
                        del data[0]
                    f.close()


if __name__ == '__main__':
    a = SixDir('jobslhc31b_inj55_itv19')
    # a.load_extra()
    # a.load_mad6t_run()
    # a.load_mad6t_run2()
    # a.load_mad6t_results()
    # a.load_six_beta()
    # a.load_six_input_results()
    # a.join10()