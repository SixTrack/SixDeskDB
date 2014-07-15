import sqlite3,time
import os, re, gzip
from sys import platform as _platform
import sys
import sixdeskdir
import sixdeskdb
import cStringIO

def load_dict(cur,table):
    sql='SELECT keyname,value from %s'%(table)
    cur.execute(sql)
    a = cur.fetchall()
    dict = {}
    for row in a:
        dict[str(row[0])] = str(row[1]) 
    return dict 

def decompressBuf(buf):
    zbuf = StringIO.StringIO(buf)
    f = gzip.GzipFile(fileobj=zbuf)
    return f.read()

class SixDir(object):
    def __init__(self,studyName,basedir='.'):
        self.studyName = studyName
        self.basedir = basedir
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
        sql = """SELECT count(*) from env where keyname='LHCDescrip' 
        and value='%s'"""
        cur.execute(sql%self.studyName)
        temp = list(cur)[0][0]
        if temp == 0:
            'studyname not found'
            return
        self.env_var = load_dict(cur,"env")

    def info(self):     
        var = ['LHCDescrip', 'platform', 'madlsfq', 'lsfq', 'runtype', 'e0',
        'gamma', 'beam', 'dpini', 'istamad', 'iendmad', 'ns1l', 'ns2l', 
        'nsincl','sixdeskpairs', 'turnsl', 'turnsle', 'writebinl', 'kstep', 
        'kendl', 'kmaxl','trackdir', 'sixtrack_input']
        env_var = self.env_var
        for keys in var:
          print '%s=%s'%(keys,env_var[keys])

    def load_extra(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        basedir = self.basedir
        cur.execute("begin IMMEDIATE transaction")
        sql = """SELECT path,content from files"""
        cur.execute(sql)
        files = cur.fetchall()
        #print len(files)
        for file in files:
            path = os.path.join(basedir,str(file[0])[1:])
            if not os.path.exists(path):
                os.makedirs(path)
            f = open(path,'w')
            if '.gz' in path:
                f.write(str(file[1]))
            else:
                f.write(decompressBuf(str(file[1])))
        f.close()

    def load_mad6t_run(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        sql = """SELECT * from mad6t_run"""
        cur.execute(sql)
        files = cur.fetchall()
        for file in files:
            path = os.path.join(env_var['sixtrack_input'],str(file[0]))
            if not os.path.exists(path):
                    os.makedirs(path)
            mad_in,mad_out,mad_lsf,mad_log = [str(file[i]) for i in range(2,6)]
            f = open(
                path+'/'+env_var['LHCDescrip']+'.'+str(file[1]),'w')
            f.write(decompressBuf(mad_in))
            f = open(
                path+'/'+env_var['LHCDescrip']+'.out.'+str(file[1]),'w')
            f.write(decompressBuf(mad_out))
            f = open(
                path+'/mad6t_'+str(file[1])+'.lsf','w')
            f.write(decompressBuf(mad_lsf))
            f = open(
                path+'/'+env_var['LHCDescrip']+'_mad_'+str(file[1]+'.log'),'w')
            f.write(decompressBuf(mad_in))
            f = open(path+'/'+env_var)
        f.close()

    # def load_mad6t_run2(self):
    #     conn = self.conn
    #     cur = conn.cursor()
    #     env_var = self.env_var
    #     id = self.id
    #     sql = """SELECT * from mad6t_run2 where env_id = ?"""
    #     cur.execute(sql,[id])
    #     fort3 = cur.fetchone()
    #     aux,mad,m1,m2 = [str(fort3[i]) for i in range(1,5)]
    #     path = env_var['sixtrack_input']
    #     f = open(path+'/fort.3.aux','w')
    #     f.write(aux)
    #     f = open(path+'/fort.3.mad','w')
    #     f.write(mad)
    #     f = open(path+'/fort.3.mother1','w')
    #     f.write(m1)
    #     f = open(path+'/fort.3.mother2','w')
    #     f.write(m2)
    #     f.close()

    def load_mad6t_results(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        cur.execute("begin IMMEDIATE transaction")
        sql = """SELECT * from mad6t_results"""
        cur.execute(sql)
        forts = cur.fetchall()
        path = env_var['sixtrack_input']
        for fort in forts:
            seed,f2,f8,f16 = [str(fort[i]) for i in range(0,4)]
            f = open(path+'/fort.2_'+seed+'.gz','w')
            f.write(f2)
            f = open(path+'/fort.8_'+seed+'.gz','w')
            f.write(f8)
            f = open(path+'/fort.16_'+seed+'.gz','w')
            f.write(f16)
        f.close()

    def load_six_beta(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        path = env_var['sixdesktrack']
        cur.execute("begin IMMEDIATE transaction")
        sql = """SELECT * from six_beta"""
        cur.execute(sql)
        beta = cur.fetchall()
        for row in beta:
            sql = """SELECT simul from six_input where seed=? and 
                    tunex=? and tuney=?"""
            cur.execute(sql,row[0:3])
            simul = cur.fetchone()[0]
            path1 = os.path.join(
                path,str(row[0]),simul,str(row[1])+'_'+str(row[2])
                )
            if not os.path.exists(path1):
                os.makedirs(path1)
            stri = ' '.join([str(row[i]) for i in range(3,17)])
            f = open(path1+'/betavalues','w')
            f.write(stri)
            stri = str(row[9])+' '+str(row[10])
            f = open(path1+'/mychrom','w')
            f.write(stri)
            stri = str(row[19])+'\n'+str(row[20])+' '+str(row[21])+'\n'
            stri += str(row[22])+' '+str(row[23])
            f = open(path1+'/sixdesktunes','w')
            f.write(stri)
        f.close()

    def load_six_input_results(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        path = env_var['sixdesktrack']
        cur.execute("begin IMMEDIATE transaction")
        sql = """SELECT * from six_input"""
        cur.execute(sql)
        six = cur.fetchall()
        for row in six:
            path1 = os.path.join(
                path,str(row[1]),str(row[2]),str(row[3])+'_'+str(row[4]),
                str(int(float(row[5])))+'_'+str(int(float(row[6]))),
                str(row[7]),str(int(float(row[8])))
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
                where six_input_id=id and seed=? and amp1=? and amp2=?"""
                cur.execute(sql,[seed,amp,amp+2])
                data = cur.fetchall()
                while data:
                    path = os.path.join(join,str(data[0][7]),str(data[0][8]))
                    if not os.path.exists(path):
                        os.makedirs(path)
                    if amp == amp1:
                        f = gzip.open(os.path.join(path,'fort.10.gz'),'w')
                        #print os.path.join(path,'fort.10.gz')
                    else:
                        f = gzip.open(os.path.join(path,'fort.10.gz'),'a')
                    for j in xrange(30):
                        str1 = '\t'.join(
                            [str(data[0][i]) for i in range(12,72)]
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
