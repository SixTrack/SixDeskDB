import sqlite3
import time
import psutil
import os
import re
import gzip
from sys import platform as _platform
import sys
import sixdeskdir
from sixdeskdb import Fort10
import lsfqueue
import numpy as np
import matplotlib.pyplot as pl
import scipy.signal


def col_count(cur, table):
    sql = 'pragma table_info(%s)' % (table)
    cur.execute(sql)
    return len(cur.fetchall())


def dict_to_list(dict):
    lst = []
    for i in sorted(dict.keys()):
        for j in dict[i]:
            if isinstance(j, list):
                lst.append(j)
            else:
                lst.append(dict[i])
                break
    return lst


def store_dict(cur, colName, table, data):
    cur.execute("select max(%s) from %s" % (colName, table))
    temp = cur.fetchone()[0]
    if temp is not None:
        newid = int(temp) + 1
    else:
        newid = 1
    lst = []
    for head in data:
        lst.append([newid, head, data[head]])
    sql = "INSERT into %s values(?,?,?)" % (table)
    cur.executemany(sql, lst)
    return newid


def load_dict(cur, table, idcol, idnum):
    sql = 'SELECT key,value from %s WHERE %s=%d' % (table, idcol, idnum)
    cur.execute(sql)
    a = cur.fetchall()
    dict = {}
    for row in a:
        dict[str(row[0])] = str(row[1])
    return dict

def guess_range(l):
    l=sorted(set(l))
    start=l[0]
    if len(l)>1:
        step=np.diff(l).min()
    else:
        step=None
    stop=l[-1]
    return start,stop,step


class SixDB(object):

    def __init__(self, studyDir):
        self.studyDir = studyDir
        self.env_var = sixdeskdir.parse_env(studyDir)
        db = self.env_var['LHCDescrip'] + ".db"
        self.conn = sqlite3.connect(db, isolation_level="IMMEDIATE")
        print "Opened database successfully"
        self.create_empty_tables()

    def execute(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
        return list(cur)

    def create_empty_tables(self):
        env_var = self.env_var
        conn = self.conn
        cur = conn.cursor()
        sql = """CREATE table if not exists env(env_id int, key string,
            value string)"""
        cur.execute(sql)

        sql = """create table if not exists mad6t_run(env_id int, run_id string,
        seed int , mad_in blob, mad_out blob, mad_lsf blob)"""
        cur.execute(sql)

        sql = """create table if not exists mad6t_run2(env_id int,
            fort3aux blob, fort3mad blob, fort3mother1 blob,
            fort3mother2 blob)"""
        cur.execute(sql)

        sql = """create table if not exists mad6t_results(env_id int, seed int,
            fort2 blob, fort8 blob, fort16 blob)"""
        cur.execute(sql)

        sql = """create table if not exists six_beta(env_id int, seed int,
            tunex double, tuney double, beta11 double, beta12 double,
            beta22 double, beta21 double, qx double,qy double,dqx double,
            dqy double, x double, xp double,y double, yp double, sigma double,
            delta double, emitn double, gamma double, deltap double,qx1 double,
             qy1 double, qx2 double, qy2 double)"""
        cur.execute(sql)

        sql = """create table if not exists six_input(id integer primary key,
            env_id int, seed int, simul string, tunex double, tuney double,
            amp1 int, amp2 int, turns string, angle int, fort3 blob)"""
        cur.execute(sql)

        sql = """create table if not exists six_results(six_input_id int,
            row_num int, """ + \
            ','.join('%s %s' % (i[0], i[1]) for i in Fort10.fields) + \
            """, primary key(six_input_id,row_num))"""
        cur.execute(sql)

        sql = """create table if not exists files(env_id int, path text,
            content blob)"""
        cur.execute(sql)
        cur.execute("PRAGMA synchronous = OFF")
        cur.execute("PRAGMA journal_mode = MEMORY")
        cur.execute("PRAGMA auto_vacuum = FULL")
        cur.execute("PRAGMA temp_store = MEMORY")
        cur.execute("PRAGMA count_changes = OFF")
        cur.execute("PRAGMA mmap_size=2335345345")

    def st_control(self):
        extra_files = []
        cur = self.conn.cursor()
        env_var = self.env_var
        workdir = env_var['sixdeskhome']
        # workdir = os.path.join(
        #    env_var['scratchdir'],env_var['workspace'],'sixjobs')
        # print workdir
        # env_var['sixdesktrack'] += '/'+env_var['LHCDescrip']
        # print env_var['sixdesktrack']
        # print env_var['sixtrack_input']
        temp = self.execute("""SELECT env_id from env where key='LHCDescrip' 
                            and value = \'%s\'"""%(env_var['LHCDescrip']))
        if temp:
            newid = int(temp[0]) + 1
        else:
            newid = 1
        self.newid = newid
        print self.newid
        beam = env_var['beam']
        if beam == "" or beam == "b1" or beam == "B1":
            appendbeam = ""
        elif beam == "b2" or beam == "B2":
            appendbeam = "_b2"
        else:
            print 'Unrecognised beam option must be null,b1,B1,b2 or B2'
            return
        files = 'fort.3.mother2_' + str(env_var['runtype']) + appendbeam
        path = os.path.join(workdir, 'control_files', files)
        content = sqlite3.Binary(open(path, 'r').read())
        extra_files.append([newid, path, content])
        sql = """INSERT into files values(?,?,?)"""
        cur.executemany(sql,extra_files)
        self.conn.commit()

    def st_mask(self):
        extra_files = []
        cur = self.conn.cursor()
        env_var = self.env_var
        newid = self.newid
        workdir = env_var['sixdeskhome']
        files = str(env_var['LHCDescrip']) + '.mask'
        path = os.path.join(workdir, 'mask', files)
        content = sqlite3.Binary(open(path, 'r').read())
        extra_files.append([newid, path, content])
        sql = """INSERT into files values(?,?,?)"""
        cur.executemany(sql,extra_files)
        self.conn.commit()

    def st_env(self):
        extra_files = []
        cur = self.conn.cursor()
        env_var = self.env_var
        newid = self.newid
        workdir = env_var['sixdeskhome']
        flag = 0
        for dirName, subdirList, fileList in os.walk(workdir + '/studies'):
            for files in fileList:
                if ('sixdeskenv' in files or 'sysenv' in files) and flag == 0:
                    # env_var = sixdeskenvdir.parse_env(dirName)
                    self.newid = store_dict(cur, "env_id", "env", env_var)
                    self.conn.commit()
                    flag = 1
                path = os.path.join(dirName, files)
                content = sqlite3.Binary(open(path, 'r').read())
                extra_files.append([self.newid, path, content])
                extra_files.append([newid, path, content])
                sql = """INSERT into files values(?,?,?)"""
                cur.executemany(sql,extra_files)
                self.conn.commit()

    def st_mad6t_run(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        newid = self.newid
        rows = {}
        extra_files = []
        a = []
        workdir = env_var['sixtrack_input']
        a = self.execute("SELECT distinct run_id from mad6t_run")
        if a:
            a = [str(i) for i in a]
        # print newid
        col = col_count(cur, 'mad6t_run')
        for dirName, subdirList, fileList in os.walk(workdir):
            if 'mad.dorun' in dirName and not ('mad.dorun' in a):
                print 'found new mad run',dirName.split('/')[-1]
                for files in fileList:
                    if not (files.endswith('.mask') or 'out' in files
                            or files.endswith('log') or files.endswith('lsf')):
                        seed = files.split('.')[-1]
                        run_id = dirName.split('/')[-1]
                        mad_in = sqlite3.Binary(open(
                            os.path.join(dirName, files), 'r'
                        ).read()
                        )

                        out_file = files.replace('.', '.out.')
                        mad_out = sqlite3.Binary(open(
                            os.path.join(dirName, out_file), 'r'
                        ).read()
                        )
                        lsf_file = 'mad6t_' + seed + '.lsf'
                        mad_lsf = sqlite3.Binary(open(
                            os.path.join(dirName, lsf_file), 'r'
                        ).read()
                        )
                        rows[seed] = []
                        rows[seed].append(
                            [newid, run_id, seed, mad_in, mad_out, mad_lsf])
                    if files.endswith('.log'):
                        path = os.path.join(dirName, files)
                        content = sqlite3.Binary(open(path, 'r').read())
                        extra_files.append([newid, path, content])
            if rows:
                lst = dict_to_list(rows)
                sql = "INSERT into mad6t_run values("
                sql += ','.join("?" * col) + ")"
                cur.executemany(sql, lst)
                conn.commit()
                rows = {}
        if extra_files:
            sql = "INSERT into files values (?,?,?)"
            cur.execute("begin IMMEDIATE transaction")
            cur.executemany(sql, extra_files)
            conn.commit()

    def st_mad6t_run2(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        newid = self.newid
        workdir = env_var['sixtrack_input']
        fort3 = {}
        extra_files = []
        # print newid
        col = col_count(cur, 'mad6t_run2')
        for dirName, subdirList, fileList in os.walk(workdir):
            for files in fileList:
                if 'fort.3' in files and not files.endswith('.tmp'):
                    fort3[files.replace('fort.3.', '')] = sqlite3.Binary(open(
                        os.path.join(dirName, files), 'r'
                    ).read()
                    )
                if files.endswith('.tmp'):
                    path = os.path.join(dirName, files)
                    content = sqlite3.Binary(open(path, 'r').read())
                    extra_files.append([newid, path, content])
            if fort3 and len(fort3.keys()) == 4:
                sql = "INSERT into mad6t_run2 values(" + \
                    ','.join("?" * col) + ")"
                cur.execute(sql, [newid, fort3['aux'], fort3['mad'],
                                  fort3['mother1'], fort3['mother2']
                                  ]
                            )
                conn.commit()
                fort3 = {}
        if extra_files:
            sql = "INSERT into files values (?,?,?)"
            cur.execute("begin IMMEDIATE transaction")
            cur.executemany(sql, extra_files)

    def st_mad6t_results(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        newid = self.newid
        workdir = env_var['sixtrack_input']
        rows = {}
        col = col_count(cur, 'mad6t_results')
        for dirName, subdirList, fileList in os.walk(workdir):
            for files in fileList:
                if 'fort' in files and files.endswith('.gz'):
                    head = int(files.split('_')[1].replace('.gz', ''))
                    if head not in rows.keys():
                        rows[head] = [newid, head]
                    if '2' in files:
                        rows[head].insert(2, sqlite3.Binary(gzip.open(
                            os.path.join(dirName, files), 'r'
                        ).read()
                        )
                        )
                    elif '8' in files:
                        rows[head].insert(3, sqlite3.Binary(gzip.open(
                            os.path.join(dirName, files), 'r'
                        ).read()
                        )
                        )
                    else:
                        rows[head].extend([sqlite3.Binary(gzip.open(
                            os.path.join(dirName, files), 'r'
                        ).read()
                        )
                        ])
            if rows:
                lst = dict_to_list(rows)
                sql = "INSERT into mad6t_results values(" + ','.join("?" * col)
                sql += ")"
                cur.execute("begin IMMEDIATE transaction")
                cur.executemany(sql, lst)
                conn.commit()
                rows = {}

    def st_six_beta(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        newid = self.newid
        rows = {}
        extra_files = []
        col = col_count(cur, 'six_beta')
        beta = six = gen = []
        for dirName, subdirList, fileList in os.walk(env_var['sixdesktrack']):
            for files in fileList:
                if 'general_input' in files:
                    with open(os.path.join(dirName, files), 'r') as FileObj:
                        for lines in FileObj:
                            gen = lines.split()
                    path = os.path.join(dirName, files)
                    content = sqlite3.Binary(open(path, 'r').read())
                    extra_files.append([newid, path, content])
                if 'betavalues' in files or 'sixdesktunes' in files:
                    dirn = dirName.replace(env_var['sixdesktrack'] + '/', '')
                    dirn = dirn.split('/')
                    seed = int(dirn[0])
                    tunex, tuney = [i for i in dirn[2].split('_')]
                    if not (seed in rows.keys()):
                        rows[seed] = []
                    temp = [newid, seed, tunex, tuney]
                    if 'betavalues' in files:
                        f = open(os.path.join(dirName, files), 'r')
                        beta = [float(i) for i in f.read().split()]
                    if 'sixdesktunes' in files:
                        f = open(os.path.join(dirName, files), 'r')
                        six = [float(i) for i in f.read().split()]
                    f.close()
                if beta and temp and six:
                    rows[seed].append(temp + beta + gen + six)
                    beta = temp = six = []

        if rows:
            lst = dict_to_list(rows)
            sql = "INSERT into six_beta values(" + \
                ','.join("?" * col) + ")"
            cur.executemany(sql, lst)
            conn.commit()
            rows = {}
        if extra_files:
            sql = "INSERT into files values (?,?,?)"
            cur.execute("begin IMMEDIATE transaction")
            cur.executemany(sql, extra_files)
            conn.commit()

    def st_six_input(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        newid = self.newid
        extra_files = []
        rows = []
        #res = []
        cur.execute('SELECT id from six_input order by id DESC limit 1')
        six_id = cur.fetchone()
        if six_id is not None:
            six_id += 1
        else:
            six_id = 1
        col = col_count(cur, 'six_input')
        col1 = col_count(cur, 'six_results')
        for dirName, subdirList, fileList in os.walk(env_var['sixdesktrack']):
            for files in fileList:
                if 'fort.3' in files and not ('-' in dirName):
                    dirn = dirName.replace(env_var['sixdesktrack'] + '/', '')
                    # print dirn
                    dirn = re.split('/|_', dirn)
                    dirn = [six_id, newid] + dirn
                    dirn.extend([sqlite3.Binary(gzip.open(
                        os.path.join(dirName, files), 'r'
                    ).read()
                    )])
                    rows.append(dirn)
                    dirn = []
                    # files = files.replace('3', '10')
                    # if not os.path.exists(os.path.join(dirName, files)):
                    #     print "fort 10 not present at", 
                    #     print dirName.replace(env_var['sixdesktrack'], '')
                    # else:
                    #     with gzip.open(os.path.join(dirName, files), "r") \
                    #     as FileObj:
                    #         count = 1
                    #         for lines in FileObj:
                    #             res.append([six_id, count] + lines.split())
                    #             count += 1
                    six_id += 1
                if files.endswith('.log') or files.endswith('.lsf'):
                    path = os.path.join(dirName, files)
                    content = sqlite3.Binary(open(path, 'r').read())
                    extra_files.append([newid, path, content])
            if len(rows) > 3000:
                sql = "INSERT into six_input values ("
                sql += ','.join('?' * col) + ")"
                cur.execute("begin IMMEDIATE transaction")
                cur.executemany(sql, rows)
                conn.commit()
                rows = []
            # if len(res) > 90000:
            #     sql = "INSERT into six_results values ("
            #     sql += ','.join('?' * col1) + ")"
            #     cur.execute("begin IMMEDIATE transaction")
            #     cur.executemany(sql, res)
            #     conn.commit()
            #     res = []
        if rows:
            sql = "INSERT into six_input values (" + ','.join('?' * col) + ")"
            cur.executemany(sql, rows)
            conn.commit()
            rows = []
        # if res:
        #     sql = "INSERT into six_results values (" + \
        #         ','.join('?' * col1) + ")"
        #     cur.execute("begin IMMEDIATE transaction")
        #     cur.executemany(sql, res)
        #     conn.commit()
        #     res = []
        if extra_files:
            sql = "INSERT into files values (?,?,?)"
            cur.execute("begin IMMEDIATE transaction")
            cur.executemany(sql, extra_files)
            conn.commit()

    def st_six_results(self):
        conn = self.conn
        cur = conn.cursor()
        env_var = self.env_var
        newid = self.newid
        rows = []
        col = col_count(cur,'six_results')
        sql = """SELECT distinct id,seed,simul,tunex,tuney,amp1,amp2,turns,
                angle from six_input where env_id=%d"""
        inp = self.execute(sql%(newid))
        inp = [[str(i) for i in j] for j in inp]
        for dirName, subdirList, fileList in os.walk(env_var['sixdesktrack']):
            for files in fileList:
                if 'fort.10' in files and not '-' in dirName:
                    dirn = dirName.replace(env_var['sixdesktrack']+'/','')
                    dirn = re.split('/|_',dirn)
                    for i in inp:
                        if dirn == i[1:]:
                            six_id = i[0]
                            break
                    with gzip.open(os.path.join(dirName,files),"r") as FileObj:
                        count = 1
                        for lines in FileObj:
                            rows.append([six_id,count]+lines.split())
                            count += 1
                    if len(rows) > 90000:
                        sql = "INSERT into six_results values (" 
                        sql += ','.join('?'*col) + ")"
                        cur.execute("begin IMMEDIATE transaction")
                        cur.executemany(sql,rows)
                        conn.commit()
                        rows = []
        if rows:
            sql = "INSERT into six_results values (" + ','.join('?'*col) + ")"
            cur.execute("begin IMMEDIATE transaction")
            cur.executemany(sql,rows)
            conn.commit()
            rows = []

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

    def inspect_results(self):
        names='seed,tunex,tuney,amp1,amp2,turns,angle'
        for name in names.split(','):
            sql="SELECT DISTINCT %s FROM six_input"%name
            print sql
            data=[d[0] for d in self.execute(sql)]
            print name, guess_range(data)

    def iter_job_params(self):
        names="""b.value,a.seed,a.tunex,a.tuney,a.amp1,a.amp2,a.turns,a.angle,
                c.row_num"""
        sql="""SELECT DISTINCT %s FROM six_input as a,env as b,six_results as c
                where a.env_id=b.env_id and a.id=c.six_input_id and 
                b.key='LHCDescrip'"""%names
        return self.conn.cursor().execute(sql)
      
    def iter_job_params_comp(self):
        names='seed,tunex,tuney,amp1,amp2,turns,angle'
        sql='SELECT DISTINCT %s FROM six_input'%names
        return self.conn.cursor().execute(sql)
    
    def get_num_results(self):
        return self.execute('SELECT count(*) from six_results')[0][0]/30
      
    def inspect_jobparams(self):
        data=list(self.iter_job_params())
        names='study,seed,tunex,tuney,amp1,amp2,turns,angle,row'.split(',')
        data=dict(zip(names,zip(*data)))
    #    for name in names:
    #      print name, guess_range(data[name])
    #    print 'results found',len(data['seed'])
        turns=list(set(data['turns']))
        p={}
        p['ista'],p['iend'],p['istep']=guess_range(data['seed'])
        p['tunex'],p['tunex1'],p['deltax']=guess_range(data['tunex'])
        p['tuney'],p['tuney1'],p['deltay']=guess_range(data['tuney'])
        if p['deltax'] is None:
          p['deltax']=0.0001
        if p['deltay'] is None:
          p['deltay']=0.0001
        a1,a2,ast=guess_range(data['angle'])
        p['kmaxl']=90/ast-1
        p['kinil']=a1/ast
        p['kendl']=a2/ast
        p['kstep']=1
        p['ns1l'],p['ns2l'],p['nsincl']=guess_range(data['amp1']+data['amp2'])
        p['turnsl']=max(data['turns'])
        p['sixdeskpairs']=max(data['row'])+1
        p['LHCDescrip']=str(data['study'][0])
        return p
      
    def get_survival_turns(self,seed):
        cmd="""SELECT angle,amp1+(amp2-amp1)*row_num/30,
                CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
                FROM six_results,six_input WHERE seed=%s and id=six_input_id
                ORDER BY angle,sigx1"""
        # cmd="""SELECT angle,sqrt(sigx1*2+sigy1*2),
        #         CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
        #         FROM six_results,six_input WHERE seed=%s and id=six_input_id 
        #         ORDER BY angle,sigx1"""

        cur=self.conn.cursor().execute(cmd%seed)
        ftype=[('angle',float),('amp',float),('surv',float)]
        data=np.fromiter(cur,dtype=ftype)
        angles=len(set(data['angle']))
        return data.reshape(angles,-1)
      
    def plot_survival_2d(self,seed,smooth=None):
        data=self.get_survival_turns(seed)
        a,s,t=data['angle'],data['amp'],data['surv']
        self._plot_survival_2d(a,s,t,smooth=smooth)
        pl.title('Seed %d survived turns'%seed)
      
    def plot_survival_2d_avg(self,smooth=None):
        cmd=""" SELECT seed,angle,amp1+(amp2-amp1)*row_num/30,
                CASE WHEN sturns1 < sturns2 THEN sturns1 ELSE sturns2 END
                FROM six_results,six_input WHERE id=six_input_id
                ORDER BY angle,sigx1"""
        cur=self.conn.cursor().execute(cmd)
        ftype=[('seed',float),('angle',float),('amp',float),('surv',float)]
        data=np.fromiter(cur,dtype=ftype)
        angles=len(set(data['angle']))
        seeds=len(set(data['seed']))
        data=data.reshape(seeds,angles,-1)
        a,s,t=data['angle'],data['amp'],data['surv']
        a=a.mean(axis=0); s=s.mean(axis=0); t=t.mean(axis=0);
        self._plot_survival_2d(a,s,t,smooth=smooth)
        pl.title('Survived turns')
      
    def _plot_survival_2d(self,a,s,t,smooth=None):
        rad=np.pi*a/180
        x=s*np.cos(rad)
        y=s*np.sin(rad)
        #t=np.log10(t)
        if smooth is not None:
          if type(smooth)==int:
            smooth=np.ones((1.,smooth))/smooth
          elif type(smooth)==tuple:
            smooth=np.ones(smooth)/float(smooth[0]*smooth[1])
          t=scipy.signal.fftconvolve(smooth,t,mode='full')
        pl.pcolormesh(x,y,t,antialiased=True)
        pl.xlabel(r'$\sigma_x$')
        pl.ylabel(r'$\sigma_y$')
        pl.colorbar()
      
    def plot_survival_avg(self,seed):
        data=self.get_survival_turns(seed)
        a,s,t=data['angle'],data['amp'],data['surv']
        rad=np.pi*a/180
        drad=rad[0,0]
        slab='Seed %d'%seed
        #savg2=s**4*np.sin(2*rad)*drad
        pl.plot(s.mean(axis=0),t.min(axis=0) ,label='min')
        pl.plot(s.mean(axis=0),t.mean(axis=0),label='avg')
        pl.plot(s.mean(axis=0),t.max(axis=0) ,label='max')
        pl.ylim(0,pl.ylim()[1]*1.1)
        pl.xlabel(r'$\sigma$')
        pl.ylabel(r'survived turns')
        pl.legend(loc='lower left')
        return data
      
    def plot_survival_avg2(self,seed):
        def mk_tuple(a,s,t,nlim):
          region=[]
          for ia,ts in enumerate(t):
            cond=np.where(ts<=nlim)[0]
            #print ts[cond]
            if len(cond)>0:
              it=cond[0]
            else:
              it=-1
            region.append((ia,it))
            #print ts[it],s[ia,it]
          region=zip(*region)
          #print t[region]
          tmin=t[region].min()
          tavg=t[region].mean()
          tmax=t[region].max()
          savg=s[region].mean()
          rad=np.pi*a[region]/180
          drad=rad[0]
          savg2=np.sum(s[region]**4*np.sin(2*rad)*drad)**.25
          return nlim,tmin,tavg,tmax,savg,savg2
        data=self.get_survival_turns(seed)
        a,s,t=data['angle'],data['amp'],data['surv']
        table=np.array([mk_tuple(a,s,t,nlim) for nlim \
            in np.arange(0,1.1e6,1e4)]).T
        nlim,tmin,tavg,tmax,savg,savg2=table
        pl.plot(savg,tmin,label='min')
        pl.plot(savg,tavg,label='avg')
        pl.plot(savg,tmax,label='max')
        return table

if __name__ == '__main__':
    a = SixDB('./files/w7/sixjobs/')
    a.st_control()
    a.st_mask()
    a.st_env()
    a.st_mad6t_run()
    a.st_mad6t_run2()
    a.st_mad6t_results()
    a.st_six_beta()
    a.st_six_input()
    a.st_six_results()
    a.get_missing_fort10()
    # print a.get_num_results()
    # print a.inspect_jobparams()
    # a.plot_survival_2d(1)
    # a.plot_survival_2d(2)
    # a.plot_survival_avg(1)
    jobs = lsfqueue.parse_bjobs()
    if jobs:
        jobs_stats(jobs)

