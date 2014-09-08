import sqlite3
import numpy as np
import sixdeskdir
from tables import Env

class SQLTable(object):
  @staticmethod
  def cols_from_fields(fields):
    types=[]
    for i,ttt,desc in fields:
      if ttt == 'float' or ttt == 'double': types.append('%s REAL'%i)
      elif ttt == 'str' or ttt == "string": types.append('%s STRING'%i)
      elif ttt == 'int': types.append('%s INTEGER'%i)
      elif ttt == 'blob': types.append('%s BLOB'%i)
    return types
  @staticmethod
  def dtype_from_fields(fields):
    types=[]
    for i,ttt,desc in fields:
      if ttt == 'float' or ttt == 'double': types.append((i,float))
      elif ttt == 'str' or ttt == "string": types.append((i,'|S100') )
      elif ttt == 'int': types.append((i,int))
      elif ttt == 'blob': types.append((i,'S1000'))
    return types
  @staticmethod
  def cols_from_dtype(dtype):
    names=dtype.names
    types=[]
    for i in names:
      ttt=dtype.fields[i][0]
      #print i,ttt,types
      if ttt.kind=='f': types.append('REAL')
      elif ttt.kind == 'S': types.append('TEXT')
      elif ttt.kind in 'iu': types.append('INTEGER')
      else:
        raise ValueError,"%s not understood"% ttt
    return [' '.join(c) for c in zip(names,types)]
  @staticmethod
  def query_from_dict(query):
    return ' AND '.join(['%s=%s'%(k,repr(v)) for k,v in query.items()])
  def __init__(self,db,name,cols,keys=None,dbtype="sql"):
    self.db=db
    self.name=name
    self.cols=cols
    self.keys=keys
    self.dbtype = dbtype
    self.create()
  def create(self):
    db=self.db;table=self.name
    cols=self.cols
    keys=self.keys
    dbtype = self.dbtype
    sql="CREATE TABLE IF NOT EXISTS %s(%s);"
    sql_cols=','.join(cols)
    sql_cmd=sql%(table,sql_cols)
    cur=db.cursor()
    cur.execute(sql_cmd)
    if keys is not None:
      if dbtype == "sql":
        sql="CREATE UNIQUE INDEX IF NOT EXISTS keys_%s ON %s(%s)"
        sql_cmd=sql%('_'.join(keys),table,','.join(keys))
        cur.execute(sql_cmd)
      if dbtype == "mysql":
        sql = """SELECT COUNT(1) IndexIsThere FROM INFORMATION_SCHEMA.STATISTICS
            WHERE table_schema=DATABASE() AND table_name='%s' AND 
            index_name='keys_%s'"""
        sql_cmd = sql%(table,'_'.join(keys))
        # print sql_cmd
        cur.execute(sql_cmd)
        flag = cur.fetchone()[0]
        # print "flag =",flag
        if flag == 0:
          sql="CREATE UNIQUE INDEX keys_%s ON %s(%s)"
          sql_cmd=sql%('_'.join(keys),table,','.join(keys))
          cur.execute(sql_cmd)
    db.commit()
    self.cols=cols
    self.keys=keys
  def insert(self,data,replace=True):
    '''insert structured array into databse table'''
    db=self.db;table=self.name
    if replace:
      sql="REPLACE INTO %s(%s) VALUES (%s)"
    else:
      sql="INSERT INTO %s(%s) VALUES (%s)"
    cols=','.join(data.dtype.names)
    vals=','.join(('?',)*len(data.dtype.names))
    sql_cmd=sql%(table,cols,vals)
    cur=db.cursor()
    cur.executemany(sql_cmd, data)
    db.commit()
  def insertl(self,data,artype="?",replace=True):
    db=self.db
    table=self.name
    dbtype = self.dbtype
    if replace:
      sql="REPLACE INTO %s VALUES (%s)"
    else:
      sql="INSERT INTO %s VALUES (%s)"
    cur=db.cursor()
    if dbtype == "sql":
      cur.execute("begin IMMEDIATE transaction")
    if len(data) == 0:
      return
    if not isinstance(data[0], list):
      vals=','.join([artype] * len(data))
      sql_cmd=sql%(table,vals)
      cur.execute(sql_cmd, data)
    else:
      vals=','.join([artype] * len(data[0]))
      # print vals
      sql_cmd=sql%(table,vals)
      cur.executemany(sql_cmd, data)
      count = cur.rowcount
    # print sql_cmd
    db.commit()
  def delete(self,where):
    db=self.db;table=self.name
    sql="DELETE FROM %s WHERE %s"
    sql_cmd=sql%(table,where)
    cur=db.cursor()
    cur.execute(sql_cmd)
    db.commit()
  def selectl(self,cols='*',where=None,orderby=None):
    db=self.db;table=self.name
    if not 'distinct' in cols:
      cols=','.join(cols.split())
    sql="SELECT %s FROM %s"%(cols,table)
    if where is not None:
      sql+=' WHERE %s'%where
    if orderby is not None:
      sql+=' ORDER BY %s'%orderby
    cur=db.cursor()
    cur.execute(sql)
    data=list(cur)
    return data
  def select(self,cols='*',where=None,orderby=None):
    db=self.db;table=self.name
    cols=','.join(cols.split())
    sql="SELECT %s FROM %s"%(cols,table)
    if where is not None:
      sql+=' WHERE %s'%where
    if orderby is not None:
      sql+=' ORDER BY %s'%(','.join(orderby.split()))
    cur=db.cursor()
    cur.execute(sql)
    types = []
    data=list(cur)
    if len(data)>0:
      for i in data[0]:
        if type(i) == float : types.append('float')
        elif type(i) == str: types.append('|S100')
        elif type(i) == int: types.append('int')
      names=[i[0] for i in cur.description]
      data = np.array(data, dtype = zip(names,types))
    return data


if __name__=='__main__':
  db=sqlite3.connect(':memory:')
  rectype=[
      ('f1','float'),
      ('f2','float'),
      ('f3','int')]
  records=[(1,2,3),(4,5,6),(7,8,9),(1,5,2)]
  data=np.array(records,dtype=rectype)
  cols=SQLTable.cols_from_dtype(data.dtype)
  print cols
  db=sqlite3.connect(':memory:')
  tab=SQLTable(db,'tab1',cols,['f1','f2'])
  tab.insert(data)
  print tab.select()
  print tab.select('f3 f2',where='f3 = 2',orderby='f3 f2')
  tab.insert(data)
  tab.select()
  try:
    tab.insert(data,replace=False)
  except sqlite3.IntegrityError,msg:
    print 'ERROR:',msg
  data=np.random.rand(10000*3).view(rectype)
  tab.insert(data)
  print tab.select()

  #rectype=[(i,ttt) for i,ttt,desc in Env.fields]
  #a = sixdeskdir.parse_env('./files/w7/sixjobs/')
  #data=[(1,i,str(a[i])) for i in a.keys()]
  #cols=SQLTable.cols_from_fields(Env.fields)
  ## print cols
  #db=sqlite3.connect('test.db')
  #tab=SQLTable(db,'env',cols,['env_id','key'])
  #tab.insertl(data)

