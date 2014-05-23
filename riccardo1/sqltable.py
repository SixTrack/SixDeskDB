import sqlite3
import numpy as np


class SQLTable(object):
  @staticmethod
  def cols_from_fields(fields):
    types=[]
    for i,ttt,desc in fields:
      if ttt == 'float': types.append('%s REAL'%i)
      elif ttt == 'str': types.append('%s TEXT'%i)
      elif ttt == 'int': types.append('%s INTEGER'%i)
    return types
  @staticmethod
  def cols_from_dtype(dtype):
    names=dtype.names
    types=[]
    for i in names:
      ttt=dtype.fields[i][0]
      if ttt == np.float_: types.append('REAL')
      elif ttt == np.string_: types.append('TEXT')
      elif ttt == np.int_: types.append('INTEGER')
    return [' '.join(c) for c in zip(names,types)]
  @staticmethod
  def query_from_dict(query):
    return ' AND '.join(['%s=%s'%(k,repr(v)) for k,v in query.items()])
  def __init__(self,db,name,cols,keys=None):
    self.db=db
    self.name=name
    self.cols=cols
    self.keys=keys
    self.create()
  def create(self):
    db=self.db;table=self.name
    cols=self.cols
    keys=self.keys
    sql="CREATE TABLE IF NOT EXISTS %s(%s);"
    sql_cols=','.join(cols)
    sql_cmd=sql%(table,sql_cols)
    cur=db.cursor()
    cur.execute(sql_cmd)
    if keys is not None:
      sql="CREATE UNIQUE INDEX IF NOT EXISTS keys_%s ON %s(%s)"
      sql_cmd=sql%('_'.join(keys),table,','.join(keys))
      cur.execute(sql_cmd)
    db.commit()
    self.cols=cols
    self.keys=keys
  def insert(self,data,replace=True):
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
  def delete(self,where):
    db=self.db;table=self.name
    sql="DELETE FROM %s WHERE %s"
    sql_cmd=sql%(table,where)
    cur=db.cursor()
    cur.execute(sql_cmd)
    db.commit()
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
        if type(i) == float: types.append('float')
        elif type(i) == unicode: types.append('str')
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

