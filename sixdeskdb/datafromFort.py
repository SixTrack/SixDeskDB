from queries import queries, dataQueried
import sys
import numpy as np
import os

ntlint =4
class Fort:
    def __init__(self, fort, sd,  seed=None, angle=None,):
        self.name = "fort.{unit}".format(unit=fort)
        self.fort = fort
        self.sd = sd
        self.angles = self.sd.get_angles() if angle == None else [self.sd.get_angles()[angle]]
        self.seeds = self.sd.get_seeds() if seed == None else [seed]
        self.fields =list(zip(*dataQueried[str(fort)])[0])
        self.data = self.dispatch[str(fort)](self)

    def __getitem__(self, key):
        if type(key)==tuple:
           mask = ((self.data['seed']==key[0]) & 
                      (self.data['angle']==self.sd.get_angles()[key[1]]))
           return self.data[mask]
        elif type(key)==int:
           return self.data[self.data['seed']==key]
        return self.data[key]

    def __repr__(self):
        return self.data.__repr__()

    def line(self, key):
        return self.data[key]

    def column(self, key):
        return self.data[self.fields[key]]

    def retrieve(self, extraFields=[]):
        rectype = [('seed', 'int'),('angle', 'float')]+dataQueried[str(self.fort)]
        rectype += [i for i in extraFields if i not in rectype]
        names , ind= np.unique(zip(*rectype)[0], return_index=True)
        names = ','.join(names[np.argsort(ind)])
        sql = queries['else'] if self.fort !=10 else queries['10']
        seedsSeq  = ('={0}' if len(self.seeds) ==1 else ' IN ({0})').format(','.join(map(str, self.seeds)))
        anglesSeq = ('={0}' if len(self.angles)==1 else ' IN ({0})').format(','.join(map(str, self.angles)))
        sql = sql.format( names = names, seedsSeq=seedsSeq, anglesSeq=anglesSeq)
        # sql = sql.format( names = names, 
        #                   seedsSeq=','.join(map(str, self.sd.get_seeds())), 
        #                   anglesSeq=','.join(map(str, self.sd.get_angles())))
        return np.array(self.sd.execute(sql), dtype=rectype)

    def retrieveAl(self, indices=None):
        f = self.getUnique()
        for i in xrange(len(f['al'])):
             f['al'][i] = np.frombuffer(f['al'][i])
        if indices:
            arr = f['al']['arr'][:, indices][:,::-1] 
            dt = f.dtype.descr
            ind = f.dtype.names.index('al')
            dt[ind]=('al', [('arr', '<f8', (len(indices),))])
            f=f.astype(dt)
            f['al']['arr'] = arr
        return f

    def write(self):
        directory="job_tracking_forts/%s/simul/62.31_60.32/6-14/e5/.%s/"
        nAngle=0
        for seed in self.sd.get_seeds():
            for angle in self.sd.get_angles():
                nAngle+=1 
                path = directory%(seed,nAngle)
                f = path+self.name
                if not os.path.exists(path): os.makedirs(path)
                # fileMask = list((self.data['seed']==seed) & (self.data['angle']==angle))
                #np.savetxt(f, self.data[fileMask][self.fields], fmt="%14s")
                np.savetxt(f, self.data[seed, nAngle][self.fields], fmt="%14s")

    def filterLostAndApertures(self, extraFields=[]):
        extraF = extraFields+[('alost1', 'float'), ('alost2', 'float'), ('rad', 'float')]
        data = self.retrieve(extraF)
        data = data[((data['alost1']) < 1e-38) | (data['rad']<data['alost2'])]
        return data

    def filterDistancesLostApertures(self):
        extraF = [('distp', 'float'), ('dist', 'float')]
        data = self.filterLostAndApertures(extraF)
        if len(data):
             data = data[(data['distp'] < 2.0) & (data['dist'] < 0.1)]
        return np.copy(data[['seed', 'angle'] + self.fields])

    def getUnique(self):
        data = self.retrieve()
        dummy, ind = np.unique(data[['seed','angle']], return_index=True)
        return data[ind]

    def f11(self):
        data = self.retrieveAl([x*ntlint for x in range(3,8)])
        res = []
        for row in xrange(len(data)):
            for field in self.fields:
                if field == 'al':
                    for column in data[row][field]['arr']:
                        res.append(tuple(list(data[['seed','angle']][row])+[column, 1e-1])) 
                else:
                    res.append(tuple(list(data[['seed','angle', field]][row])+ [1e-1]))
        self.fields = ["col1", "col2"]
        return np.array(res, dtype=([('seed','int'), ('angle','float'),('col1','float'),('col2','float')])) 

    def f14(self):
        data = self.getUnique()
        #data = data[data['f14']==1]
        res = []
        self.fields = self.fields[:-3] + ["c2"]
        for i in xrange(len(data)):
            res.append(tuple(list(data[i])[:3]+[data[i][3]/2.0]))
            res.append(tuple(list(data[i])[:3]+[data['turn_max'][0]*2]))
        return np.array(res, dtype=([('seed','int'), ('angle','float'),('achaos','float'),('c2','float')])) 

    def f15(self): 
        data = self.retrieve()
        data['sturns1'][data['sturns1']==0] = 1
        data['sturns2'][data['sturns2']==0] = 1
        res=[]
        self.fields = self.fields[:-2]+["sturns"]
        for i in xrange(len(data)):
            res.append(tuple(list(data[i])[:-1]))
            res.append(tuple(list(data[i])[:-2]+[data[i][-1]]))
        return np.array(res, dtype=([('seed','int'), ('angle','float'),('rad','float'),('sturns','float')])) 
    
    def f16(self):
        data = self.filterDistancesLostApertures()
        data['qx'] = data['qx'] + data['qy']
        data = data[list(data.dtype.names[:-1])]
        data.dtype.names=data.dtype.names[:-1]+("sum",)
        self.fields= self.fields[:-2]+["sum"]
        return data
    
    def f17(self):
        data = self.filterDistancesLostApertures()
        data['qx'] = data['qx'] - data['qy']
        data = data[list(data.dtype.names[:-1])]
        data.dtype.names=data.dtype.names[:-1]+("sub",)
        self.fields= self.fields[:-2]+["sub"]
        return data

    def f26(self):
        data = self.getUnique()
        res = []
        rectype = [('seed','int'), ('angle','float'),('c1','float'),('c2','float')]
        self.fields = list(zip(*rectype))
        for i in xrange(len(data)):
            res.append(tuple(list(data[i])[:-2]+[1e-1]))
            res.append(tuple(list(data[i])[:-3]+(
                [data[i][-2], 1e-1] if data[i][-2] > 1e-38 else [data[i][-1], 1e-1])))
        return np.array(res, dtype = (rectype))

    def f27(self):
        return self.retrieveAl()
    
    def f28(self):
        self.fields = ['angle'] + self.fields
        return self.retrieveAl()

    def f40(self):
        self.fields = ['angle'] + self.fields
        return self.retrieveAl([x*ntlint for x in range(3,8)])

    dispatch ={
     '10': retrieve,
     '11': f11,
     '12': retrieve,
     '13': retrieve,
     '14': f14,
     '15': f15,
     '16': f16,
     '17': f17,
     '18': filterLostAndApertures,
     '19': filterLostAndApertures,
     '20': filterDistancesLostApertures,
     '21': filterDistancesLostApertures,
     '22': filterLostAndApertures,
     '23': filterLostAndApertures,
     '24': filterLostAndApertures,
     '25': filterDistancesLostApertures,
     '26': f26,
     '27': f27,
     '28': f28,
     '40': f40
    }
