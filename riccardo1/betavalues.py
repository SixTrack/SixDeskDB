
class BetaValue(object):
  def __init__(self,data):
    for nnn,name,typ,desc in Fort10.fields:
      try:
        val=data[name]
        setattr(self,name,val)
      except KeyError:
        try:
          val=data[nnn]
          setattr(self,name,val)
        except KeyError:
          pass
  __getitem__=__getattribute__
  fields=[
   ('betx','float',''),
   ('alfx','float',''),
   ('bety','float',''),
   ('alfy','float',''),
   ('qx','float',''),
   ('qy','float',''),
   ('dqx','float',''),
   ('dqy','float',''),
   ('x','float',''),
   ('px','float',''),
   ('y','float',''),
   ('py','float',''),
   ('z','float',''),
   ('pz','float','')]



