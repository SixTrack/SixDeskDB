class Env(object):
  fields=[
  ('env_id','int','study id'),
  ('key','str','key'),
  ('value','str','value')]
  key=['env_id','key']

class Mad_Run(object):
  fields=[
  ('env_id','int','study id'),
  ('run_id','str','mad run name'),
  ('seed','int','seed value'),
  ('mad_in','blob','mad_in file'),
  ('mad_out','blob','mad_out file'),
  ('mad_lsf','blob','mad_lsf file'),
  ('mad_log','blob','mad_log file'),
  ('mtime','double','last modification time')]
  key=['env_id','run_id','seed']

class Mad_Run2(object):
  fields=[
  ('env_id','int','study id'),
  ('fort3aux','blob','fort3aux file'), 
  ('fort3mad','blob','fort3mad file'),
  ('fort3mother1','blob','fort3mother1 file'),
  ('fort3mother2','blob','fort3mother2 file')]
  key=['env_id']

class Mad_Res(object):
  fields=[
  ('env_id','int','study id'),
  ('seed','int','seed value'),
  ('fort2','blob','fort2 file'),
  ('fort8','blob','fort8 file'),
  ('fort16','blob','fort16 file')]
  key=['env_id','seed']

class Six_Be(object):
  fields=[
  ('env_id', 'int','study id'),
  ('seed', 'int', 'seed value'),
  ('tunex', 'double', 'tunex value'),
  ('tuney' ,'double', 'tuney value'),
  ('beta11', 'double', 'beta11 value'),
  ('beta12', 'double', 'beta12 value'),
  ('beta22', 'double', 'beta12 value'),
  ('beta21', 'double', 'beta12 value'),
  ('qx', 'double', 'qx value'),
  ('qy', 'double', 'qy value'),
  ('dqx', 'double', 'dqx value'),
  ('dqy', 'double', 'dqy value'), 
  ('x', 'double', 'x value'), 
  ('xp', 'double', 'xp value'),
  ('y', 'double', 'y value'), 
  ('yp', 'double', 'yp value'), 
  ('sigma', 'double', 'sigma value'),
  ('delta', 'double', 'delta value'), 
  ('emitn', 'double', 'emitn value'), 
  ('gamma', 'double', 'gamma value'),
  ('deltap', 'double', 'deltap value'),
  ('qx1', 'double', 'qx1 value'),
  ('qy1', 'double', 'qy1 value'), 
  ('qx2', 'double', 'qx2 value'), 
  ('qy2', 'double','qy2 value')]
  key=['env_id','seed','tunex','tuney']

class Six_In(object):
  fields=[
  ('id', 'int', 'unique id'), 
  ('env_id', 'int','study id'),
  ('seed', 'int', 'seed value'),
  ('simul', 'string', 'simul'), 
  ('tunex', 'double', 'tunex value'),
  ('tuney' ,'double', 'tuney value'),
  ('amp1', 'double', 'amp1 value'),
  ('amp2', 'double', 'amp2 value'), 
  ('turns', 'string', 'turns value'), 
  ('angle', 'double', 'angle value'), 
  ('fort3','blob','fort3 file')]
  key=['env_id','seed','simul','tunex','tuney','amp1','amp2','turns','angle']

class Six_Res(object):
  fields=[
  ('six_input_id','int','unique id for each fort10 file'),
  ('row_num','int','row number'),
  ('turn_max', 'int', 'Maximum turn number'),
  ('sflag', 'int', 'Stability Flag (0=stable 1=lost)'),
  ('qx', 'float', 'Horizontal Tune'),
  ('qy', 'float', 'Vertical Tune'),
  ('betx', 'float', 'Horizontal beta-function'),
  ('bety', 'float', 'Vertical beta-function'),
  ('sigx1', 'float', 'Horizontal amplitude 1st particle'),
  ('sigy1', 'float', 'Vertical amplitude 1st particle'),
  ('deltap', 'float', 'Relative momentum deviation Deltap'),
  ('dist', 'float', 'Final distance in phase space'),
  ('distp', 'float', 'Maximumslope of distance in phase space'),
  ('qx_det', 'float', 'Horizontal detuning'),
  ('qx_spread', 'float', 'Spread of horizontal detuning'),
  ('qy_det', 'float', 'Vertical detuning'),
  ('qy_spread', 'float', 'Spread of vertical detuning'),
  ('resxfact', 'float', 'Horizontal factor to nearest resonance'),
  ('resyfact', 'float', 'Vertical factor to nearest resonance'),
  ('resorder', 'int', 'Order of nearest resonance'),
  ('smearx', 'float', 'Horizontal smear'),
  ('smeary', 'float', 'Vertical smear'),
  ('smeart', 'float', 'Transverse smear'),
  ('sturns1', 'int', 'Survived turns 1st particle'),
  ('sturns2', 'int', 'Survived turns 2nd particle'),
  ('sseed', 'float', 'Starting seed for random generator'),
  ('qs', 'float', 'Synchrotron tune'),
  ('sigx2', 'float', 'Horizontal amplitude 2nd particle'),
  ('sigy2', 'float', 'Vertical amplitude 2nd particle'),
  ('sigxmin', 'float', 'Minimum horizontal amplitude'),
  ('sigxavg', 'float', 'Mean horizontal amplitude'),
  ('sigxmax', 'float', 'Maximum horizontal amplitude'),
  ('sigymin', 'float', 'Minimum vertical amplitude'),
  ('sigyavg', 'float', 'Mean vertical amplitude'),
  ('sigymax', 'float', 'Maximum vertical amplitude'),
  ('sigxminld', 'float', 'Minimum horizontal amplitude (linear decoupled)'),
  ('sigxavgld', 'float', 'Mean horizontal amplitude (linear decoupled)'),
  ('sigxmaxld', 'float', 'Maximum horizontal amplitude (linear decoupled)'),
  ('sigyminld', 'float', 'Minimum vertical amplitude (linear decoupled)'),
  ('sigyavgld', 'float', 'Mean vertical amplitude (linear decoupled)'),
  ('sigymaxld', 'float', 'Maximum vertical amplitude (linear decoupled)'),
  ('sigxminnld', 'float','Minimum horizontal amplitude (nonlinear decoupled)'),
  ('sigxavgnld', 'float', 'Mean horizontal amplitude (nonlinear decoupled)'),
  ('sigxmaxnld', 'float','Maximum horizontal amplitude (nonlinear decoupled)'),
  ('sigyminnld', 'float', 'Minimum vertical amplitude (nonlinear decoupled)'),
  ('sigyavgnld', 'float', 'Mean vertical amplitude (nonlinear decoupled)'),
  ('sigymaxnld', 'float', 'Maximum vertical amplitude (nonlinear decoupled)'),
  ('emitx', 'float', 'Emittance Mode I'),
  ('emity', 'float', 'Emittance Mode II'),
  ('betx2', 'float', 'Secondary horizontal beta-function'),
  ('bety2', 'float', 'Secondary vertical beta-function'),
  ('qpx', 'float', "Q'x"),
  ('qpy', 'float', "Q'y"),
  ('version', 'float', 'Dummy1'),
  ('cx', 'float', 'Dummy2'),
  ('cy', 'float', 'Dummy3'),
  ('csigma', 'float', 'Dummy4'),
  ('xp', 'float', 'Dummy5'),
  ('yp', 'float', 'Dummy6'),
  ('delta', 'float', 'Dummy7'),
  ('dnms', 'float', 'Internal1'),
  ('trttime', 'float', 'Internal2')]
  key=['six_input_id','row_num']

class Files(object):
  fields=[
  ('env_id','int','study id'),
  ('path','str','file path'),
  ('content','blob','file content')]
  key=['env_id','path']
