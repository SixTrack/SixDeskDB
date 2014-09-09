class Env(object):
  fields=[
  ('keyname','str','key'),
  ('value','str','value'),
  ('mtime','float','last modification time')]
  key=['keyname']

class Mad_Run(object):
  fields=[
  ('run_id','str','mad run name'),
  ('seed','int','seed value'),
  ('mad_in','blob','mad_in file'),
  ('mad_out','blob','mad_out file'),
  ('mad_lsf','blob','mad_lsf file'),
  ('mad_log','blob','mad_log file'),
  ('mad_out_mtime','float','last modification time')]
  key=['run_id','seed']

class Da_Res(object):
  fields=[
      ('study', '|S100'), ('tunex','float'),('tuney','float'),
      ('seed','int'),('angle','float'),
      ('achaos','float'),('achaos1','float'),
      ('alost1','float'),('alost2','float'),
      ('Amin','float'),('Amax','float'),('mtime','int')]
  key=['study','tunex','tuney']

class Mad_Res(object):
  fields=[
  ('seed','int','seed value'),
  ('fort2','blob','fort2 file'),
  ('fort8','blob','fort8 file'),
  ('fort16','blob','fort16 file'),
  ('fort_mtime','float','last modification time')]
  key=['seed']

class Six_Be(object):
  fields=[
  ('seed', 'int', 'seed value'),
  ('tunex', 'float', 'tunex value'),
  ('tuney' ,'float', 'tuney value'),
  ('beta11', 'float', 'beta11 value'),
  ('beta12', 'float', 'beta12 value'),
  ('beta22', 'float', 'beta12 value'),
  ('beta21', 'float', 'beta12 value'),
  ('qx', 'float', 'qx value'),
  ('qy', 'float', 'qy value'),
  ('dqx', 'float', 'dqx value'),
  ('dqy', 'float', 'dqy value'),
  ('x', 'float', 'x value'),
  ('xp', 'float', 'xp value'),
  ('y', 'float', 'y value'),
  ('yp', 'float', 'yp value'),
  ('sigma', 'float', 'sigma value'),
  ('delta', 'float', 'delta value'),
  ('deltap', 'float', 'deltap value'),
  ('qx1', 'float', 'qx1 value'),
  ('qy1', 'float', 'qy1 value'),
  ('qx2', 'float', 'qx2 value'),
  ('qy2', 'float','qy2 value'),
  ('emitn', 'float', 'emitn value'),
  ('gamma', 'float', 'gamma value'),
  ('mtime', 'float', 'modification time'),]
  key=['seed','tunex','tuney']

class Six_In(object):
  fields=[
  ('id', 'int', 'unique id'),
  ('seed', 'int', 'seed value'),
  ('simul', 'string', 'simul'),
  ('tunex', 'float', 'tunex value'),
  ('tuney' ,'float', 'tuney value'),
  ('amp1', 'float', 'amp1 value'),
  ('amp2', 'float', 'amp2 value'),
  ('turns', 'string', 'turns value'),
  ('angle', 'float', 'angle value'),
  ('fort3','blob','fort3 file'),
  ('mtime','float','last modification time')]
  key=['seed','simul','tunex','tuney','amp1','amp2','turns','angle']

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
  ('trttime', 'float', 'Internal2'),
  ('mtime','float','last modification time')]
  key=['six_input_id','row_num']


class Da_Post(object):
  fields=[('name', 'str',''),
         ('tunex','float',''),
         ('tuney','float',''),
         ('seed','int',''),
         ('angle','float',''),
         ('achaos','float',''),
         ('achaos1','float',''),
         ('alost1','float',''),
         ('alost2','float',''),
         ('Amin','float',''),
         ('Amax','float',''),
         ('mtime','float','')]

class Files(object):
  fields=[
  ('path','str','file path'),
  ('content','blob','file content'),
  ('mtime','float','file modification time')]
  key=['path']

class Da_Vst(object):
  fields=[
  ('seed', 'int', 'seed value'),
  ('tunex', 'float', 'tunex value'),
  ('tuney' ,'float', 'tuney value'),
  ('DAstrap', 'float', 'DAs trap. integ.'),
  ('DAwtrap', 'float', 'DAw trap. integ.'),
  ('DAssimp', 'float', 'DAs simp. integ.'),
  ('DAwsimp', 'float', 'DAw simp. integ.'),
  ('DAstraperr', 'float', 'error DAs trap. integ.'),
  ('DAstraperrang', 'float', 'angular error DAs trap. integ.'),
  ('DAstraperramp', 'float', 'amplitude error DAs trap. integ.'),
  ('nturn', 'float', 'lost turn number [turnstep,2*turnstep,...]'),
  ('tlossmin', 'float', 'minimum lost turn number over all angles'),
  ('mtime','float','last modification time')]
  key=['seed','tunex','tuney','nturn']

acc_var = ['BNL','COLUMNS','CORR_TEST','G_FILENAME_ENCODING','LHCDesHome',
    'LHCDesName','LHCDescrip','LINES','MADX','MADX_PATH','SIXTRACKBNLEXE',
    'SIXTRACKDAEXE','SIXTRACKEXE','basedir','beam','boincdir','bunch_charge',
    'chrom','chrom_eps','chromx','chromy','cronlogdir','cronlogs','da','dalsfq',
    'deltax','deltay','dimda','dimen','dpini','dpmax','e0','emit','fort_34',
    'gamma','ibtype','idfor','iend','iendmad','iplot','ista','istamad','kendl',
    'kinil','kmaxl','kstep','kvar','level1','level2','level4','long','longlsfq',
    'lsfjobtype','lsfq','madlsfq','ns1l','ns2l','nsincl','platform','pmass',
    'reson','runtype','scratchdir','short','shortlsfq','sixdeskAuthor',
    'sixdeskComments','sixdeskCpuSpeedMin','sixdeskFileName','sixdeskOsMax',
    'sixdeskOsMin','sixdeskPriority','sixdeskProgramDescription',
    'sixdeskProgramName','sixdeskStatus','sixdeskTargetFileName',
    'sixdeskTaskGroupDescription','sixdeskTaskGroupName','sixdeskVersion',
    'sixdeskboincdir','sixdeskboincdirname','sixdeskboinctest','sixdeskcastor',
    'sixdeskclientv','sixdeskcr','sixdeskecho','sixdeskexec','sixdeskforce',
    'sixdeskfpopse','sixdeskhome','sixdeskjobs','sixdeskjobs_logs',
    'sixdesklevel','sixdesklogdir','sixdesklogs','sixdeskpairs','sixdeskparts',
    'sixdeskpath','sixdeskplatform','sixdeskpts','sixdeskstudy','sixdesktrack',
    'sixdeskturns','sixdeskwork','sixtrack_input','sussix','trackdir','tune',
    'tunex','tunex1','tuney','tuney1','turnse','turnsemax','turnsl','turnsle',
    'workspace','writebinl']

def_var = ['COMPIZ_BIN_PATH', 'COMPIZ_CONFIG_PROFILE',
    'DBUS_SESSION_BUS_ADDRESS', 'DEFAULTS_PATH', 'DESKTOP_SESSION',
    'GDMSESSION', 'GNOME_DESKTOP_SESSION_ID', 'GNOME_KEYRING_CONTROL',
    'GNOME_KEYRING_PID', 'GPG_AGENT_INFO', 'GTK_MODULES', 'MANDATORY_PATH',
    'SESSION_MANAGER', 'SSH_AGENT_PID', 'SSH_AUTH_SOCK', 'UBUNTU_MENUPROXY',
    'WINDOWID', 'XDG_CONFIG_DIRS', 'XDG_CURRENT_DESKTOP', 'XDG_DATA_DIRS',
    'XDG_RUNTIME_DIR', 'XDG_SEAT_PATH', 'XDG_SESSION_COOKIE','XDG_SESSION_PATH']
