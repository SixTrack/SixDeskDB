import numpy
queries = {
       '10': 'SELECT {names} FROM results WHERE seed in ({seedsSeq}) ' +
              'AND angle in ({anglesSeq}) ORDER BY amp1',

       'else': 'SELECT {names} FROM results INNER JOIN six_post ON (results.six_input_id=six_post.six_input_id AND results.row_num=six_post.row_num) WHERE betx>0'+
              'AND bety>0 AND emitx>0 AND emity>0 AND seed in ({seedsSeq}) ' +
              'AND angle in ({anglesSeq}) ORDER BY amp1'
       }

dataQueried = {
 '10': [('six_input_id','int'),
       ('row_num','int'),
       ('turn_max', 'int'),
       ('sflag', 'int'),
       ('qx', 'float'),
       ('qy', 'float'),
       ('betx', 'float'),
       ('bety', 'float'),
       ('sigx1', 'float'),
       ('sigy1', 'float'),
       ('deltap', 'float'),
       ('dist', 'float'),
       ('distp', 'float'),
       ('qx_det', 'float'),
       ('qx_spread', 'float'),
       ('qy_det', 'float'),
       ('qy_spread', 'float'),
       ('resxfact', 'float'),
       ('resyfact', 'float'),
       ('resorder', 'int'),
       ('smearx', 'float'),
       ('smeary', 'float'),
       ('smeart', 'float'),
       ('sturns1', 'int'),
       ('sturns2', 'int'),
       ('sseed', 'float'),
       ('qs', 'float'),
       ('sigx2', 'float'),
       ('sigy2', 'float'),
       ('sigxmin', 'float'),
       ('sigxavg', 'float'),
       ('sigxmax', 'float'),
       ('sigymin', 'float'),
       ('sigyavg', 'float'),
       ('sigymax', 'float'),
       ('sigxminld', 'float'),
       ('sigxavgld', 'float'),
       ('sigxmaxld', 'float'),
       ('sigyminld', 'float'),
       ('sigyavgld', 'float'),
       ('sigymaxld', 'float'),
       ('sigxminnld', 'float'),
       ('sigxavgnld', 'float'),
       ('sigxmaxnld', 'float'),
       ('sigyminnld', 'float'),
       ('sigyavgnld', 'float'),
       ('sigymaxnld', 'float'),
       ('emitx', 'float'),
       ('emity', 'float'),
       ('betx2', 'float'),
       ('bety2', 'float'),
       ('qpx', 'float'),
       ('qpy', 'float'),
       ('version', 'float'),
       ('cx', 'float'),
       ('cy', 'float'),
       ('csigma', 'float'),
       ('xp', 'float'),
       ('yp', 'float'),
       ('delta', 'float'),
       ('dnms', 'float'),
       ('trttime', 'float'),
       ('mtime','float')],

 '11': [('achaos', 'float'),
       ('al', numpy.dtype([('arr','f8',(48,))])),
       ('amin', 'float'),
       ('amax', 'float'),
       ('achaos1', 'float')],

 '12': [('rad','float'),
       ('distp','float')],

 '13': [('rad','float'),
       ('dist','float')],

 '14': [('achaos', 'float'),
       ('alost3', 'float'),
       ('turn_max', 'float'),
       ('f14', 'int')],

 '15': [('rad','float'),
       ('sturns1','float'),
       ('sturns2','float')],

 '16': [('deltap','float'),
       ('qx','float'),
       ('qy','float')], 

 '17': [('deltap','float'),
       ('qx','float'),
       ('qy','float')], 

 '18': [('rad','float'),
       ('smearx','float')],

 '19': [('rad','float'),
       ('smeary','float')],

 '20': [('rad','float'),
       ('qx_det','float')],

 '21': [('rad','float'),
       ('qy_det','float')],

 '22': [('rad','float'),
       ('(rad1*sigxminnld)','float')],

 '23': [('rad','float'),
       ('(rad1*sigxavgnld)','float')],

 '24': [('rad','float'),
       ('(rad1*sigxmaxnld)','float')],

 '25': [('qx_det+qx', 'float'),
       ('qy_det+qy', 'float'),
       ('qx_det', 'float'),
       ('qy_det', 'float')],

 '26': [('achaos', 'float'),
       ( 'alost2', 'float'),
       ( 'amax', 'float')],
 
 '27': [('al', numpy.dtype([('arr','f8',(48,))]))],

 '28': [('al', numpy.dtype([('arr','f8',(48,))]))],

 '40': [('achaos', 'float'),
       ('al', numpy.dtype([('arr','f8',(48,))])),
       ('amin', 'float'),
       ('amax', 'float'),
       ('achaos1', 'float')]
}
