import os
import json
import time
from vina import Vina
from openbabel import pybel
from tempfile import NamedTemporaryFile
from typing import Collection, Dict, List, Optional, Tuple, Union

v = Vina(sf_name='vina', seed=123, verbosity=1)
receptor_name = '/home/linjie/Programs/4UG2/rep.pdbqt'
v.set_receptor(rigid_pdbqt_filename=receptor_name)
v.set_ligand_from_file(pdbqt_filename='/home/linjie/Programs/4UG2/4ug2_lig.pdbqt')
v.compute_vina_maps(center=[-6.9465, -18.7935, 19.9355], box_size=[20, 20, 22])
v.dock(exhaustiveness=10, n_poses=8)
score = v.poses().split('    ', 2)[1]
print('score:', score)
print('pose1:', v.poses())
res = [{'score': score, 'content': v.poses()}]


print('\nError: SWIG failed.',
                  'You may need to manually specify the location of Open Babel include and library directories. '
                  'For example:',
                  '  python setup.py build_ext -I{} -L{}'.format(1, 2),
                  '  python setup.py install',
                  sep="\n")
