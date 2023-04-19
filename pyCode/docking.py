import os
import json
import time
from vina import Vina
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
from openbabel import pybel
from tempfile import NamedTemporaryFile
from typing import Collection, Dict, List, Optional, Tuple, Union
from utils import canonical_smiles, \
    producer_message, get_kafka_config, consumer_config


# def convert_receptor(file_path:str):
#     if not os.path.exists(file_path):
#         raise RuntimeError('Error: file %s does not exist.' % file_path)
#     filename, extension = os.path.splitext(file_path)
#     if extension == '.pdbqt':
#         return file_path
#     elif extension == '.pdb':
#         obmol = pybel.readfile('pdb', file_path)
#         obmol.write(format='pdbqt',filename=filename+'.pdbqt',overwrite=True)
#     elif extension == '.mol2':
#         obmol = pybel.readfile('mol2', file_path)
#         obmol.write(format='pdbqt',filename=filename+'.pdbqt',overwrite=True)
#     return filename+'.pdbqt'

def convert_receptor(config_msg):
    with NamedTemporaryFile('w+t', delete=False, suffix='.' + config_msg['receptorSuffix']) as f:
        receptor_name = f.name
        f.write(config_msg['receptor'])
    if config_msg['receptorSuffix'] == 'pdbqt':
        return receptor_name
    else:
        convert_receptor_name = os.path.splitext(receptor_name)[0] + '.pdbqt'
        cmd = ['pythonsh', './Utilities24/prepare_receptor4.py', '-r', receptor_name, '-o', convert_receptor_name]
        cmd = ' '.join(cmd)
        os.system(cmd)
        os.remove(receptor_name)
        return convert_receptor_name


def vina_config(config_msg):
    receptor_name = convert_receptor(config_msg)
    v = Vina(sf_name='vina', seed=123, verbosity=0)
    # v = Vina(sf_name='vina', seed=123, verbosity=1)
    # v.set_receptor(flex_pdbqt_filename=config_msg['receptor'])
    v.set_receptor(rigid_pdbqt_filename=receptor_name)
    v.compute_vina_maps(center=[float(config_msg['centerX']),
                                float(config_msg['centerY']),
                                float(config_msg['centerZ'])],
                        box_size=[float(config_msg['areaX']),
                                  float(config_msg['areaY']),
                                  float(config_msg['areaZ'])])
    return v, receptor_name


def vina_docking(v, ligand, config_msg):
    v.set_ligand_from_string(ligand['pdbqt'])
    if config_msg['notClick']:
        v.dock(exhaustiveness=int(config_msg['exhaustiveness']), n_poses=1)
        score = v.poses().split('    ', 2)[1]
        print('score:', score)
        print('pose1:', v.poses())
        # obmol = pybel.readstring('pdbqt', v.poses())
        # pose = obmol.write(format='sdf')
        res = [{'score': score, 'content': v.poses()}]
    else:
        '''1-CLICK DOCKING'''
        res = []
        v.dock(exhaustiveness=8, n_poses=4)
        for pose in v.poses().split('ENDMDL\n')[:-1]:
            score = pose.split('    ', 2)[1]
            pose = pose + 'ENDMDL\n'
            # pose = pose.split('____\n',1)[1] +'ENDMDL\n'
            print('score:', score)
            print('pose2:', pose)
            # obmol = pybel.readstring('pdbqt', pose)
            # pose = obmol.write(format='sdf')
            res.append({'score': score, 'content': pose})

        # v.write_poses(pdbqt_filename=config_msg['result'],
        #               overwrite=True)

    return {'caddId': ligand['caddId'], 'scoreList': res}


def consumer_message():
    # consumer
    consumer, keep_alive, servers = consumer_config()
    for msgs in consumer:
        res = []
        print('startTime', time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime()))
        try:
            msgs = json.loads(msgs.value.decode("utf-8"))
            msgs.update({'receiveTime': int(time.time() * 1000)})
            ligand_list = msgs['data']['dockingCompoundList']
            del msgs['data']['dockingCompoundList']

            v, receptor_name = vina_config(msgs['data'])
            del msgs['data']['receptor']
            for ligand in ligand_list:
                print('this startTime', time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime()))
                print(ligand['caddId'])
                print(ligand['pdbqt'])
                res.append(vina_docking(v, ligand, msgs['data']))
                print('this endTime', time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime()))
            # print(receptor_name)
            os.remove(receptor_name)

            # producter result
            msgs['data']['compoundList'] = res
            msgs.update({'partition': os.getenv('partition')})
            msgs['sendTime'] = int(time.time() * 1000)
        except Exception as e:
            print(e)
            msgs.update({'receiveTime': int(time.time() * 1000)})
            msgs['data']['compoundList'] = []
            msgs.update({'partition': os.getenv('partition')})
            msgs['sendTime'] = int(time.time() * 1000)
            producer_message(servers, os.getenv('to_topic'), json.dumps(msgs), True)
            continue
        producer_message(servers, os.getenv('to_topic'), json.dumps(msgs), True)
        print('endTime', time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime()))
        print('ok len(smi)=', len(res))
        if not keep_alive:
            break


if __name__ == "__main__":
    # consumer message
    consumer_message()
