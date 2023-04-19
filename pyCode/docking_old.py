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
from utils import canonical_smiles,\
    producer_message, get_kafka_config, consumer_config

def convert_receptor(file_path:str):
    if not os.path.exists(file_path):
        raise RuntimeError('Error: file %s does not exist.' % file_path)
    filename, extension = os.path.splitext(file_path)
    if extension == '.pdbqt':
        return file_path
    elif extension == '.pdb':
        obmol = pybel.readfile('pdb', file_path)
        obmol.write(format='pdbqt',filename=filename+'.pdbqt',overwrite=True)
    elif extension == '.mol2':
        obmol = pybel.readfile('mol2', file_path)
        obmol.write(format='pdbqt',filename=filename+'.pdbqt',overwrite=True)
    return filename+'.pdbqt'


def vina_config(config_msg:Dict):
    with NamedTemporaryFile('w+t', delete=False, suffix='.pdbqt') as f:
        receptor_name = f.name
        f.write(config_msg['receptor'])
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


def vina_docking(v, ligand:Dict, config_msg:Dict):
    v.set_ligand_from_string(ligand['pdbqt'])
    if config_msg['notClick']:
        v.dock(exhaustiveness=int(config_msg['exhaustiveness']), n_poses=1)
        score = v.poses().split('    ', 2)[1]
        obmol = pybel.readstring('pdbqt', v.poses())
        pose = obmol.write(format='sdf')
        res = [{'score': score, 'content': pose}]
    else:
        '''1-CLICK DOCKING'''
        res = []
        v.dock(exhaustiveness=int(config_msg['exhaustiveness']), n_poses=1)
        for pose in v.poses().split('ENDMDL\n')[:-1]:
            score = pose.split('    ',2)[1]
            pose = pose + 'ENDMDL\n'
            # pose = pose.split('____\n',1)[1] +'ENDMDL\n'
            obmol = pybel.readstring('pdbqt', pose)
            pose = obmol.write(format='sdf')
            res.append({'score': score, 'content': pose})

        # v.write_poses(pdbqt_filename=config_msg['result'],
        #               overwrite=True)

    return {'caddId':ligand['caddId'], 'scoreList':res}


def consumer_message():
    # get kafka config
    #config_dict = get_kafka_config()
    servers = os.getenv('bootstrap_servers').split(',')
    from_topic = os.getenv('from_topic')
    partition = os.getenv('partition')
    to_topic = os.getenv('to_topic')
    print("bootstrap_servers:", servers)
    print("from_topic:", from_topic)
    print("partition:", partition)
    print("to_topic:", to_topic)

    #consumer
    # consumer = KafkaConsumer(bootstrap_servers=servers, auto_offset_reset='earliest', api_version=(0,10))
    consumer = KafkaConsumer(bootstrap_servers=servers, auto_offset_reset='earliest', api_version=(0,10))
    consumer.assign([TopicPartition(from_topic, int(partition))])

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        ligand_list = msgs['data']['dockingCompoundList']
        del msgs['data']['dockingCompoundList']
        v,receptor_name = vina_config(msgs['data'])
        del msgs['data']['receptor']
        for ligand in ligand_list:
            res.append(vina_docking(v, ligand, msgs['data']))
        # print(receptor_name)
        os.remove(receptor_name)

        # producter result
        msgs['data']['dockingCompoundList'] = res
        msgs.update({'partition': partition})
        msgs['sendTime'] = int(time.time()*1000)
        producer_message(servers, to_topic, json.dumps(msgs),True)

        break

if __name__ == "__main__":
    # consumer message
    consumer_message()
