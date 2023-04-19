import os
import time
import json
from rdkit import Chem
from rdkit.Chem import MolStandardize
from kafka import KafkaConsumer
from kafka import TopicPartition
from openbabel import pybel
from typing import Collection, Dict, List, Optional, Tuple, Union
from utils import canonical_smiles,\
    producer_message, get_kafka_config, consumer_config
import random
import string

uc = MolStandardize.charge.Uncharger()


def convert3D(smi: str):
    '''the default method for rdkit to generate 3D conformations is ETKDG'''
    obmol = pybel.readstring('smi',smi)
    obmol.make3D(forcefield="mmff94", steps=500)
    mol2_string = obmol.write(format='mol2')
    obmol = pybel.readstring('mol2', mol2_string)
    pdbqt_string = obmol.write(format='pdbqt')
    return pdbqt_string.replace('UNL','LIG')

# def convert3D(smi: str):
#     '''the default method for rdkit to generate 3D conformations is ETKDG'''
#     mol = Chem.MolFromSmiles(smi)
#     mol = Chem.AddHs(mol)
#     AllChem.EmbedMolecule(mol, randomSeed=1)
#     AllChem.MMFFOptimizeMolecule(mol)
#     obmol = pybel.readstring('mol', Chem.MolToMolBlock(mol))
#     mol2_string = obmol.write(format='mol2')
#     obmol = pybel.readstring('mol2', mol2_string)
#     pdbqt_string = obmol.write(format='pdbqt')
#     return pdbqt_string

def consumer_message():
    # consumer
    consumer, keep_alive, servers = consumer_config()

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        i = 1
        for msg in msgs['data']['compoundList']:
            try:
                smi = msg["smiles"]
                pdbqt = convert3D(smi)
                print(i)
                i+=1
            except:
                pass
            else:
                res.append({"caddId":msg['caddId'], 'pdbqt':pdbqt, 'smiles':smi})

        # producter result
        msgs['data']['compoundList'] = res
        msgs.update({'partition': os.getenv('partition')})
        msgs['sendTime'] = int(time.time() * 1000)
        producer_message(servers, os.getenv('to_topic'), json.dumps(msgs), True)
        print('ok len(smi)=', len(res))
        if not keep_alive:
            break

if __name__ == "__main__":
    start = time.time()
    # consumer message
    consumer_message()
    print(time.time()-start)


