import os
import json
import time
from rdkit import Chem
from kafka import KafkaConsumer
from kafka import TopicPartition
from typing import Collection, Dict, List, Optional, Tuple, Union
from openbabel import pybel
from utils import canonical_smiles,\
    producer_message, get_kafka_config, consumer_config

def smilesToSdf(compound: str):
    obmol = pybel.readstring('smi', compound)
    obmol.make2D()
    # sdf = obmol.write('sdf')
    return [{'input': compound, 'output': obmol.write('sdf')}]

def smilesToInchi(compound: str):
    smi = canonical_smiles(compound)
    return [{'input': compound, 'output': Chem.MolToInchi(Chem.MolFromSmiles(smi))}]

def smilesToInchiKey(compound: str):
    smi = canonical_smiles(compound)
    return [{'input': compound, 'output': Chem.MolToInchiKey(Chem.MolFromSmiles(smi))}]

def inchiTosmiles(compound:str):
    mol = Chem.MolFromInchi(compound)
    return [{'input': compound, 'output': Chem.MolToSmiles(mol)}]

def smilesToMolBlock(compound:str):
    smi = canonical_smiles(compound)
    return [{'input': compound, 'output': Chem.MolToMolBlock(Chem.MolFromSmiles(smi))}]

def convert_func(compound:str, operate:str):
    # func_dict = {
    #     'smilesToSdf': smilesToSdf(compound),
    #     'smilesToInchi': smilesToInchi(compound),
    #     'smilesToInchiKey': smilesToInchiKey(compound),
    #     'smilesToMolBlock': smilesToMolBlock(compound),
    #     'inchiTosmiles': inchiTosmiles(compound)
    # }
    if "smilesToSdf" == operate:
        return smilesToSdf(compound)
    if "smilesToInchi" == operate:
        return smilesToInchi(compound)
    if "smilesToInchiKey" == operate:
        return smilesToInchiKey(compound)
    if "smilesToMolBlock" == operate:
        return smilesToMolBlock(compound)
    if "inchiTosmiles" == operate:
        return inchiTosmiles(compound)
    # return func_dict[operate]

def convert(compound:str, operate:str):
    ''':parameter
    compound: the compound content entered by the user
    operate: choose one of ['smilesToInchi', 'smilesToSdf', 'smilesToInchiKey', 'smilesToMolBlock]
    :return converted format content
    '''
    return convert_func(compound, operate)

def consumer_message():
    # consumer
    consumer, keep_alive, servers = consumer_config()

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        for compound in msgs['data']['compoundList']:
            res += convert(compound, msgs['data']['operate'])

        # producter result
        msgs['data']['compoundList'] = res
        msgs.update({'partition': os.getenv('partition')})
        msgs['sendTime'] = int(time.time() * 1000)
        producer_message(servers, os.getenv('to_topic'), json.dumps(msgs), True)
        print('ok len(smi)=', len(res))
        if not keep_alive:
            break

    # return json.dumps({file_type: res})

if __name__ == "__main__":
    # consumer message
    consumer_message()
