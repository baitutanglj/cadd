import os
import time
import json
from rdkit import Chem
from kafka import KafkaConsumer
from kafka import TopicPartition
from typing import Collection, Dict, List, Optional, Tuple, Union
from utils import get_kafka_config, \
    producer_message,input_type, consumer_config

def subsearch(compound:str, query: str, chirality: bool=False):
    '''
    compound: the molecules in the database
    query: the molecule to query
    regardless of the chirality, chiral molecules maybe match
    the non-chiral patterns and erroneous chiral patterns
    :return bool'''
    compound_mol = Chem.MolFromSmiles(compound['smiles'])
    query = Chem.MolFromSmiles(query)
    flag = compound_mol.HasSubstructMatch(query, useChirality=chirality)
    return flag

def consumer_message():
    # consumer
    consumer, keep_alive, servers = consumer_config()

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        query = msgs['data']['queries']
        for compound in msgs['data']['compoundList']:
            flag = subsearch(compound, query)
            if flag:
                res += [{'caddId':compound['caddId']}]

        # producter result
        msgs['data']['compoundList'] = res
        msgs.update({'partition': os.getenv('partition')})
        msgs['sendTime'] = int(time.time() * 1000)
        producer_message(servers, os.getenv('to_topic'), json.dumps(msgs), True)
        print('ok len(smi)=', len(res))
        if not keep_alive:
            break

if __name__ == "__main__":
    consumer_message()