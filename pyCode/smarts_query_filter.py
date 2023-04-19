import os
import time
import json
from rdkit import Chem
from kafka import KafkaConsumer
from kafka import TopicPartition
from typing import Collection, Dict, List, Optional, Tuple, Union
from utils import get_kafka_config, producer_message, consumer_config

def smarts_search(compound: str, queries: List, mode: str,
                  if_include: str,chirality: bool=False):
    '''
    compound: the molecule in the database
    queries: the list of query mol
    mode: match mode choose in  [any,all]
    if_include: whether the query molecule is included ,choose in ['include', 'exclude']
    :return bool'''
    compound_mol = Chem.MolFromSmiles(compound['smiles'])
    if if_include=='include':
        flag_list = [compound_mol.HasSubstructMatch(x, useChirality=chirality) for x in queries]
    else:
        flag_list = [not(compound_mol.HasSubstructMatch(x, useChirality=chirality)) for x in queries]

    flag = eval(mode)(flag_list)
    return [{'caddId':compound['caddId'], 'result':flag}]

def read_smarts(queries:List):
    queries = [Chem.MolFromSmarts(x) for x in queries]
    queries = list(filter(None, queries))
    if len(queries) != 0:
        return queries
    else:
        return False

def consumer_message():


    # consumer
    consumer, keep_alive, servers = consumer_config()

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        mode = msgs['data']['mode']
        if_include = msgs['data']['if_include']
        queries = read_smarts(msgs['data']['queries'])
        if queries:
            for compound in msgs['data']['compoundList']:
                res += smarts_search(compound, queries, mode, if_include)
        else:
            res = []

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