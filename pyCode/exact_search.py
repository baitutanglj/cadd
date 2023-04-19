import os
import time
import json
from rdkit import Chem
from kafka import KafkaConsumer
from kafka import TopicPartition
from openbabel import pybel
from typing import Collection, Dict, List, Optional, Tuple, Union
from utils import enumerate_smiles, tautomer_enumerator,\
    canonical_smiles, get_kafka_config, producer_message,input_type

# C1=CC=C2C(=C1)C(=O)N([C@H]1CC(=O)[C@@H]3OCC1O3)C2=O
def get_DrawMolecule(smi: str):
    '''
    :param smi:the Draw molecule smiles
    :return :the cannonical smiles
    '''
    smi = canonical_smiles(smi)
    return smi


def match(query: str, best_match: bool=True):
    # query_enume = tautomer_enumerator(query)
    # query_enume = enumerate_smiles(query)
    inchi_list = [Chem.MolToInchi(Chem.MolFromSmiles(x))
                  for x in enumerate_smiles(query)]
    if best_match:
        return [inchi_list[0]]
    else:
        return inchi_list

def consumer_message():
    # get kafka config
    config_dict = get_kafka_config()
    servers = config_dict['bootstrap_servers'].split(',')
    from_topic = config_dict['from_topic']
    partition = config_dict['partition']
    to_topic = config_dict['to_topic']

    # consumer message
    consumer = KafkaConsumer(bootstrap_servers=servers, auto_offset_reset='earliest', api_version=(0,10))
    consumer.assign([TopicPartition(from_topic, int(partition))])

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        for query in msgs['queries']:
            query = input_type(query)
            query_key = list(query.keys())[0]
            if query_key in ['smiles', 'sdf']:
                res += [{query_key: match(query[query_key], best_match=bool(msgs['best_match']))}]
            # else:
            #     res += [query]

        # producter result
        producer_message(servers, to_topic, json.dumps(res),True)

        break
        # print(res)
    # return json.dumps({file_type: res})


if __name__ == "__main__":
    start = time.time()
    consumer_message()
    print('run time: '+str(time.time()-start))










