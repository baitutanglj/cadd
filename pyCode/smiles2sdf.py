import os
import json
import time
from kafka import KafkaConsumer
from kafka import TopicPartition
from typing import Collection, Dict, List, Optional, Tuple, Union
from openbabel import pybel
from utils import canonical_smiles,\
    producer_message, get_kafka_config, consumer_config

def smiles2sdf(compound: Dict):
    obmol = pybel.readstring('smi', compound['smiles'])
    obmol.make2D()
    sdf = obmol.write('sdf')

    return [{'smiles': compound['smiles'], 'sdf': sdf}]


def consumer_message():
    # consumer
    consumer, keep_alive, servers = consumer_config()

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        for compound in msgs['data']['compoundList']:
            res += smiles2sdf(compound)

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
    start = time.time()
    # consumer message
    consumer_message()

    print('run time: '+str(time.time()-start))