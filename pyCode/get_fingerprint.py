import json
import time
import numpy as np
from rdkit import Chem
from rdkit.Chem import AllChem, DataStructs, MACCSkeys
from rdkit.Chem.AtomPairs import Pairs, Torsions
from rdkit.Chem.rdchem import Mol
from kafka import KafkaConsumer
from kafka import TopicPartition
from typing import Collection, Dict, List, Optional, Tuple, Union
from utils import canonical_smiles,\
    producer_message, consumer_config
from rdkit.DataStructs import cDataStructs



def convert(compound:dict):
    ''':parameter
    compound: the molecule in the database
    descritor: functions to generate fingerprint,
            one of ['TopologicalFingerprints','MASSCkeys','AtomPairs','TopologicalTorsions','ECFPs','FCFPs']
    :return the molecule fingerprint
    '''
    fp = AllChem.GetMorganFingerprintAsBitVect(Chem.MolFromSmiles(compound['smiles']), radius=2, nBits=1024)
    fpsArray = np.zeros(1, dtype=np.int8)
    DataStructs.ConvertToNumpyArray(fp, fpsArray)
    fpsStr = ''.join(map(str,fpsArray.tolist()))
    return [{'caddId': compound['caddId'], 'fp': fpsStr}]



def consumer_message():
    # consumer
    consumer, keep_alive, servers = consumer_config()

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        for compound in msgs['data']['compoundList']:
            res += convert(compound)

        # producter result
        msgs['data']['compoundList'] = res
        msgs.update({'partition': os.getenv('partition')})
        msgs['sendTime'] = int(time.time() * 1000)
        producer_message(servers, os.getenv('to_topic'), json.dumps(msgs), True)
        print('ok len(smi)=', len(res))
        if not keep_alive:
            break

if __name__ == "__main__":
    # consumer message
    consumer_message()
