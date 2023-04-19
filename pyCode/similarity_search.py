import os
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
from utils import get_kafka_config, \
    producer_message, canonical_smiles, consumer_config


from rdkit.DataStructs import cDataStructs

similarityFunctions = {
  "Tanimoto": cDataStructs.TanimotoSimilarity,
  "Dice": cDataStructs.DiceSimilarity,
  "Cosine": cDataStructs.CosineSimilarity,
  "Sokal": cDataStructs.SokalSimilarity,
  "Russel": cDataStructs.RusselSimilarity,
  "RogotGoldberg": cDataStructs.RogotGoldbergSimilarity,
  "AllBit": cDataStructs.AllBitSimilarity,
  "Kulczynski": cDataStructs.KulczynskiSimilarity,
  "McConnaughey": cDataStructs.McConnaugheySimilarity,
  "Asymmetric": cDataStructs.AsymmetricSimilarity,
  "BraunBlanquet": cDataStructs.BraunBlanquetSimilarity
}

def fp_func(mol:Mol, func_name: str):
    func_dict = {
        'TopologicalFingerprints':Chem.RDKFingerprint(mol),
        'MASSCkeys': MACCSkeys.GenMACCSKeys(mol),
        'AtomPairs': Pairs.GetAtomPairFingerprint(mol),
        'TopologicalTorsions':Torsions.GetTopologicalTorsionFingerprintAsIntVect(mol),
        'ECFPs': AllChem.GetMorganFingerprintAsBitVect(mol, radius=2,nBits=1024),
        'FCFPs': AllChem.GetMorganFingerprintAsBitVect(mol, radius=2,nBits=1024,useFeatures=True)
    }
    return func_dict[func_name]

scoring_func = {
    'Best':np.max,
    'Average':np.mean
}

def similarity(compound: str, query: str, descritor, similarity_index):
    ''':parameter
    compound: the molecule in the database
    query: the molecule to query
    descritor: functions to generate fingerprint,
            one of ['TopologicalFingerprints','MASSCkeys','AtomPairs','TopologicalTorsions','ECFPs','FCFPs']
    similarity_index: functions for calculating similarity, one of ['Tanimoto', "Dice","Cosine","Sokal",
                    "Russel","RogotGoldberg","AllBit","Kulczynski", "McConnaughey","Asymmetric","BraunBlanquet"]
    :return bool
    '''
    query = canonical_smiles(query)
    compound = fp_func(Chem.MolFromSmiles(compound),descritor)
    query = fp_func(Chem.MolFromSmiles(query),descritor)
    score = DataStructs.FingerprintSimilarity(compound, query,
                                              metric=similarityFunctions[similarity_index])
    # print(score)
    return score
    # if score >= cut_off:
    #     return True
    # else:
    #     return False

def consumer_message():


    # consumer
    consumer, keep_alive, servers = consumer_config()

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []

        descritor = msgs['data']['descriptor']
        similarity_index = msgs['data']['similarity_index']
        scoring_method = msgs['data']['scoring_method']

        for compoundDict in msgs['data']['compoundList']:
            compoundId = compoundDict['caddId']
            compound = compoundDict['smiles']
            score = []
            for query in msgs['data']['queries']:
                if scoring_method == "":
                    res += [{'caddId':compoundId, 'score': similarity(compound, query, descritor, similarity_index)}]
                else:
                    score += [similarity(compound, query, descritor, similarity_index)]

            if scoring_method != "":
                res += [{'caddId':compoundId, 'score': scoring_func[scoring_method](score)}]

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