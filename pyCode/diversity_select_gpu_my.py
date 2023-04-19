import os
import json
import time
import math
import random
import numpy as np
from rdkit import Chem
from rdkit.ML.Cluster import Butina
from rdkit.Chem.rdchem import Mol
from rdkit.Chem.AtomPairs import Pairs, Torsions
from rdkit.Chem import AllChem, DataStructs, MACCSkeys
from typing import Collection, Dict, List, Optional, Tuple, Union
from kafka import KafkaConsumer
from kafka import TopicPartition
from utils import producer_message, get_kafka_config
from tempfile import NamedTemporaryFile
from distanceMatrixGPU.cudaDistanceMatrix import DistanceMatrix
from Butina import ClusterData

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


def get_fpsArray(compoundList:List, descriptor:str):
    ''':parameter
    compound: the molecule in the database
    descritor: functions to generate fingerprint,
            one of ['TopologicalFingerprints','MASSCkeys','AtomPairs','TopologicalTorsions','ECFPs','FCFPs']
    '''
    start = time.time()
    print('Start get_fpsArray')
    idArray = np.empty(len(compoundList),'<U30')
    fp_len = len(fp_func(Chem.MolFromSmiles(compoundList[0]['smiles']), descriptor))
    fpsArray = np.zeros((len(compoundList), fp_len), dtype=np.int8)
    for index, compound in enumerate(compoundList):
        fp = fp_func(Chem.MolFromSmiles(compound['smiles']), descriptor)
        DataStructs.ConvertToNumpyArray(fp, fpsArray[index, :])
        idArray[index] = compound['caddId']
    print('generate fpsArray time', time.time() - start)
    return fpsArray, idArray


def distance_matrix(fpsArray):
    '''first generate the distance matrix by GPU'''
    with NamedTemporaryFile('w+t', delete=False, suffix='.h5') as f:
        dist_file_name = f.name
    DM = DistanceMatrix(file_name=dist_file_name)
    start = time.time()
    print('Start distance matrix')
    DM.calculate_distmatrix(fpsArray)
    print('generate the distance matrix time',time.time()-start)
    flatten_distance_matrix = DM.get_distance_matrix(fullMatrix=False)
    os.remove(dist_file_name)
    return flatten_distance_matrix

def clustering(dists:List, nfps:int, cut_off:float,sample_num:int, idArray:List):
    res_index = []
    start = time.time()
    print('Start clustering')
    cs = ClusterData(dists, nfps, cut_off, isDistData=True)
    print('Clustering complete time',time.time()-start)
    if sample_num > len(cs):
        for cluster in cs:
            if len(cluster)==1:
                snum=1
            else:
                snum = int(sample_num * (len(cluster) / nfps))
            # print(snum)
            res_index += random.sample(cluster, snum)
    else:
        cluster_index = random.sample(range(len(cs)), sample_num)
        for index in cluster_index:
            res_index += random.sample(cs[index], 1)
    res_index = idArray[res_index]
    return list(res_index)


def diversity(data):
    fpsArray, idArray = get_fpsArray(data['compoundList'], data['descriptor'])
    nfps = len(idArray)
    flatten_distance_matrix = distance_matrix(fpsArray)
    res_index = clustering(flatten_distance_matrix, nfps,
                           float(data['cut_off']), int(data['sample_num']),
                           idArray)

    return res_index


def consumer_message():
    # get kafka config
    config_dict = get_kafka_config()
    servers = config_dict['bootstrap_servers'].split(',')
    from_topic = config_dict['from_topic']
    partition = config_dict['partition']
    to_topic = config_dict['to_topic']

    #consumer
    consumer = KafkaConsumer(bootstrap_servers=servers, api_version = (0,10))
    consumer.assign([TopicPartition(from_topic, int(partition))])
    compoundList = []
    msgs_num = 0
    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        msgs_num += 1
        compoundList += msgs['data']['compoundList']
        if msgs_num == int(msgs['data']['totalMsgCount']):
            print("msgs_num == int(msgs['data']['totalMsgCount'])",int(msgs['data']['totalMsgCount']))
            msgs['data']['compoundList'] = compoundList
            res = diversity(msgs['data'])

            # producter result
            #每次发送1000条
            msgs['data']['totalMsgCount'] = math.ceil(len(res)/1000)
            for i in range(0, len(res), 1000):
                msgs['data']['compoundList'] = res[i:i + 1000]
                msgs.update({'partition': partition})
                msgs['sendTime'] = int(time.time())
                producer_message(servers, to_topic, json.dumps(msgs),True)

            print('diversity select finished!!!')

            compoundList = []
            msgs_num = 0

            break

        else:
            print("msgs_num:{} != int(msgs['data']['totalMsgCount']):{}".format(
                msgs_num, int(msgs['data']['totalMsgCount'])))




if __name__ == "__main__":
    start = time.time()
    # consumer message
    consumer_message()
    print('run time: '+str(time.time()-start))