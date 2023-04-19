import os
import random
from rdkit import Chem
from rdkit.Chem import AllChem
from rdkit.Chem.rdchem import Mol
from rdkit.Chem.EnumerateStereoisomers import \
    EnumerateStereoisomers,StereoEnumerationOptions
from rdkit.Chem.MolStandardize import rdMolStandardize
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
from typing import Collection, Dict, List, Optional, Tuple, Union

random.seed(123)

# def read_molecule(input):
#     format_str = os.path.splitext(input)[-1]
#     try:
#         if format_str == '.smi':
#             mol = Chem.SmilesMolSupplier(input,nameColumn=False, titleLine=False)
#         elif format_str == '.sdf':
#             mol = Chem.SDMolSupplier(input)
#         elif format_str == '.mol':
#             mol = Chem.MolFromMolFile(input)
#         elif format_str == '.mol2':
#             mol = Chem.MolFromMol2File(input)
#         elif format_str == 'pdb':
#             mol = Chem.MolFromPDBFile(input)
#         else:
#             raise Exception('Could not read the {} file format'.format(format_str))
#     except Exception as e:
#         print(e)
#         logging.info(e)
#     else:
#         if len(mol) == 1:
#             return mol[0]
#         else:
#             return mol

def fingerprint(mol,radius=2, nBits=1024):
    if type(mol) == str:
        morgan = AllChem.GetMorganFingerprintAsBitVect(Chem.MolFromSmiles(mol), radius, nBits)
    else:
        morgan = AllChem.GetMorganFingerprintAsBitVect(mol, radius, nBits)
    return morgan

def getInChi(mol):
    if type(mol) == str:
        inchi = Chem.MolToInchi(Chem.MolFromSmiles(mol))
    else:
        inchi = Chem.MolToInchi(mol)
    return inchi

def getInChiKey(mol):
    if type(mol) == str:
        inchikey = Chem.MolToInchiKey(Chem.MolFromSmiles(mol))
    else:
        inchikey = Chem.MolToInchiKey(mol)
    return inchikey

def canonical_smiles(smi: str):
    smi = Chem.MolToSmiles(Chem.MolFromSmiles(smi), canonical=True)
    return smi


# def tautomer_enumerator(smi: str, canonical: bool=False):
#     '''
#     As an aside, it's worth noticing that double bond stereochemistry
#     is removed in all tautomers if the double bond is involved in the tautomerization:
#     '''
#     if canonical:
#         smi = canonical_smiles(smi)
#         mol = Chem.MolFromSmiles(smi)
#     else:
#         mol = Chem.MolFromSmiles(smi)
#     enumerator = rdMolStandardize.TautomerEnumerator()
#     tauts = enumerator.Enumerate(mol)
#     # res = random.sample([Chem.MolToSmiles(x, canonical=True) for x in tauts],10)
#     res = [smi] + [Chem.MolToSmiles(x, canonical=True) for x in tauts]
#     if len(res)>11:
#         res = res[:11]
#     return sorted(set(res),key=res.index)

def tautomer_enumerator(mol: Mol):
    '''
    As an aside, it's worth noticing that double bond stereochemistry
    is removed in all tautomers if the double bond is involved in the tautomerization:
    '''

    # if canonical:
    #     smi = canonical_smiles(smi)
    #     mol = Chem.MolFromSmiles(smi)
    # else:
    #     mol = Chem.MolFromSmiles(smi)
    enumerator = rdMolStandardize.TautomerEnumerator()
    tauts = enumerator.Enumerate(mol)
    # res = random.sample([Chem.MolToSmiles(x, canonical=True) for x in tauts],10)
    res = [Chem.MolToSmiles(x, canonical=True) for x in tauts]
    if len(res)>10:
        res = res[:10]
    return sorted(set(res),key=res.index)

def stereo_enumerator(smi: str, canonical: bool=False):
    mol = Chem.MolFromSmiles(smi)
    opts = StereoEnumerationOptions(tryEmbedding=False, onlyUnassigned=True,
                                    unique=True, maxIsomers=32, rand=0xf00d)
    stereo_list = [Chem.MolToSmiles(x,isomericSmiles=True,canonical=True)
                   for x in EnumerateStereoisomers(mol,options=opts)]
    return stereo_list

def enumerate_smiles(smi: str, canonical: bool=False):
    '''
    As an aside, it's worth noticing that double bond stereochemistry
    is removed in all tautomers if the double bond is involved in the tautomerization.
    Therefore, the tautomeric isomers are enumerated first and then the stereoisomerism
    is enumerated in a cyclic manner.
    '''
    if canonical:
        smi = canonical_smiles(smi)
        mol = Chem.MolFromSmiles(smi)
    else:
        mol = Chem.MolFromSmiles(smi)
    stereo_list = [smi]
    tauts_smi = tautomer_enumerator(mol)
    for s in tauts_smi:
        stereo_list += stereo_enumerator(s)
    return sorted(set(stereo_list),key=stereo_list.index)

def get_kafka_config()->Dict:
    #get kafka config
    with open('/data/cadd/config','r') as f:
        config_dict = {}
        for line in f.readlines():
            key,value = line.strip().split(' ')[0],line.strip().split(' ')[1]
            config_dict[key] = value
    return config_dict

def producer_message(servers, to_topic,result,encoder=False):
    producer = KafkaProducer(bootstrap_servers=servers, max_request_size=2147483647, compression_type="lz4", api_version=(0,10))
    try:
        if encoder:
            producer.send(topic=to_topic, value=bytes(result.encode("utf-8")))
            # print(bytes(result.encode("utf-8")))
        else:
            producer.send(topic=to_topic, value=result)
            # print(result)
    except Exception as e:
        print(e)


class InChiError(Exception):
    pass

def input_type(msg: str):
    from openbabel import pybel
    '''Determine the type of user input
    input type:smiles, InChI, InChIKey,
    '''
    try:
        smi = canonical_smiles(msg)
        return {'smiles':smi}
    except:
        print('no smi')
        try:
            mol = Chem.MolFromInchi(msg)
            if mol == None:
                raise InChiError('InChI Parse Error')
            msg = Chem.MolToSmiles(mol)
            return {'inchi':msg}
        except:
            print('no inchi')
            pass
            try:
                block = msg.strip().split('-')
                if (len(block[0])==14) & \
                    (len(block[1])==10 )& \
                    (len(block[2]) == 1):
                    return {'inchikey':msg}
            except:
                print('no inchikey')
                if '$$$$' in msg:
                    try:
                        obmol = pybel.readstring('sdf', msg)
                        msg = obmol.write('smiles').split('\t')[0]
                        return {'sdf': msg}
                    except:
                        return {'invalid': msg}
            else:
                print('no inchikey, it is id or invalid')
                return {'undefined':msg}



def consumer_config():
    keep_alive = os.getenv('keep_alive')
    servers = os.getenv('bootstrap_servers').split(',')
    from_topic = os.getenv('from_topic').split(',')
    print('keep_alive:', keep_alive)
    print('servers:', servers)
    print('from_topic:', from_topic)
    if keep_alive:
        consumer = KafkaConsumer(bootstrap_servers=servers,
                                 auto_offset_reset='latest',
                                 group_id=from_topic[0],
                                 api_version=(0, 10))
        consumer.subscribe(topics= from_topic)

    else:
        consumer = KafkaConsumer(bootstrap_servers=servers, auto_offset_reset='earliest',
                                 group_id=from_topic[0],
                                 api_version=(0, 10))
        consumer.assign([TopicPartition(from_topic[0], int(os.getenv('partition')))])

    return consumer, keep_alive, servers


