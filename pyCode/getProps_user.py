import os
import json
import time
from rdkit import Chem
from rdkit import Chem, DataStructs
import rdkit.Chem.Lipinski as rkcLi
import rdkit.Chem.Descriptors as rkcde
import rdkit.Chem.rdMolDescriptors as rdmde
from rdkit.Chem import MolStandardize
from kafka import KafkaConsumer
from kafka import TopicPartition
from typing import Collection, Dict, List, Optional, Tuple, Union
from openbabel import pybel
import subprocess
from rdkit.Chem.EnumerateStereoisomers import \
    EnumerateStereoisomers,StereoEnumerationOptions
from rdkit.Chem.MolStandardize.standardize import Standardizer
from utils import canonical_smiles,\
    producer_message, get_kafka_config, consumer_config


s = Standardizer(prefer_organic=True, max_tautomers=10)
uc = MolStandardize.charge.Uncharger()

def shell_cmd(cmd:str,origin=None):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, error = p.communicate()
    out = out.decode(encoding='utf-8').strip()
    # p.wait()
    # print("out:" + out)
    if p.returncode != 0 or error:
        # error occurs
        print(error.decode(encoding='utf-8').strip())
        return origin
    else:
        return out


def standardizer_smiles(smiles):
    """Return a standardized mol object given a SMILES string."""

    mol = Chem.MolFromSmiles(smiles)
    mol = s.standardize(mol)
    return mol

def desalt(mol):
    """Return the fragment parent of a given molecule."""
    largest_mol = s.fragment_parent(mol)
    # largest_smi = Chem.MolToSmiles(largest_mol)
    return largest_mol

def IsOnlyElement(mol):
    onlyElements = ('H', 'C', 'O', 'N', 'S', 'P', 'F', 'Cl', 'Br', 'I', 'B', 'Se')
    # mol = Chem.MolFromSmiles(smiles)
    elements = {atom.GetSymbol() for atom in mol.GetAtoms()}
    flag = elements.difference(onlyElements)
    if flag:
        return None
    else:
        return mol

def standardizer_neutralize(mol):
    mol = uc.uncharge(mol)
    uncharge_smi = Chem.MolToSmiles(mol)
    return uncharge_smi



def protonate(smiles):
    cmd = 'obabel -:'+"\"" + smiles + "\""' -osmi -p 7.4'
    smiles = shell_cmd(cmd,smiles).rsplit('\t',1)[-1]
    smiles = Chem.MolToSmiles(Chem.MolFromSmiles(smiles))
    return smiles

def prepare(original_smi: str):
    # res_smiles = []
    # 标准化
    mol = standardizer_smiles(original_smi)
    # 获取有机片段
    mol = desalt(mol)
    #去除含有杂原子的化合物
    mol = IsOnlyElement(mol)
    if mol:
        # 中和电荷
        smiles = standardizer_neutralize(mol)
        # 中和PH=7.4
        smiles = protonate(smiles)
        return smiles

def convert3D(smi: str):
    '''the default method for rdkit to generate 3D conformations is ETKDG'''
    obmol = pybel.readstring('smi',smi)
    obmol.make3D(forcefield="mmff94", steps=500)
    mol2_string = obmol.write(format='mol2')
    obmol = pybel.readstring('mol2', mol2_string)
    pdbqt_string = obmol.write(format='pdbqt')
    return pdbqt_string.replace('UNL','LIG')

def read_CONDITIONS_FUNC():
    script_path = os.path.dirname(os.path.realpath(__file__))
    with open(script_path + "/conditions.json", 'r', encoding='UTF-8') as f:
        CONDITIONS_FUNC = json.load(f)
    return CONDITIONS_FUNC


def getMolProp(smi: str, parent:str=None, thirdId:str=None) -> Dict:
    '''
    Get all the descriptions of the mol.
    return: A dict containing descriptions,dict.keys():smi, dict.values():descriptions
    '''
    # print(smi)
    CONDITIONS_FUNC = read_CONDITIONS_FUNC()
    props_dict = {}
    mol = Chem.MolFromSmiles(smi)
    # props_dict['thirdId'] = thirdId
    props_dict['smiles'] = smi
    # props_dict['parent'] = parent
    # smi = Chem.MolToSmiles(mol, canonical=True)
    for name in CONDITIONS_FUNC.keys():
        value = eval(CONDITIONS_FUNC[name])(mol)
        if isinstance(value, (int, float)):
            props_dict[name] = str(round(value,4))
        elif isinstance(value, str):
            props_dict[name] = value
    props_dict['inchi'] = Chem.MolToInchi(mol)
    props_dict['inchikey'] = Chem.MolToInchiKey(mol)
    props_dict['pdbqt'] = convert3D(smi)

    return props_dict

def consumer_message():
    # get kafka config
    # config_dict = get_kafka_config()
    # servers = config_dict['bootstrap_servers'].split(',')
    # from_topic = config_dict['from_topic']
    # partition = config_dict['partition']
    # to_topic = config_dict['to_topic']


    #consumer
    # consumer = KafkaConsumer(bootstrap_servers=servers, auto_offset_reset='earliest',
    #                          api_version=(0,10))
    # consumer.assign([TopicPartition(from_topic, int(partition))])
    # consumer
    consumer, keep_alive, servers = consumer_config()

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        for compound in msgs['data']['compoundList']:
            smi = ''
            try:
                if msgs['fileType'].upper()=='SMILES':
                    smi = canonical_smiles(compound['smiles'])
                    standard_smi = prepare(smi)
                    if standard_smi:
                        res.append(getMolProp(standard_smi, parent=compound['smiles']))


                elif msgs['fileType'].upper()=='SDF':
                    obmol = pybel.readstring('sdf', compound['sdf'])
                    smi = obmol.write('smiles').split('\t')[0]
                    standard_smi = prepare(smi)
                    if standard_smi:
                        res.append(getMolProp(standard_smi, parent=smi))

            except Exception as e:
                # print(smi)
                continue

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

    print('run time: '+str(time.time()-start))