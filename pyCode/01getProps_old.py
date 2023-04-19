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

def shell_cmd(cmd:str,origin=None):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, error = p.communicate()
    out = out.decode(encoding='utf-8').strip()
    # p.wait()
    print("out:" + out)
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
    largest_smi = Chem.MolToSmiles(largest_mol)
    return largest_smi

def IsOnlyElement(smiles):
    onlyElements = ('H', 'C', 'O', 'N', 'S', 'P', 'F', 'Cl', 'Br', 'I', 'B', 'Se')
    mol = Chem.MolFromSmiles(smiles)
    elements = {atom.GetSymbol() for atom in mol.GetAtoms()}
    flag = elements.difference(onlyElements)
    if flag:
        return None
    else:
        return smiles

def standardizer_neutralize(smiles):
    # cmd = "java -cp '{ChemAxonLib}' chemaxon.standardizer.StandardizerCLI \"{smiles}\" -c neutralize"\
    #     .format(ChemAxonLib = ChemAxonLib,smiles = smiles)
    cmd = "java -cp '/mnt/home/linjie/bao/chemaxonJChem/lib/*' chemaxon.standardizer.StandardizerCLI" \
          " \"" + smiles + "\" -c neutralize"
    smiles = Chem.MolToSmiles(Chem.MolFromSmiles(shell_cmd(cmd,smiles)))
    return smiles

def tautomer_enumerator(smiles):
    canonical_taut = s.canonicalize_tautomer(Chem.MolFromSmiles(smiles))
    tauts = list({m for m in s.enumerate_tautomers(canonical_taut)})
    return tauts

def stereo_enumerator(mol):
    # mol = Chem.MolFromSmiles(smi)
    opts = StereoEnumerationOptions(tryEmbedding=False, onlyUnassigned=True,
                                    unique=True, maxIsomers=32, rand=0xf00d)
    stereo_list = [Chem.MolToSmiles(x, isomericSmiles=True, canonical=True)
                   for x in EnumerateStereoisomers(mol, options=opts)]
    return stereo_list

def protonate(smiles,ph:float):
    cmd = "java -cp '/mnt/home/linjie/bao/chemaxonJChem/lib/*' chemaxon.marvin.Calculator "\
           "majorms -H " +str(ph)+" \"" + smiles + "\""
    smiles = shell_cmd(cmd,smiles).rsplit('\t',1)[-1]
    return smiles


def prepare(original_smi: str):
    res_smiles = []
    # 标准化
    mol = standardizer_smiles(original_smi)
    # 获取有机片段
    smiles = desalt(mol)
    #去除含有杂原子的化合物
    smiles = IsOnlyElement(smiles)
    if smiles:
        # 中和电荷
        smiles = standardizer_neutralize(smiles)
        #互变异构枚举
        tauts = tautomer_enumerator(smiles)
        #立体异构枚举
        stereo_list = [smiles]
        for mol in tauts:
            stereo_list += stereo_enumerator(mol)
        stereo_list = sorted(set(stereo_list), key=stereo_list.index)
        #中和PH=7.4
        for smi in stereo_list:
            smi = protonate(smi,7.4)
            smi = Chem.MolToSmiles(Chem.MolFromSmiles(smi),
                                   isomericSmiles=True,canonical=True)
            res_smiles.append(smi)

    return res_smiles

def read_CONDITIONS_FUNC():
    script_path = os.path.dirname(os.path.realpath(__file__))
    with open(script_path + "/conditions.json", 'r', encoding='UTF-8') as f:
        CONDITIONS_FUNC = json.load(f)
    return CONDITIONS_FUNC

def getMolProp(smi: str, parent: str=None,thirdId:str=None,smilesBase:str=None) -> Dict:
    '''
    Get all the descriptions of the mol.
    return: A dict containing descriptions,dict.keys():smi, dict.values():descriptions
    '''
    # print(smi)
    CONDITIONS_FUNC = read_CONDITIONS_FUNC()
    props_dict = {}
    mol = Chem.MolFromSmiles(smi)
    props_dict['thirdId'] = thirdId
    props_dict['smiles'] = smi
    props_dict['smilesBase'] = smilesBase
    # smi = Chem.MolToSmiles(mol, canonical=True)
    for name in CONDITIONS_FUNC.keys():
        value = eval(CONDITIONS_FUNC[name])(mol)
        props_dict[name] = str(round(value,4))
    props_dict['inchi'] = Chem.MolToInchi(mol)
    props_dict['inchikey'] = Chem.MolToInchiKey(mol)
    props_dict['parent'] = parent
    return props_dict

def consumer_message():
    # get kafka config
    #config_dict = get_kafka_config()
    servers = os.getenv('bootstrap_servers').split(',')
    from_topic = os.getenv('from_topic')
    partition = os.getenv('partition')
    to_topic = os.getenv('to_topic')
    print("bootstrap_servers:", servers)
    print("from_topic:", from_topic)
    print("partition:", partition)
    print("to_topic:", to_topic)

    # consumer
    consumer, keep_alive, servers = consumer_config()

    for msgs in consumer:
        msgs = json.loads(msgs.value.decode("utf-8"))
        msgs.update({'receiveTime': int(time.time()*1000)})
        res = []
        for msg in msgs['data']['compoundList']:
            try:
                if msgs['fileType'].upper()=='SMILES':
                    smi = canonical_smiles(msg['smiles'])
                elif msgs['fileType'].upper()=='SDF':
                    obmol = pybel.readstring('sdf', msg['sdf'])
                    smi = obmol.write('smiles').split('\t')[0]

                enumerate_smi = prepare(smi)
                res.append(getMolProp(enumerate_smi[0], parent=None,
                                      thirdId=msg['thirdId'],smilesBase=smi))
                if len(enumerate_smi) > 2:
                    for x in enumerate_smi[1:]:
                        res.append(getMolProp(x, parent=enumerate_smi[0],
                                              thirdId=None,smilesBase=smi))
            except Exception as e:
                # print(smi)
                continue


        # producter result
        msgs['data']['compoundList'] = res
        msgs.update({'partition': partition})
        msgs['sendTime'] = int(time.time()*1000)
        producer_message(servers, to_topic, json.dumps(msgs),True)
        print('ok len(smi)=',len(res))
    # return json.dumps({file_type: res})

if __name__ == "__main__":
    start = time.time()
    # consumer message
    consumer_message()

    print('run time: '+str(time.time()-start))

