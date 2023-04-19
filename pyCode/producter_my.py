from kafka import KafkaProducer
import json

# bootstrap_servers = '192.168.109.38:9092'

def producer_message(servers, topic_name,msg,encoder=False):
    producer = KafkaProducer(bootstrap_servers=servers,max_request_size=1073741824)
    if encoder:
        producer.send(topic=topic_name, value=bytes(msg.encode("utf-8")))
        print(bytes(msg.encode("utf-8")))
    else:
        producer.send(topic=topic_name, value=msg)
        print(msg)


def read_receptor(file_path):
    with open(file_path,'r') as f:
        receptor = f.read()
    return receptor

if __name__ == "__main__":
    bootstrap_servers = '192.168.109.38:9092'
    topic_name = 'getProps'
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #         'data': {"compoundList": [{'thirdId': 'mcule001', 'smiles': 'C(=C(\\C=C\\CCCCC)/C)/C(=O)OCC'},
    #                                 {'thirdId': 'mcule001', 'smiles': '[K+]'},
    #                                   {'thirdId': 'mcule005', 'smiles':'[Na]OC(=O)c1ccc(C[S+2]([O-])([O-]))cc1'},
    #                                   {'thirdId': 'mcule006', 'smiles': 'FC=CC(F)Cl'},
    #                                  {'thirdId': 'mcule003', 'smiles': 'CCCCCCCCO'},
    #                                  {'thirdId': 'mcule004', 'smiles': 'C1=CC=NC=C1'},
    #                                   {'thirdId': 'mcule007', 'smiles': 'N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O'},
    #                                   {'thirdId': 'mcule008', 'smiles': 'O=C(NCc1ccc(CNC(=O)c2ccc(NC(=O)N3Cc4ccc(F)cc4C3)cc2)cc1)c1ccccc1'},
    #                                   {'thirdId': 'mcule009', 'smiles': 'CNC(=S)NS(=O)(=O)c1cc(CCNC(=O)c2cc(Cl)ccc2OC)ccc1OCCOC'},
    #                                   {'thirdId': 'mcule0010', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'}],
    #                     "dbType": "MCULE"}
    #         }
    # msg = {"data":{"compoundList":[{"smiles":"N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O","thirdId":"MCULE-3188569799"},{"smiles":"N1N=CNN=1","thirdId":"MCULE-5933021454"},{"smiles":"C1(=NNC=N1)N(=O)=O","thirdId":"MCULE-3707390029"},{"smiles":"N(=O)(=O)C1=CC2=CC=CN=C2C(O)=C1","thirdId":"MCULE-2532563996"},{"smiles":"N(=O)(=O)C1C=CC(=C(O)C=1)Cl","thirdId":"MCULE-6058343212"},{"smiles":"C(C1C=CC=CC=1)(=O)C(CCC#N)(CCC#N)CCC#N","thirdId":"MCULE-6510012271"},{"smiles":"C12/C(/CCCC1=NON=2)=N/O","thirdId":"MCULE-9050945793"},{"smiles":"C1(NN=C(N)N=1)C1NN=C(N)N=1","thirdId":"MCULE-9882542002"},{"smiles":"C1(=C(C)NC(NC1=O)=O)S(Cl)(=O)=O","thirdId":"MCULE-8802195640"},{"smiles":"N1N=CN(CC2C=CC(=C(OC)C=2)O)N=1","thirdId":"MCULE-8906278187"},{"smiles":"C1(=C(C)N=C(C(C#N)=C1COC)Cl)N(=O)=O","thirdId":"MCULE-6278883029"},{"smiles":"C1(C#N)C2CCC(CC=2SC=1N)C","thirdId":"MCULE-5358393564"},{"smiles":"C1(C#N)C2CCC(CC=2SC=1N)C(C)(C)C","thirdId":"MCULE-1711223729"},{"smiles":"C1(C#N)C2CCN(CC=2SC=1N)C(C)C","thirdId":"MCULE-1403709332"},{"smiles":"C1(C#N)C2CCN(CC=2SC=1N)CC1C=CC=CC=1","thirdId":"MCULE-3825617563"},{"smiles":"C1(C(OCC)=O)C2CCN(CC=2SC=1N)CC1C=CC=CC=1","thirdId":"MCULE-4130902600"},{"smiles":"C1(C)(C)C(CC=O)CC=C1C","thirdId":"MCULE-1670316367"},{"smiles":"O=C(C)CC1CCCCCCCCCCC1","thirdId":"MCULE-3880635347"},{"smiles":"C12=CC(CCC(C)=O)=CC=C1OCO2","thirdId":"MCULE-3259096651"},{"smiles":"C1(N)=CC(Br)=CC2=CC=CN=C12","thirdId":"MCULE-7955984831"},{"smiles":"C1(C#N)C2CCCCC=2SC=1N","thirdId":"MCULE-3255541087"},{"smiles":"C1(N)=NON=C1N","thirdId":"MCULE-9556144390"},{"smiles":"C1(=NON=C1N)/C(/N)=N\\O","thirdId":"MCULE-2773160019"},{"smiles":"C1(N)=NC2=C(C=CC=C2)N1CCN1CCOCC1","thirdId":"MCULE-9652595625"},{"smiles":"C1(N)=NC2=C(C=CC=C2)N1CCN(CC)CC","thirdId":"MCULE-5001472841"},{"smiles":"C1(C#N)C(C2C=CC(=CC=2)N(=O)=O)C2C=CC(=CC=2OC=1N)N","thirdId":"MCULE-5953242421"},{"smiles":"C1(C#N)C2CCCC=2SC=1N","thirdId":"MCULE-4060235602"},{"smiles":"N1=C(N)SC(C)=C1C1=CC=C(C=C1)OC","thirdId":"MCULE-3475217874"},{"smiles":"N1C(C2=CC=C(C=C2)C)=CSC=1N","thirdId":"MCULE-7081636496"},{"smiles":"C1(N(=O)=O)N=NN(CC(C)=O)N=1","thirdId":"MCULE-5436124308"},{"smiles":"C1(=CN(C=N1)C)N(=O)=O","thirdId":"MCULE-4199906753"},{"smiles":"C1(=NON=C1CC(O)=O)N(=O)=O","thirdId":"MCULE-1466194642"},{"smiles":"C1(/N=N\\C2=NON=C2CC(O)=O)=NON=C1CC(O)=O","thirdId":"MCULE-3722008595"},{"smiles":"C12C(=O)C=CC(CO1)O2","thirdId":"MCULE-4366139156"},{"smiles":"C1(=O)C(=O)C2=C(C=CC(F)=C2)N1","thirdId":"MCULE-1624734258"},{"smiles":"N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O","thirdId":"MCULE-9646919190"},{"smiles":"N1C2C=C(C=CC=2NC=1C1C=CN=CC=1)N(=O)=O","thirdId":"MCULE-7050646249"},{"smiles":"C1(C=NC(=CC=1C1C=CC=CC=1)C(O)=O)C(O)=O","thirdId":"MCULE-8442566333"},{"smiles":"C1(C(C)=O)C(C2C=CC=CC=2)NC(NC=1C)=O","thirdId":"MCULE-4930571630"},{"smiles":"C1(C(OCC)=O)CC(C(OCC)=O)=C(NC=1C)C","thirdId":"MCULE-9011758012"},{"smiles":"C1(C(OCC)=O)=C(C)NC(=C(C(OCC)=O)C1C1C=CC=CC=1)C","thirdId":"MCULE-8185586246"},{"smiles":"C12C(=O)C3=C(C=CC=C3)C=1N=C1C=CC=CC1=N2","thirdId":"MCULE-2508475811"},{"smiles":"C1(C(O)=O)C(C1)CCCCCCCC","thirdId":"MCULE-6572964179"},{"smiles":"C1(=CC=CO1)/C=C/C(OCC)=O","thirdId":"MCULE-7171775414"},{"smiles":"C(/C(C1C=CC=CC=1)=O)=C\\C1=CC=CO1","thirdId":"MCULE-2971755050"},{"smiles":"C1(C(=O)/C=C/C2=CC=CO2)CC1","thirdId":"MCULE-6287018930"},{"smiles":"C(F)(F)(F)C(O)CC(OCC)=O","thirdId":"MCULE-2073476806"},{"smiles":"C1(N)C(O)=CC=C(Br)C=1","thirdId":"MCULE-6355681750"},{"smiles":"C1=CC=C2C(=C1)C(=O)N([C@H]1CC(=O)[C@@H]3OCC1O3)C2=O","thirdId":"MCULE-4376362632"},{"smiles":"C1(=O)OC(C(=C)O1)(C)C","thirdId":"MCULE-6317618259"},{"smiles":"C(C(C)(C)N)#C","thirdId":"MCULE-4795277170"},{"smiles":"C1(C(O)=O)SC2=C(C(C3=CC=CC=C3)=CC(C3C=CC=CC=3)=N2)C=1N","thirdId":"MCULE-6155536617"},{"smiles":"C(C)(OCCCC(C)=O)=O","thirdId":"MCULE-1256846745"},{"smiles":"C(OCCCC(C)=O)=O","thirdId":"MCULE-7473574321"},{"smiles":"C(/C(=O)C1CC1)=C(\\C)/C1CC1","thirdId":"MCULE-6700194065"},{"smiles":"N1=CC=CC2C=C(Br)C=CC=21","thirdId":"MCULE-1986381749"},{"smiles":"S(N)(=O)(=O)C1=C(Cl)C=C(C(C(O)=O)=C1)NCC1=CC=CO1","thirdId":"MCULE-8037874224"},{"smiles":"C(NCCCC(O)=O)(=O)C1C=CC=NC=1","thirdId":"MCULE-5107124225"},{"smiles":"S(Cl)(=O)(=O)C1=C(Cl)C=C(C(C(O)=O)=C1)Cl","thirdId":"MCULE-9643596736"},{"smiles":"C1(C(O)=O)=NON=C1N","thirdId":"MCULE-6850187023"},{"smiles":"C1(C#N)=NON=C1N","thirdId":"MCULE-3473741896"},{"smiles":"C1(C(N)=O)=NON=C1N","thirdId":"MCULE-7036883680"},{"smiles":"N(=O)(=O)C1=CC=C(C=C1)NC(=O)C(CC1=CC=CC=C1)NC(C1=CC=CC=C1)=O","thirdId":"MCULE-5302082216"},{"smiles":"N(=O)(=O)C1C=CC(=CC=1)/N=N/N(O)C1C=CC=CC=1","thirdId":"MCULE-5865556861"},{"smiles":"C(C1(CCCCC1)O)#CBr","thirdId":"MCULE-3496500170"},{"smiles":"C1(=O)CC(C)(C)CC(=O)C1Br","thirdId":"MCULE-1266823579"},{"smiles":"C(/C(OCC)=O)(\\C(OCC)=O)=C/C1=CC=CO1","thirdId":"MCULE-2019901627"},{"smiles":"S1(=O)(=O)C2C=C(C=C(N(=O)=O)C=2NC2C(=CC(=CC1=2)N(=O)=O)N(=O)=O)N(=O)=O","thirdId":"MCULE-7477471152"},{"smiles":"C(C(C)(CCCC(C)(C)OC)O)#C","thirdId":"MCULE-6342285799"},{"smiles":"C(C)(C)(CCCC(C)CCO)OC","thirdId":"MCULE-5794597129"},{"smiles":"C(=O)(C1C=CC=C(N)C=1)NC1=CC=CC(Cl)=C1","thirdId":"MCULE-3252039351"},{"smiles":"N1(C2=CC=C(C=C2)C(O)=O)C(=O)C=CC1=O","thirdId":"MCULE-8024146135"},{"smiles":"N1(C2C=CC=CC=2OC)C(=O)C=CC1=O","thirdId":"MCULE-3304216938"},{"smiles":"N(=O)(=O)C1C(N)=CC=C(C(O)=O)C=1","thirdId":"MCULE-7368561992"},{"smiles":"N1(C2C=CC=CC=2Cl)C(=O)C=CC1=O","thirdId":"MCULE-4973809689"},{"smiles":"C1(C#N)C2CCN(CC=2SC=1N)C","thirdId":"MCULE-6835153403"},{"smiles":"C1(C(C)=O)C(=O)CC(CC1=O)(C)C","thirdId":"MCULE-6520407789"},{"smiles":"C1(C=CC=CC=1OCCCC)N","thirdId":"MCULE-8335122948"},{"smiles":"C1C(=CC=CC=1N)OCCCCCC","thirdId":"MCULE-6880993556"},{"smiles":"N1(C(=O)C=CC1=O)C1=CC=C(C=C1C)C","thirdId":"MCULE-9238775867"},{"smiles":"N(C1=CC=C(C=C1)OC)C(C)C1=CC=CC=C1","thirdId":"MCULE-9207731480"},{"smiles":"N(=O)(=O)C1=CC(N)=CC=C1C(O)=O","thirdId":"MCULE-4820209497"},{"smiles":"C(CCC)(=O)C1C=CC(=CC=1)O","thirdId":"MCULE-9021396113"},{"smiles":"O1CCCC1CCC(C)O","thirdId":"MCULE-9471233943"},{"smiles":"O1CCCC1CC(CC)CCC(C)O","thirdId":"MCULE-3866594493"},{"smiles":"C12OCOCC1CC1C=CC=CC2=1","thirdId":"MCULE-3843167560"},{"smiles":"C1(=CC=C2C3C(=CC=CC1=3)CC2)C(C1=CC=CC=C1)=O","thirdId":"MCULE-5174431551"},{"smiles":"C(=O)(NC1=CC=CC=C1)CC(=O)NC1C=CC=CC=1","thirdId":"MCULE-1833737994"},{"smiles":"N#CCC1=CC=C(C(OCC)=C1)OCC","thirdId":"MCULE-5069269926"},{"smiles":"C1(=CC=CC=C1OCCC)C=O","thirdId":"MCULE-7807414930"},{"smiles":"C1(O)=C(OC)C=C(C=C1Cl)C=O","thirdId":"MCULE-2096501908"},{"smiles":"C1(C=O)=CC2OCOC=2C=C1Br","thirdId":"MCULE-4811593775"},{"smiles":"C1(=CC=CO1)C1CC(=O)CC(=O)C1","thirdId":"MCULE-6518617420"},{"smiles":"C1(C)=CC(=CC=C1N)CC1=CC=C(C(C)=C1)N","thirdId":"MCULE-5798656388"},{"smiles":"C1(C=O)=CC(=CC=C1O)/N=N/C1C=CC=CC=1","thirdId":"MCULE-4512707685"},{"smiles":"N(=O)(=O)C1C=CC2=C(C=1)N=C(N2)C","thirdId":"MCULE-7536953858"},{"smiles":"N1(C2C=CC=CC=2)CC(C(=O)N1)C","thirdId":"MCULE-6879663923"},{"smiles":"C12C(CCCC=1C1=C(C=CC=C1)N2)=O","thirdId":"MCULE-8165438115"},{"smiles":"N(C1C=CC=CC=1)C1=CC=CC(N)=C1","thirdId":"MCULE-3184176554"},{"smiles":"C1(N)N(C2C=CC=CC=2)N=CC=1C(OCC)=O","thirdId":"MCULE-7310377477"}],"dbType":"MCULE","conversionJobId":"ee50f04c07844af3942485d0278ba938"},"fileType":"SMILES","msgId":"7c75ff1a81f5428ca494f40d8449d8ff","sendTime":1625126176103}
    # msg = {"compoundList": ["CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1",
    #                   "O=C(NCc1ccc(CNC(=O)c2ccc(NC(=O)N3Cc4ccc(F)cc4C3)cc2)cc1)c1ccccc1",
    #                   "CN(C)C(=O)Cc1nn(-c2ccc(OCCF)cc2)c(=O)c2c1c1ccc(Cl)cc1n2C",
    #                   "CNC(=S)NS(=O)(=O)c1cc(CCNC(=O)c2cc(Cl)ccc2OC)ccc1OCCOC",
    #                   "O=C(c1nn(C2CN3CCC2CC3)c2c1CS(=O)(=O)c1ccccc1-2)N1CCOCC1",
    #                   "Cc1cccc(NC(=O)CSc2nc(=N)cc(N)[nH]2)c1", "CCC(C)Sc1cc(C(F)(F)F)c(C(=O)Nc2ccc(Cl)cc2)cn1",
    #                   "CN(CCc1ccccc1)C(=O)CN1C(=O)OCC1c1ccccc1", "CCCN(CC1CC1)c1nc(C)nc(C(=O)c2c(OC)cc(OC)cc2OC)c1C",
    #                   "Oc1ccccc1Cn1c2c(c3ccccc31)CCN(Cc1ccccc1)C2=S", "Nc1cccc(N)c1Oc1ccccc1CC(=O)O",
    #                   "COc1ccc(-c2cnc3c(N4CCOCC4)snc3c2)cc1OC", "CN(Cc1ccccc1)C(=O)c1ccc(C2=CC3(CCNCC3)Oc3ccccc32)cc1",
    #                   "Cc1ccc(Sc2cc(=N)[nH]c(=N)[nH]2)cc1", "Cc1cccc(CC(O)C=CC2CCC(=O)N2CCSCCCC(=O)O)c1",
    #                   "COc1ccccc1NC(=O)N1c2ccccc2NC(=O)CC1C",
    #                   "COc1cc2[nH]c(N3CCCN(C4CCCCC4)CC3)nc(=Nc3ccc(Cl)cc3)c2cc1OC",
    #                   "COc1cc(C#CC2CCCC2)ccc1-n1c(=O)ccc2cc(S(=O)(=O)N=c3cccn[nH]3)ccc21",
    #                   "CC(C)(C)c1cc(=NC(=O)C2(C)CCN2C(=O)C2(c3ccccc3)CCCC2)[nH]o1",
    #                   "O=S(=O)(NCC(c1ccc2c(c1)OCO2)N1CCOCC1)c1ccc(F)c(C(F)(F)F)c1",
    #                   "O=C([O-])c1ccc(C[SH](=O)=O)cc1.[Na+]"], "dbType": "MCULE"}

    ###01getProps sdf
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'sdf','conversionJobld':'asdfg',
    #         'data': {"compoundList": [{'thirdId': 'mcule001', 'sdf': "\n  mcule   16062110052D\n\n  5  5  0  0  0  0  0  0  0  0999 V2000\n    1.2352    0.6294    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.9803   -0.1553    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.1553   -0.1553    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n   -0.0997    0.6294    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.5678    1.1143    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n  1  2  1  0  0  0  0\n  2  3  1  0  0  0  0\n  3  4  1  0  0  0  0\n  4  5  1  0  0  0  0\n  1  5  1  0  0  0  0\nM  END\n>  <mcule ID>\nMCULE-8453099153\n$$$$"},
    #                                   {'thirdId': 'mcule005', 'sdf':"\n  mcule   16062110052D\n\n  6  6  0  0  0  0  0  0  0  0999 V2000\n    1.4289    0.8250    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    1.4289   -0.0000    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.7145   -0.4125    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.0000   -0.0000    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.0000    0.8250    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.7145    1.2375    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n  1  2  2  0  0  0  0\n  2  3  1  0  0  0  0\n  3  4  2  0  0  0  0\n  4  5  1  0  0  0  0\n  5  6  2  0  0  0  0\n  1  6  1  0  0  0  0\nM  END\n>  <mcule ID>\nMCULE-4899719484\n$$$$"},
    #                                  ],
    #                     "dbType": "MCULE"}
    #         }

    ##########standardized.py###############
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #         'data': {"compoundList": [{'thirdId': 'mcule001', 'smiles': 'C(=C(\\C=C\\CCCCC)/C)/C(=O)OCC'},
    #                                 {'thirdId': 'mcule001', 'smiles': '[K+]'},
    #                                   {'thirdId': 'mcule005', 'smiles':'[Na]OC(=O)c1ccc(C[S+2]([O-])([O-]))cc1'},
    #                                   {'thirdId': 'mcule006', 'smiles': 'FC=CC(F)Cl'},
    #                                  {'thirdId': 'mcule003', 'smiles': 'CCCCCCCCO'},
    #                                  {'thirdId': 'mcule004', 'smiles': 'C1=CC=NC=C1'},
    #                                   {'thirdId': 'mcule007', 'smiles': 'N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O'},
    #                                   {'thirdId': 'mcule008', 'smiles': 'O=C(NCc1ccc(CNC(=O)c2ccc(NC(=O)N3Cc4ccc(F)cc4C3)cc2)cc1)c1ccccc1'},
    #                                   {'thirdId': 'mcule009', 'smiles': 'CNC(=S)NS(=O)(=O)c1cc(CCNC(=O)c2cc(Cl)ccc2OC)ccc1OCCOC'},
    #                                   {'thirdId': 'mcule0010', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'}],
    #                     "dbType": "MCULE"}
    #         }
    ###########enumerate.py###################
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #         'data': {"compoundList": [{'thirdId': 'mcule001', 'smiles': 'C(=C(\\C=C\\CCCCC)/C)/C(=O)OCC'},
    #                                   {'thirdId': 'mcule006', 'smiles': 'FC=CC(F)Cl'},
    #                                  {'thirdId': 'mcule003', 'smiles': 'CCCCCCCCO'},
    #                                  {'thirdId': 'mcule004', 'smiles': 'C1=CC=NC=C1'},
    #                                   {'thirdId': 'mcule007', 'smiles': 'N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O'},
    #                                   {'thirdId': 'mcule008', 'smiles': 'O=C(NCc1ccc(CNC(=O)c2ccc(NC(=O)N3Cc4ccc(F)cc4C3)cc2)cc1)c1ccccc1'},
    #                                   {'thirdId': 'mcule009', 'smiles': 'CNC(=S)NS(=O)(=O)c1cc(CCNC(=O)c2cc(Cl)ccc2OC)ccc1OCCOC'},
    #                                   {'thirdId': 'mcule0010', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'}],
    #                     "dbType": "MCULE"}
    #         }

    ############getProps_user.py###################
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #         'data': {"compoundList": [{'thirdId': 'mcule001', 'smiles': 'C(=C(\\C=C\\CCCCC)/C)/C(=O)OCC'},
    #                                 {'thirdId': 'mcule001', 'smiles': '[K+]'},
    #                                   {'thirdId': 'mcule005', 'smiles':'[Na]OC(=O)c1ccc(C[S+2]([O-])([O-]))cc1'},
    #                                   {'thirdId': 'mcule006', 'smiles': 'FC=CC(F)Cl'},
    #                                  {'thirdId': 'mcule003', 'smiles': 'CCCCCCCCO'},
    #                                  {'thirdId': 'mcule004', 'smiles': 'C1=CC=NC=C1'},
    #                                   {'thirdId': 'mcule007', 'smiles': 'N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O'},
    #                                   {'thirdId': 'mcule008', 'smiles': 'O=C(NCc1ccc(CNC(=O)c2ccc(NC(=O)N3Cc4ccc(F)cc4C3)cc2)cc1)c1ccccc1'},
    #                                   {'thirdId': 'mcule009', 'smiles': 'CNC(=S)NS(=O)(=O)c1cc(CCNC(=O)c2cc(Cl)ccc2OC)ccc1OCCOC'},
    #                                   {'thirdId': 'mcule0010', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'}],
    #                     "dbType": "MCULE"}
    #         }

    #
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'sdf','conversionJobld':'asdfg',
    #         'data': {"compoundList": [{'thirdId': 'mcule001', 'sdf': "\n  mcule   16062110052D\n\n  5  5  0  0  0  0  0  0  0  0999 V2000\n    1.2352    0.6294    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.9803   -0.1553    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.1553   -0.1553    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n   -0.0997    0.6294    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.5678    1.1143    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n  1  2  1  0  0  0  0\n  2  3  1  0  0  0  0\n  3  4  1  0  0  0  0\n  4  5  1  0  0  0  0\n  1  5  1  0  0  0  0\nM  END\n>  <mcule ID>\nMCULE-8453099153\n$$$$"},
    #                                   {'thirdId': 'mcule005', 'sdf':"\n  mcule   16062110052D\n\n  6  6  0  0  0  0  0  0  0  0999 V2000\n    1.4289    0.8250    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    1.4289   -0.0000    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.7145   -0.4125    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.0000   -0.0000    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.0000    0.8250    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n    0.7145    1.2375    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0\n  1  2  2  0  0  0  0\n  2  3  1  0  0  0  0\n  3  4  2  0  0  0  0\n  4  5  1  0  0  0  0\n  5  6  2  0  0  0  0\n  1  6  1  0  0  0  0\nM  END\n>  <mcule ID>\nMCULE-4899719484\n$$$$"},
    #                                  ],
    #                     "dbType": "MCULE"}
    #         }

    #####exact search####
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #        'data':{'compoundList':['O=S(=O)(NCC(c1ccc2c(c1)OCO2)N1CCOCC1)c1ccc(F)c(C(F)(F)F)c1',
    #               'CC(C)(C)c1cc(=NC(=O)C2(C)CCN2C(=O)C2(c3ccccc3)CCCC2)[nH]o1'],
    #                'best_match':False}}
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #        'data': {'compoundList': [
    #            'InChI=1S/C20H20F4N2O5S/c21-16-3-2-14(10-15(16)20(22,23)24)32(27,28)25-11-17(26-5-7-29-8-6-26)13-1-4-18-19(9-13)31-12-30-18/h1-4,9-10,17,25H,5-8,11-12H2/t17-/m1/s1',
    #            'O=S(=O)(NCC(c1ccc2c(c1)OCO2)N1CCOCC1)c1ccc(F)c(C(F)(F)F)c1'],
    #                 'best_match': 'False'}}

    ####similarity search####
    # msg = {'msgId': 'msg001', 'sendTime': 1624969133, 'fileType': 'smiles', 'conversionJobld': 'asdfg',
    #        'data':{'queries':['O=S(=O)(NCC(c1ccc2c(c1)OCO2)N1CCOCC1)c1ccc(F)c(C(F)(F)F)c1'],
    #        'compoundList':[{'caddId':'cadd001','smiles':'O=C(c1nn(C2CN3CCC2CC3)c2c1CS(=O)(=O)c1ccccc1-2)N1CCOCC1'},
    #                        {'caddId':'cadd002','smiles':'CC(C)(C)c1cc(=NC(=O)C2(C)CCN2C(=O)C2(c3ccccc3)CCCC2)[nH]o1'},
    #                        {'caddId':'cadd003','smiles':'CN(CCc1ccccc1)C(=O)CN1C(=O)OCC1c1ccccc1'}],
    #        'scoring_method':None,
    #        'descriptor':'ECFPs',
    #        'similarity_index':'Tanimoto'}}
    ###multiple similarity search
    # msg = {'msgId': 'msg001', 'sendTime': 1624969133, 'fileType': 'smiles', 'conversionJobld': 'asdfg',
    #        'data':{'queries':['O=S(=O)(NCC(c1ccc2c(c1)OCO2)N1CCOCC1)c1ccc(F)c(C(F)(F)F)c1','CN(CCc1ccccc1)C(=O)CN1C(=O)OCC1c1ccccc1'],
    #        'compoundList':[{'caddId':'cadd001','smiles':'O=C(c1nn(C2CN3CCC2CC3)c2c1CS(=O)(=O)c1ccccc1-2)N1CCOCC1'},
    #                        {'caddId':'cadd002','smiles':'CC(C)(C)c1cc(=NC(=O)C2(C)CCN2C(=O)C2(c3ccccc3)CCCC2)[nH]o1'},
    #                        {'caddId':'cadd003','smiles':'CN(CCc1ccccc1)C(=O)CN1C(=O)OCC1c1ccccc1'}],
    #        'scoring_method':'Best',
    #        'descriptor':'ECFPs',
    #        'similarity_index':'Tanimoto'}}


    ####substructure_search.py#####
    # msg = {'msgId': 'msg001', 'sendTime': 1624969133, 'fileType': 'smiles', 'conversionJobld': 'asdfg',
    #        'data':{'queries':'O=C(c1nn(C2CN3CCC2CC3)c2c1CS(=O)(=O)c1ccccc1-2)N1CCOCC1',
    #        'compoundList':[{'caddId':'cadd001','smiles':'O=C(c1nn(C2CN3CCC2CC3)c2c1CS(=O)(=O)c1ccccc1-2)N1CCOCC1'},
    #                        {'caddId':'cadd002','smiles':'CC(C)(C)c1cc(=NC(=O)C2(C)CCN2C(=O)C2(c3ccccc3)CCCC2)[nH]o1'},
    #                        {'caddId':'cadd003','smiles':'CN(CCc1ccccc1)C(=O)CN1C(=O)OCC1c1ccccc1'}],
    #         }}

    #####smarts_query_filter#####
    # msg = {'msgId': 'msg001', 'sendTime': 1624969133, 'fileType': 'smiles', 'conversionJobld': 'asdfg',
    #        'data':{'queries': ['[#6]-[#6]-[#8]', '[#6]-[#8]-[#6]'],
    #           'compoundList':[{'caddId':'cadd001','smiles':'O=C(c1nn(C2CN3CCC2CC3)c2c1CS(=O)(=O)c1ccccc1-2)N1CCOCC1'},
    #                           {'caddId':'cadd002','smiles':'CC(C)(C)c1cc(=NC(=O)C2(C)CCN2C(=O)C2(c3ccccc3)CCCC2)[nH]o1'},
    #                           {'caddId':'cadd003','smiles':'CN(CCc1ccccc1)C(=O)CN1C(=O)OCC1c1ccccc1'}],
    #         'mode': 'any',
    #         'if_include': 'exclude'}
    # }

    ####docking####
    # with open('/mnt/home/linjie/projects/Molecular_Search/docking/data/AA/AABAAC/00000/Z1252751589_1_T1.pdbqt',
    #           'r')as f:
    #     pdbqt = f.read()
    # receptor = read_receptor('/mnt/home/linjie/projects/Molecular_Search/docking/data/rep.pdbqt')
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #        'data':{'receptor': receptor, 'receptorSuffix': 'pdbqt'
    #         'dockingCompoundList': [{'caddId':'cadd001',
    #             'pdbqt':pdbqt}, {'caddId':'cadd002','pdbqt':pdbqt}],
    #         'centerX': '-6.9465', 'centerY': '-18.7935', 'centerZ': '19.9355',
    #         'areaX': '20.0', 'areaY': '20.0', 'areaZ': '20.0',
    #         'exhaustiveness': '2',
    #         'notClick': False}}
    #######generate_pdbqt.py#########
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #         'data': {"compoundList": [{'caddId': 'mcule006', 'smiles': 'FC=CC(F)Cl'},
    #                                  {'caddId': 'mcule002', 'smiles': 'O1CCCC1'},
    #                                  {'caddId': 'mcule003', 'smiles': 'CCCCCCCCO'},
    #                                  {'caddId': 'mcule004', 'smiles': 'C1=CC=NC=C1'},
    #                                   {'caddId': 'mcule007', 'smiles': 'N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O'},
    #                                   {'caddId': 'mcule008', 'smiles': 'O=C(NCc1ccc(CNC(=O)c2ccc(NC(=O)N3Cc4ccc(F)cc4C3)cc2)cc1)c1ccccc1'},
    #                                   {'caddId': 'mcule009', 'smiles': 'CNC(=S)NS(=O)(=O)c1cc(CCNC(=O)c2cc(Cl)ccc2OC)ccc1OCCOC'},
    #                                   {'caddId': 'mcule0010', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'},
    #                                   {'caddId': 'mcule0010', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'},
    #                                   {'caddId': 'mcule0010', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'}],
    #                     "dbType": "MCULE"}
    #         }


    ########diversity_select_gpu#################
    # import json
    # with open('/home/linjie/Downloads/gputest.json') as f:
    #     data = json.load(f)['data']
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #         'data': data
    #         }
    msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
            'data': {"compoundList": [{'caddId': 'mcule006', 'smiles': 'FC=CC(F)Cl'},
                                         {'caddId': 'mcule002', 'smiles': 'O1CCCC1'},
                                         {'caddId': 'mcule003', 'smiles': 'CCCCCCCCO'},
                                         {'caddId': 'mcule004', 'smiles': 'C1=CC=NC=C1'},
                                          {'caddId': 'mcule007', 'smiles': 'N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O'},
                                          {'caddId': 'mcule008', 'smiles': 'O=C(NCc1ccc(CNC(=O)c2ccc(NC(=O)N3Cc4ccc(F)cc4C3)cc2)cc1)c1ccccc1'},
                                          {'caddId': 'mcule009', 'smiles': 'CNC(=S)NS(=O)(=O)c1cc(CCNC(=O)c2cc(Cl)ccc2OC)ccc1OCCOC'},
                                          {'caddId': 'mcule005', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'},
                                          {'caddId': 'mcule001', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'},
                                          {'caddId': 'mcule000', 'smiles': 'CCCC(=O)Oc1c(O)c(-c2ccc(O)cc2)c(OC(=O)CCc2ccccc2)c(O)c1-c1ccc(O)cc1'}]*10,
                     'totalMsgCount':1,
                     'descriptor':'ECFPs',
                     "sample_num":17,
                     "cut_off":0.8,
                     "dbType": "MCULE"}
            }


    ##################smiles2sdf.py###################
    # msg = {'msgId':'msg001', 'sendTime':1624969133, 'fileType':'smiles','conversionJobld':'asdfg',
    #         'data': {"compoundList": [{'caddId': 'mcule006', 'smiles': 'FC=CC(F)Cl'},
    #                                      {'caddId': 'mcule002', 'smiles': 'O1CCCC1'},
    #                                      {'caddId': 'mcule003', 'smiles': 'CCCCCCCCO'},
    #                                      {'caddId': 'mcule004', 'smiles': 'C1=CC=NC=C1'},
    #                                       {'caddId': 'mcule007', 'smiles': 'N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O'},
    #                                       {'caddId': 'mcule008', 'smiles': 'O=C(NCc1ccc(CNC(=O)c2ccc(NC(=O)N3Cc4ccc(F)cc4C3)cc2)cc1)c1ccccc1'},
    #                                       {'caddId': 'mcule009', 'smiles': 'CNC(=S)NS(=O)(=O)c1cc(CCNC(=O)c2cc(Cl)ccc2OC)ccc1OCCOC'}],
    #                  "dbType": "MCULE"}
    #         }

    ################## convert_input.py################
    # msg = {'msgId':'msg001', 'sendTime':1624969133,'conversionJobld':'asdfg',
    #         'data': {"compoundList": ['FC=CC(F)Cl',
    #                              'O1CCCC1',
    #                              'CCCCCCCCO',
    #                              'C1=CC=NC=C1',
    #                              'N12C3C=CC=CC=3C=CC1C1C(C3COC(O3)C1=O)C2C(C)=O',
    #                              'O=C(NCc1ccc(CNC(=O)c2ccc(NC(=O)N3Cc4ccc(F)cc4C3)cc2)cc1)c1ccccc1',
    #                               'CNC(=S)NS(=O)(=O)c1cc(CCNC(=O)c2cc(Cl)ccc2OC)ccc1OCCOC'],
    #                  "operate": 'smilesToMol2',
    #                  "dbType": "MCULE"}
    #         }

    msg = json.dumps(msg)
    producer_message(bootstrap_servers, topic_name, msg, encoder=True)

