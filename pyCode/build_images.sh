#!/bin/bash
#标准化
#docker build -t cadd/py-standard:latest -f ./images/standard/Dockerfile .
#属性解析
#docker build -t cadd/py-analyze:latest -f ./images/analyze/Dockerfile .
#转换pdbqt
docker build -t cadd/py-conversions:latest -f ./images/conversions/Dockerfile .
#对接
docker build -t cadd/py-docking:latest -f ./images/docking/Dockerfile .
#用户导入
docker build -t cadd/py-collections:latest -f ./images/collections/Dockerfile .
#多样性筛选
#docker build -t cadd/py-diversity:latest -f ./images/diversity/Dockerfile .
#相似的筛选
docker build -t cadd/py-similarity:latest -f ./images/similarity/Dockerfile .
#smarts 查询
docker build -t cadd/py-smarts:latest -f ./images/smarts/Dockerfile .
#转换sdf
docker build -t cadd/py-smiles2sdf:latest -f ./images/smiles2sdf/Dockerfile .
#子结构搜索
docker build -t cadd/py-substructure:latest -f ./images/substructure/Dockerfile .
#格式转换
docker build -t cadd/py-trans-tool:latest -f ./images/trans-tool/Dockerfile .

docker-compose -f ../docker-compose-py.yml up -d