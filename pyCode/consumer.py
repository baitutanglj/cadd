from kafka import KafkaConsumer
from kafka import TopicPartition
import pickle
from rdkit import Chem

# 消费方式1，默认消费所有分区的消息
def consumer_message1(servers,topic):
    # consumer = KafkaConsumer(topic,
    #                          bootstrap_servers=servers,
    #                          )
    consumer = KafkaConsumer(topic, bootstrap_servers=servers,
                             auto_offset_reset='earliest')
    for msg in consumer:
        # print(msg.offset,msg.value.decode("utf-8"))
        print(msg.offset,msg.value.decode())


# 消费方式2, 指定消费分区
def consumer_message2(servers,topic):
    consumer = KafkaConsumer(bootstrap_servers=servers,
                             group_id="kafka-group-id")
    consumer.assign([TopicPartition(topic, 0)])
    for msg in consumer:
        print(msg)

# 消费方式3，手动commit，生产中建议使用这种方式
def consumer_message3(servers,topic):
    consumer = KafkaConsumer(bootstrap_servers=servers,
                             consumer_timeout_ms=1000,
                             group_id="kafka-group-id",
                             enable_auto_commit=False)
    consumer.assign([TopicPartition(topic, 0)])
    for msg in consumer:
        print(msg.offset,msg.value)
        consumer.commit()


def consumer_message(servers, from_topic, partition):
    consumer = KafkaConsumer(bootstrap_servers=servers, api_version = (0,10))
    consumer.assign([TopicPartition(from_topic, partition)])
    res = []
    for msg in consumer:
        msg = msg.value.decode("utf-8")
        print(msg)
        res.append(msg)
    return res

def main():
    bootstrap_servers = '192.168.109.38:9092'
    topic_name = 'result'
    # topic_name = 'getProps'
    partition = 0
    res = consumer_message(bootstrap_servers, topic_name, partition)
    print(res)

if __name__ == "__main__":
    # bootstrap_servers = '192.168.109.38:9092'
    # # topic_name = 'result'
    # topic_name = 'getProps'
    # partition = 0
    # res = consumer_message(bootstrap_servers, topic_name, partition)
    # print(res)
    main()
    print('ok')
