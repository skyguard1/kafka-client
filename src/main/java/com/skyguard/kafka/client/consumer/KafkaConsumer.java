package com.skyguard.kafka.client.consumer;


import com.google.common.collect.Lists;
import com.skyguard.kafka.client.annotation.KafkaClient;
import com.skyguard.kafka.client.common.ClientType;
import com.skyguard.kafka.client.config.KafkaConfig;
import com.skyguard.kafka.client.util.ClassUtil;
import com.skyguard.kafka.client.util.PropertyUtil;
import kafka.utils.ZKConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaConsumer<K,V> {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final org.apache.kafka.clients.consumer.KafkaConsumer<K,V> consumer;

    private static final String PACKAGE_NAME = PropertyUtil.getValue(KafkaConfig.PACKAGE_NAME);

    private static String topic = "";

    public KafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", PropertyUtil.getValue(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));      //配置kafka broker list的地址
        props.put("zookeeper.connect", PropertyUtil.getValue(KafkaConfig.ZK_CONNECT_URL));
        props.put("group.id", PropertyUtil.getValue(ConsumerConfig.GROUP_ID_CONFIG));
        props.put("key.deserializer",PropertyUtil.getValue(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        props.put("value.deserializer",PropertyUtil.getValue(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        props.put("zookeeper.session.timeout.ms", PropertyUtil.getValue(KafkaConfig.ZK_SESSION_TIMEOUT));
        props.put("zookeeper.sync.time.ms", PropertyUtil.getValue(KafkaConfig.ZK_SYNC_TIMEOUT));
        props.put("auto.commit.interval.ms", PropertyUtil.getValue(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));


        //配置consumer的值
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);

        if(isKafkaConsumer()) {
            consumer.subscribe(Lists.newArrayList(topic));
        }
    }

    private boolean isKafkaConsumer(){

        Set<Class<?>> classes = ClassUtil.getClasses(PACKAGE_NAME);

        if(classes.stream().anyMatch(clazz->clazz.isAnnotationPresent(KafkaClient.class)&&clazz.getAnnotation(KafkaClient.class).clientType()==ClientType.CONSUMER)){
            KafkaClient kafkaClient = classes.stream().filter(clazz->clazz.isAnnotationPresent(KafkaClient.class)&&clazz.getAnnotation(KafkaClient.class).clientType()==ClientType.CONSUMER).map(clazz->clazz.getAnnotation(KafkaClient.class)).findFirst().get();
            topic = kafkaClient.topic();
            return true;
        }


        return false;
    }

    public List<ConsumerRecord<K,V>> consume(){

        List<ConsumerRecord<K,V>> consumerRecord = new ArrayList<>();


            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, new Integer(1));


            ConsumerRecords<K, V> consumerRecords = consumer.poll(1000);
            Iterator<ConsumerRecord<K, V>> it = consumerRecords.iterator();
            while (it.hasNext()) {
                consumerRecord.add(it.next());
            }


        return consumerRecord;
    }

    public List<V> consumeMessage(){

        List<ConsumerRecord<K,V>> consumerRecords = consume();

        List<V> list = Lists.newArrayList();

        for(ConsumerRecord<K,V> consumerRecord:consumerRecords){
            V value = consumerRecord.value();
            list.add(value);
        }

        return list;
    }






}
