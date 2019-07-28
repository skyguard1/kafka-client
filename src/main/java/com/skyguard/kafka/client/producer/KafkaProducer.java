package com.skyguard.kafka.client.producer;

import com.skyguard.kafka.client.annotation.KafkaClient;
import com.skyguard.kafka.client.common.ClientType;
import com.skyguard.kafka.client.config.KafkaConfig;
import com.skyguard.kafka.client.util.ClassUtil;
import com.skyguard.kafka.client.util.PropertyUtil;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaProducer {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    //定义一个produce的参数
    private final org.apache.kafka.clients.producer.KafkaProducer<Integer, String> producer;

    private static final String PACKAGE_NAME = PropertyUtil.getValue(KafkaConfig.PACKAGE_NAME);

    private static String topic = "";

    public KafkaProducer() {
        //给producer属性赋值
        Map<String, String> props = new HashMap<String, String>();
        //序列化 防止在转换的时候抛出异常
        props.put("value.serializer", PropertyUtil.getValue(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        props.put("key.serializer", PropertyUtil.getValue(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        //配置kafka broker list的地址
        props.put("bootstrap.servers", PropertyUtil.getValue(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));      //配置kafka broker list的地址
        props.put("zk.connect", PropertyUtil.getValue(KafkaConfig.ZK_CONNECT_URL));
        //将producer 的属性值赋值进去
        producer = new org.apache.kafka.clients.producer.KafkaProducer(props);

    }

    private boolean isKafkaProducer(){

        Set<Class<?>> classes = ClassUtil.getClasses(PACKAGE_NAME);

        if(classes.stream().anyMatch(clazz->clazz.isAnnotationPresent(KafkaClient.class)&&clazz.getAnnotation(KafkaClient.class).clientType()==ClientType.PRODUCER)){
            KafkaClient kafkaClient = classes.stream().filter(clazz->clazz.isAnnotationPresent(KafkaClient.class)&&clazz.getAnnotation(KafkaClient.class).clientType()==ClientType.PRODUCER).map(clazz->clazz.getAnnotation(KafkaClient.class)).findFirst().get();
            topic = kafkaClient.topic();
            return true;
        }


        return false;
    }

    //发消息的
    public void produce(String message) throws InterruptedException {

        try {
            if(isKafkaProducer()) {
                //发送消息
                producer.send(new ProducerRecord<Integer, String>(topic, message));
            }
        } catch (Exception e){
            LOG.error("send message error",e);
        }


    }







}
