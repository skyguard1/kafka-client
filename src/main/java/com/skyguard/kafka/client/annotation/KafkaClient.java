package com.skyguard.kafka.client.annotation;

import com.skyguard.kafka.client.common.ClientType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaClient {

     String topic() default "";

     ClientType clientType();



}
