package com.lzp.kafka.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class KafkaProducerService {
    

    private final KafkaTemplate<String, String> cluster1KafkaTemplate;
    private final KafkaTemplate<String, String> cluster2KafkaTemplate;

    public KafkaProducerService(
             KafkaTemplate<String, String> cluster1KafkaTemplate,
            @Qualifier("cluster2KafkaTemplate") KafkaTemplate<String, String> cluster2KafkaTemplate) {
        this.cluster1KafkaTemplate = cluster1KafkaTemplate;
        this.cluster2KafkaTemplate = cluster2KafkaTemplate;
    }

    /**
     * 发送消息到集群1
     */
    public SendResult<String, String> sendToCluster1(String topic, String message) {
        log.info("发送消息到集群1 - Topic: {}, Message: {}", topic, message);
        
        ListenableFuture<SendResult<String, String>> future =
            cluster1KafkaTemplate.send(topic, message);
        
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("发送消息到集群1失败", e);
            throw new RuntimeException("发送消息到集群1失败", e);
        }
    }

    /**
     * 发送消息到集群2
     */
    public SendResult<String, String> sendToCluster2(String topic, String message) {
        log.info("发送消息到集群2 - Topic: {}, Message: {}", topic, message);
        
        ListenableFuture<SendResult<String, String>> future = 
            cluster2KafkaTemplate.send(topic, message);
        
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("发送消息到集群2失败", e);
            throw new RuntimeException("发送消息到集群2失败", e);
        }
    }

    /**
     * 异步发送到集群1
     */
    public void sendToCluster1Async(String topic, String message) {
        cluster1KafkaTemplate.send(topic, message).addCallback(
            result -> log.info("集群1异步发送成功 - Topic: {}, Offset: {}",
                                topic, result.getRecordMetadata().offset()),
            ex -> log.error("集群1异步发送失败 - Topic: {}", topic, ex)
        );
    }

    /**
     * 异步发送到集群2
     */
    public void sendToCluster2Async(String topic, String message) {
        cluster2KafkaTemplate.send(topic, message).addCallback(
            result -> log.info("集群2异步发送成功 - Topic: {}, Offset: {}",
                                topic, result.getRecordMetadata().offset()),
            ex -> log.error("集群2异步发送失败 - Topic: {}", topic, ex)
        );
    }
}