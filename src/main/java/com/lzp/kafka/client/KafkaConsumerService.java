package com.lzp.kafka.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumerService {
    

    /**
     * 消费集群1的消息（使用默认的Listener Factory）
     */
    @KafkaListener(topics = "topic1", groupId = "group1")
    public void consumeFromCluster1(String message) {
        log.info("从集群1消费消息: {}", message);
        // 处理业务逻辑
        processCluster1Message(message);
    }

    /**
     * 消费集群2的消息（指定特定的Listener Factory）
     */
    @KafkaListener(
        topics = "topic2", 
        groupId = "group2",
        containerFactory = "cluster2KafkaListenerContainerFactory"
    )
    public void consumeFromCluster2(String message, Acknowledgment ack) {
        log.info("从集群2消费消息: {}", message);
        
        try {
            // 处理业务逻辑
            processCluster2Message(message);
            // 手动提交偏移量
            ack.acknowledge();
            log.info("集群2消息处理完成并提交: {}", message);
        } catch (Exception e) {
            log.error("集群2消息处理失败: {}", message, e);
            // 不提交偏移量，等待重试
        }
    }

    private void processCluster1Message(String message) {
        // 集群1消息处理逻辑
        System.out.println("处理集群1消息: " + message);
    }

    private void processCluster2Message(String message) {
        // 集群2消息处理逻辑
        System.out.println("处理集群2消息: " + message);
    }
}