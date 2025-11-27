package com.lzp.kafka.controller;

import com.lzp.kafka.client.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private final KafkaProducerService kafkaProducerService;

    public KafkaController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping("/send/cluster1")
    public ResponseEntity<String> sendToCluster1(
            @RequestParam String topic,
            @RequestParam String message) {
        try {
            SendResult<String, String> result = kafkaProducerService.sendToCluster1(topic, message);
            return ResponseEntity.ok("消息已发送到集群1, Offset: " + result.getRecordMetadata().offset());
        } catch (Exception e) {
            return ResponseEntity.status(500).body("发送到集群1失败: " + e.getMessage());
        }
    }

    @PostMapping("/send/cluster2")
    public ResponseEntity<String> sendToCluster2(
            @RequestParam String topic,
            @RequestParam String message) {
        try {
            SendResult<String, String> result = kafkaProducerService.sendToCluster2(topic, message);
            return ResponseEntity.ok("消息已发送到集群2, Offset: " + result.getRecordMetadata().offset());
        } catch (Exception e) {
            return ResponseEntity.status(500).body("发送到集群2失败: " + e.getMessage());
        }
    }

    @PostMapping("/send/async/cluster1")
    public ResponseEntity<String> sendAsyncToCluster1(
            @RequestParam String topic,
            @RequestParam String message) {
        kafkaProducerService.sendToCluster1Async(topic, message);
        return ResponseEntity.ok("异步消息已发送到集群1");
    }

    @PostMapping("/send/async/cluster2")
    public ResponseEntity<String> sendAsyncToCluster2(
            @RequestParam String topic,
            @RequestParam String message) {
        kafkaProducerService.sendToCluster2Async(topic, message);
        return ResponseEntity.ok("异步消息已发送到集群2");
    }
}