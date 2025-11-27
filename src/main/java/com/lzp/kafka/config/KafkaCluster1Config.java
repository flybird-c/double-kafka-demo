package com.lzp.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaCluster1Config {

    @Value("${app.kafka.cluster1.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.cluster1.group-id}")
    private String groupId;
    @Value("${app.kafka.cluster1.security.protocol:SASL_PLAINTEXT}")
    private String securityProtocol;

    @Value("${app.kafka.cluster1.security.enabled:true}")
    private Boolean enabled;

    @Value("${app.kafka.cluster1.security.mechanism:PLAIN}")
    private String saslMechanism;

    @Value("${app.kafka.cluster1.security.username}")
    private String username;

    @Value("${app.kafka.cluster1.security.password}")
    private String password;

    // 构建SASL JAAS配置字符串
    private String buildJaasConfig() {
        return String.format("%s required username=\"%s\" password=\"%s\";",
                PlainLoginModule.class.getName(), username, password);
    }

    // Cluster1 Consumer Factory
    @Bean
    public ConsumerFactory<String, String> cluster1ConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        // 连接配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 反序列化配置
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 消费行为配置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5000);

        if (enabled) {
            // SASL安全配置
            props.put("security.protocol", securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig());
        }

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Cluster1 Producer Factory
    @Bean
    public ProducerFactory<String, String> cluster1ProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        // 连接配置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // 序列化配置
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 生产行为配置
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);

        if (enabled) {
            // SASL安全配置
            props.put("security.protocol", securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig());
        }

        return new DefaultKafkaProducerFactory<>(props);
    }

    // Cluster1 Kafka Template
    @Bean
    @Primary  // 标记为主要KafkaTemplate
    public KafkaTemplate<String, String> cluster1KafkaTemplate() {
        return new KafkaTemplate<>(cluster1ProducerFactory());
    }

    // Cluster1 Kafka Listener Container Factory
    @Bean
    @Primary  // 标记为主要Listener Factory
    public ConcurrentKafkaListenerContainerFactory<String, String> cluster1KafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cluster1ConsumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);

        // 设置消息监听器
        factory.setRecordInterceptor(record -> {
            System.out.println("Cluster1 收到消息 - Topic: " + record.topic() + ", Value: " + record.value());
            return record;
        });

        return factory;
    }
}