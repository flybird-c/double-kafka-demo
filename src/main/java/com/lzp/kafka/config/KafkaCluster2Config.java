package com.lzp.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.ProducerListener;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaCluster2Config {

    @Value("${app.kafka.cluster2.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.cluster2.group-id}")
    private String groupId;
    @Value("${app.kafka.cluster2.security.protocol:SASL_PLAINTEXT}")
    private String securityProtocol;

    @Value("${app.kafka.cluster2.security.enabled:true}")
    private Boolean enabled;

    @Value("${app.kafka.cluster2.security.mechanism:PLAIN}")
    private String saslMechanism;

    @Value("${app.kafka.cluster2.security.username}")
    private String username;

    @Value("${app.kafka.cluster2.security.password}")
    private String password;

    // 构建SASL JAAS配置字符串
    private String buildJaasConfig() {
        return String.format("%s required username=\"%s\" password=\"%s\";",
                PlainLoginModule.class.getName(), username, password);
    }

    // Cluster2 Consumer Factory
    @Bean
    public ConsumerFactory<String, String> cluster2ConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        // 连接配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 反序列化配置
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 消费行为配置 - 可以有不同的配置策略
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // 手动提交
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);     // 更小的批次
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 更长的超时
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 40000);

        if (enabled) {
            // SASL安全配置
            props.put("security.protocol", securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig());
        }
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Cluster2 Producer Factory
    @Bean
    public ProducerFactory<String, String> cluster2ProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        // 连接配置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // 序列化配置
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 生产行为配置 - 不同的调优策略
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);         // 更大的批次
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);   // 更大的缓冲区
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");  // 启用压缩
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 120000);      // 更长的阻塞时间

        if (enabled) {
            // SASL安全配置
            props.put("security.protocol", securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig());
        }
        return new DefaultKafkaProducerFactory<>(props);
    }

    // Cluster2 Kafka Template
    @Bean("cluster2KafkaTemplate")
    public KafkaTemplate<String, String> cluster2KafkaTemplate() {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(cluster2ProducerFactory());

        // 添加发送监听器
        template.setProducerListener(new ProducerListener<String, String>() {
            @Override
            public void onSuccess(ProducerRecord<String, String> record, RecordMetadata recordMetadata) {
                System.out.println("Cluster2 发送成功 - Topic: " + record.topic() + ", Value: " + record.value());
            }

            @Override
            public void onError(ProducerRecord<String, String> record, RecordMetadata recordMetadata, Exception exception) {
                System.err.println("Cluster2 发送失败 - Topic: " + record.topic() + ", Error: " + exception.getMessage());
            }
        });

        return template;
    }

    // Cluster2 Kafka Listener Container Factory
    @Bean("cluster2KafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> cluster2KafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cluster2ConsumerFactory());
        factory.setConcurrency(2);  // 不同的并发数
        factory.getContainerProperties().setPollTimeout(5000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);  // 手动提交

        // 设置消息监听器
        factory.setRecordInterceptor(record -> {
            System.out.println("Cluster2 收到消息 - Topic: " + record.topic() + ", Value: " + record.value());
            return record;
        });
        return factory;
    }
}