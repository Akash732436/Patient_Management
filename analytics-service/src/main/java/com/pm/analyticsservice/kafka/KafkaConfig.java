package com.pm.analyticsservice.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String,byte[]> producerFactory(){
        Map<String,Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public AdminClient adminClient(KafkaProperties properties) {
        return AdminClient.create(properties.buildAdminProperties());
    }

    @Bean
    public KafkaTemplate<String,byte[]> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public PartitionFinder finder(ConsumerFactory<String, String> consumerFactory) {
        return new PartitionFinder(consumerFactory);
    }

    public static class PartitionFinder {

        private final ConsumerFactory<String, String> consumerFactory;

        public PartitionFinder(ConsumerFactory<String, String> consumerFactory) {
            this.consumerFactory = consumerFactory;
        }

        public String[] partitions(String topic) {
            try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
                return consumer.partitionsFor(topic).stream()
                        .map(pi -> "" + pi.partition())
                        .toArray(String[]::new);
            }
        }

    }
}
