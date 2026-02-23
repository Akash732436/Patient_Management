package com.pm.analyticsservice.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaTopicService {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicService.class);
    private final AdminClient adminClient;

    private ConsumerFactory<String, byte[]> consumerFactory;

    public void getOffsets() throws Exception {
        try (Consumer<String, byte[]> consumer = consumerFactory.createConsumer()) {

            List<TopicPartition> partitions =
                    consumer.partitionsFor("patient")
                            .stream()
                            .map(p -> new TopicPartition("patient", p.partition()))
                            .toList();

            consumer.assign(partitions);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            endOffsets.forEach((tp, offset) ->
                    System.out.println("Partition " + tp.partition() + " latest offset = " + offset));
        }
    }

    public KafkaTopicService(AdminClient adminClient, ConsumerFactory<String, byte[]> consumerFactory) {
        this.consumerFactory = consumerFactory;
        this.adminClient = adminClient;
        try {
            log.info("list of available topics: {}",adminClient.listTopics().names().get().toString());
            getOffsets();
        } catch (InterruptedException e) {
            log.error("Error when listing topic names : {}", e.getMessage());
        } catch (Exception e) {
            log.error("Error when listing topic names : {}", e.getMessage());
        }
    }

    public Set<String> listTopics() throws Exception {
        return adminClient.listTopics().names().get();
    }
}
