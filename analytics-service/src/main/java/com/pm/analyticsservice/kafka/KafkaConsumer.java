package com.pm.analyticsservice.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topicPartitions = @TopicPartition(topic = "patient",
            partitions = "#{@finder.partitions('patient')}",
            partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0")))
    public void consumeEvent(byte[] event){
        try {
            PatientEvent patientEvent = PatientEvent.parseFrom(event);
            log.info("Received patient event: [PatientId = {}, PatientName = {}, PatientEmail = {}]",
                    patientEvent.getPatientId(),
                    patientEvent.getName(),
                    patientEvent.getEmail());
        } catch (InvalidProtocolBufferException e) {
            log.error("Error deserializing event: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Exception: {}", e.getMessage());
        }
    }

//    @KafkaListener(topics = "patient2", groupId = "analytics-service")
//    public void consumeEvent(String event){
//        try {
//            //PatientEvent patientEvent = PatientEvent.parseFrom(event);
//            log.info("Received patient2 event: {}",event);
//        } catch (Exception e) {
//            log.error("Error deserializing event: {}", e.getMessage());
//        }
//    }

}
