package de.pkutschera.spring.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;

@Slf4j
public class MessageProducer {

    private ReplyingKafkaTemplate<String, String, String> kafkaTemplate;
    private String topicName;

    public MessageProducer(ReplyingKafkaTemplate<String, String, String> kafkaTemplate, String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public String sendAndReceiveMessage(String message) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
        RequestReplyFuture<String, String, String> replyFuture = kafkaTemplate.sendAndReceive(record);
        SendResult<String, String> sendResult = replyFuture.getSendFuture().get();
        log.debug("Sent ok: " + sendResult.getRecordMetadata());
        ConsumerRecord<String, String> consumerRecord = replyFuture.get();
        log.debug("Return value: " + consumerRecord.value());
        return consumerRecord.value();
    }
}
