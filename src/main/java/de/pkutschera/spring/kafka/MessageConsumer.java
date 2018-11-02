package de.pkutschera.spring.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;

public class MessageConsumer {

    @KafkaListener(topics = "${message.request.topic.name}", id = "server")
    @SendTo // send to response queue
    public String listen(String message) {
        System.out.println("Received Message in group 'standard': " + message);
        return message;
    }
}
