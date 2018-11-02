package de.pkutschera.spring.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Value(value = "${message.request.topic.name}")
    private String requestTopicName;

    @Value(value = "${message.response.topic.name}")
    private String responseTopicName;

    @Bean
    /**
     * Dynamically created topic by KafkaAdmin bean
     */
    public NewTopic requestTopic() {
        return new NewTopic(requestTopicName, 1, (short) 1);
    }

    @Bean
    /**
     * Dynamically created topic by KafkaAdmin bean
     */
    public NewTopic responseTopic() {
        return new NewTopic(responseTopicName, 1, (short) 1);
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate (
            ProducerFactory<String, String> producerFactory,
            KafkaMessageListenerContainer<String, String> responseContainer) {

        return new ReplyingKafkaTemplate<>(producerFactory, responseContainer);
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> responseContainer(ConsumerFactory<String, String> consumerFactory) {
        ContainerProperties containerProperties = new ContainerProperties(responseTopicName);
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    }

    @Bean
    public MessageProducer messageProducer(ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate) {
        return new MessageProducer(replyingKafkaTemplate, requestTopicName);
    }
}