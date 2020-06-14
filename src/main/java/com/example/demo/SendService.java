package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;


@Service
public class SendService {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    public Mono<String> send() {
        final String TOPIC1 = "Topic1";
        final String TOPIC2 = "Topic2";
        final String MESSAGE1 = "Message1";
        final String MESSAGE2 = "Message2";

        return sendToTopic(TOPIC1, MESSAGE1)
                .flatMap(x -> sendToTopic(TOPIC2, MESSAGE2));
    }

    public Mono<String> sendToTopic2() {

        return sendToTopic("Topic2", "singleMessage");
    }

    public Mono<String> sendToTopic1() {

        return sendToTopic("Topic1", "singleMessage");
    }

    private Mono<String> sendToTopic(String topic, String message) {
        System.out.println("sending messages to kafka on topic ---" + topic);
        return Mono.create(sink -> kafkaTemplate.send(topic, message)
                .addCallback(
                        ok -> {
                            System.out.println("Successfully sent: " + message);
                            sink.success(message);
                        },
                        err -> {
                            System.out.println("Failed to send: " + message);
                            sink.error(err);
                        }));
    }
}
