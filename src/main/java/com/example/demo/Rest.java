package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class Rest {

    @Autowired
    private SendService sendService;

    @GetMapping("/send")
    public Mono<String> send() {
        return sendService.send();
    }

    // sending message to topic2
    @GetMapping("/sendToTopic2")
    public Mono<String> sendTo() {
        return sendService.sendToTopic2();
    }

    // sending message to topic1
    @GetMapping("/sendToTopic1")
    public Mono<String> sendTo0() {
        return sendService.sendToTopic1();
    }
}
