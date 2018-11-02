package de.pkutschera.spring.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.time.LocalDateTime;

@org.springframework.web.bind.annotation.RestController
public class Controller {

    @Autowired
    private MessageProducer producer;

    @GetMapping("/time")
    public String getTime() {
        return LocalDateTime.now().toString();
    }

    @GetMapping("/pingpong")
    public ResponseEntity<String> send(@RequestParam("message") String message) {
        ResponseEntity<String> responseEntity;
        try {
            String reply = producer.sendAndReceiveMessage(message);
            responseEntity = new ResponseEntity<>(reply, HttpStatus.OK);
        } catch (Exception e) {
            responseEntity = new ResponseEntity<>(e.getLocalizedMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return responseEntity;
    }
}
