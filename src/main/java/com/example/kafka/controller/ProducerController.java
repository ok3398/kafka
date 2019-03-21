package com.example.kafka.controller;

import com.example.kafka.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping(value = "/kafka")
public class ProducerController {
    @Autowired
    ProducerService producerService;

    @RequestMapping(value = "send", method = RequestMethod.GET)
    public String sendKafka(HttpServletRequest request) throws Exception{
        String returncode = producerService.sendKafka();
        return returncode;
    }
}
