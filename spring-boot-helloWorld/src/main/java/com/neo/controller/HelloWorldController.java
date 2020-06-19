package com.neo.controller;

import com.neo.domain.Monitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class HelloWorldController {


    @Autowired
    List<Monitor> monitors;

    @RequestMapping("/hello")
    @ResponseBody
    public List<Monitor> index() {
        return monitors;
    }

}