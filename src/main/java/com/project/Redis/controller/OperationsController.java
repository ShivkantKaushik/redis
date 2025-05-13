package com.project.Redis.controller;


import com.project.Redis.dao.ListDao;
import com.project.Redis.dao.StreamsDao;
import com.project.Redis.dao.StringDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;

@RequestMapping("/ops")
@RestController
public class OperationsController {

    @Autowired
    StringDao stringDao;

    @Autowired
    ListDao listDao;

    @Autowired
    StreamsDao streamsDao;

    @PostMapping("/string")
    public String stringOps() throws Exception {

        stringDao.stringOperations();

        return "All went smooth";

    }


    @PostMapping("/lists")
    public String listOps() throws Exception {

        listDao.listOps();

        return "All went smooth";
    }

    @PostMapping("/streams")
    public String streamsOps() throws Exception {

//        streamsDao.streamsOps1();

        streamsDao.streamOps2();

        return "All went smooth";


    }


    @PostMapping("/add-to-stream/{stream}")
    public String streamsOps(@PathVariable("stream") String stream, @RequestBody HashMap<Object,Object> data) throws Exception {

        streamsDao.addToStream(stream,data);

        return "All went smooth";


    }


    @PostMapping("/create-consumer-group")
    public String createGroup() throws Exception {

        streamsDao.createConsumerGroups();

        return "All went smooth";

    }

    @PostMapping("/stream-consumer1")
    public String testConsumer1() throws Exception {

        streamsDao.streamOps3_consumer1();

        return "All went smooth";

    }


    @PostMapping("/stream-consumer2")
    public String testConsumer2() throws Exception {

        streamsDao.streamOps3_consumer2();

        return "All went smooth";

    }

}
