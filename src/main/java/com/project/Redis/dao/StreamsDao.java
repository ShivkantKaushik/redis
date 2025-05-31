package com.project.Redis.dao;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Repository
@Slf4j
public class StreamsDao {

    @Autowired
    @Qualifier("genericBean")
    RedisTemplate<String, Object> redisTemplate;

    @Autowired()
    @Qualifier("customStringRedisTemplate")
    RedisTemplate<String,Object> stringRedisTemplate;

    public final String KEY = "my_stream";
    public final String KEY2 = "my_stream_2";
    public final String KEY3 = "my_stream_3";



    //for add, range
    public void streamsOps1() throws Exception{


        try {


            //with each entry(timestamp-sequence), there is one map, this map can have many key value pairs, that we add
            Map<String, Object> dataToPush1 = new HashMap<>(){{
                put("name", "abc");
                put("abc-age", 15);
            }};


            redisTemplate.opsForStream().add(KEY,dataToPush1);

            Map<Object, Object> dataToPush2 = new HashMap<>(){{
                put("name", "rdh");
                put("rdh-age", 14);
            }};

            MapRecord<String,Object,Object> record1 = MapRecord.create(KEY,dataToPush2);


            //second way of adding
            redisTemplate.opsForStream().add(record1);

            //also we can have record or object record

            Map<Object, Object> dataToPush3 = new HashMap<>(){{
                put("name", "rgh");
                put("rgh-age", 15);
            }};


            Map<Object, Object> dataToPush4 = new HashMap<>(){{
                put("name", "sti");
                put("sti-age", 14);
            }};

            Record<String,Map<Object,Object>> record2 = ObjectRecord.create(KEY,dataToPush3);
            ObjectRecord<String,Map<Object,Object>> record3 = ObjectRecord.create(KEY,dataToPush4);

            redisTemplate.opsForStream().add(record2);
            redisTemplate.opsForStream().add(record3);





            //        streamOps.range(STREAM_NAME, "0", "+"); // Fetch all messages
            //        streamOps.range(STREAM_NAME, "1682851200000-0", "1682851300000-0"); // Fetch messages within a specific range

            //with each entry(timestamp-sequence), there is one map, this map can have many key value pairs, that we add

            //"+" is for latest entry, "0" means, timestamp is also 0 and sequence is also 0
            List<MapRecord<String,Object,Object>> mapRecords =  redisTemplate.opsForStream().range(KEY, Range.<String>from(Range.Bound.inclusive("0")).to(Range.Bound.inclusive("+")));

            for (MapRecord<String,Object,Object> mapRecord : mapRecords){

                Map<Object,Object> keyValuePairs = mapRecord.getValue();

                Set<Map.Entry<Object,Object>> entrySet = keyValuePairs.entrySet();

                //we can use enhanced for loop on sets, instead of iterators
                for (Map.Entry<Object, Object> entry : entrySet) {

                    log.debug("Key: " + entry.getKey());

                    log.debug("Value: " + entry.getValue());

                }

            }


            log.debug("*******Now range with limit");

            // "+" refers to maximum id possible
            //"$" This special ID means that XREAD should use as last ID the maximum ID already stored in the stream mystream, so that we will receive only new messages, starting from the time we started listening. This is similar to the tail -f Unix command in some way.
            //"-" means, lowest id
            //"0" means, 0 timestamp, 0 sequence

            List<MapRecord<String,Object,Object>> mapRecordsWithLimit =  redisTemplate.opsForStream().range(KEY, Range.<String>from(Range.Bound.inclusive("0")).to(Range.Bound.inclusive("+")), Limit.limit().count(3));


            for (MapRecord<String,Object,Object> mapRecord : mapRecordsWithLimit){

                Map<Object,Object> keyValuePairs = mapRecord.getValue();

                Set<Map.Entry<Object,Object>> entrySet = keyValuePairs.entrySet();

                //we can use enhanced for loop on sets, instead of iterators
                for (Map.Entry<Object, Object> entry : entrySet) {

                    log.debug("Key: " + entry.getKey());

                    log.debug("Value: " + entry.getValue());

                }

            }


        }catch (Exception e){
            throw new Exception(e.getMessage());
        }



    }





    //for reading streams
    public void streamOps2() throws Exception {

        try {


          int emptyRecordCount = 0;



          //this technique is used, when we use blocking list like in multithreading, like in redis block pop etc.

          //means we will run infinite loop, then block the list for lets say 2 minutes, will collect all messages
          //that appear in 2 mins, then process them all, again while loop will run, again wil block for 2 mins
          //if we receive empty list, means no new messages, so we count, how many times empty received, on this
          // basis we break while loop

          //if we want instantly, when we receive new message, immediately we want to process
          //we can use something like, block 0 ms, count 1
          // but if depends how efficient we want, because in this way if will ask each millisecond, also each milliseond
          //while loop will run

          //so for efficiency we can have something like, block for 100ms, count 1,2,3, it depends



          //New Observation:- redis will not block for entire time we provide, it will return as soon as it gets new message
          //block is the maximum time it will wait, after that will return empty list
          //count means, if new messages arrive at same timestamp, it will return only 'count' message at once

         while(true) {

             if(emptyRecordCount == 2){
                 break;
             }




             //it is just for showing syntax
//             List<MapRecord<String, Object, Object>> mapRecords = redisTemplate.opsForStream().read(StreamReadOptions.empty().count(3).block(Duration.of(2, ChronoUnit.MINUTES)),
//                     StreamOffset.create(KEY2, ReadOffset.from("$")), StreamOffset.fromStart(KEY3));

             //so now, we will push data from cli and check here

             //from cli, we were getting deserialization errors, so ran multiple instances, and from one instace we will add data
             List<MapRecord<String, Object, Object>> mapRecords = redisTemplate.opsForStream().read(StreamReadOptions.empty().count(3).block(Duration.of(1, ChronoUnit.MINUTES)),
                     StreamOffset.create(KEY2, ReadOffset.from("$")), StreamOffset.create(KEY3,ReadOffset.from("+")));

             //Observation:- when we give from "+", it is reading last message again and again, because + means max id, so it reading from max(latest)
             //again and again
             // if we give from "$", reading only new messages we are pushing, because $ means, last id is max id, so now it will read new messages


             if(mapRecords.size() == 0){
                 emptyRecordCount++;
                 continue;
             }

             for (MapRecord<String, Object, Object> mapRecord : mapRecords) {

                 Map<Object, Object> keyValuePairs = mapRecord.getValue();

                 Set<Map.Entry<Object, Object>> entrySet = keyValuePairs.entrySet();

                 //we can use enhanced for loop on sets, instead of iterators
                 for (Map.Entry<Object, Object> entry : entrySet) {

                     log.debug("Key in xread testing " + entry.getKey());

                     log.debug("Value in xread testing " + entry.getValue());

                 }

             }

         }


        }catch (Exception e){
            throw new Exception(e.getMessage());
        }

    }



    public void addToStream(String stream, HashMap<Object, Object> data) throws Exception {

        try {

            MapRecord<String,Object,Object> record = MapRecord.create(stream,data);

            redisTemplate.opsForStream().add(record);

        }catch (Exception e){
            throw new Exception(e.getMessage());
        }

    }

    public void createConsumerGroups() throws Exception{
        redisTemplate.opsForStream().createGroup(KEY2, "consumer-group-1");
    }






    //for consumer groups, consumer1
    public void streamOps3_consumer1() throws Exception {

        try {

            int emptyRecordCount = 0;

            while(true) {

                if(emptyRecordCount == 2){
                    break;
                }

                // ">" means, it will read only new messages, and those new, that not delivered to any other consumer
                // also if we give from "0", it will not read all the messages in stream, but messages it have previosly read
                //may be it started reading from id 9, so it will not give 1-8, when we give from "0"
                //basicaly it will just give the history of consumer, what it has read till now

                //with two streams it was giving error, may be in redis, one consumer-group can only have one stream
                List<MapRecord<String, Object, Object>> mapRecords = redisTemplate.opsForStream().read(Consumer.from("consumer-group-1", "consumer1"), StreamReadOptions.empty().count(1).block(Duration.of(1, ChronoUnit.MINUTES)),
                        StreamOffset.create(KEY2, ReadOffset.from("0")));

                if(mapRecords.size() == 0){
                    emptyRecordCount++;
                    continue;
                }

                for (MapRecord<String, Object, Object> mapRecord : mapRecords) {

                    Map<Object, Object> keyValuePairs = mapRecord.getValue();

                    Set<Map.Entry<Object, Object>> entrySet = keyValuePairs.entrySet();

                    //we can use enhanced for loop on sets, instead of iterators
                    for (Map.Entry<Object, Object> entry : entrySet) {

                        log.debug("Key in consumer1 " + entry.getKey());

                        log.debug("Value in consumer1 " + entry.getValue());

                    }

                }

            }


        }catch (Exception e){
            throw new Exception(e.getMessage());
        }

    }


    //consumer_2
    public void streamOps3_consumer2() throws Exception {

        try {

            int emptyRecordCount = 0;

            while(true) {

                if(emptyRecordCount == 2){
                    break;
                }



                //with two streams it was giving error, may be in redis, one consumer-group can only have one stream
                List<MapRecord<String, Object, Object>> mapRecords = redisTemplate.opsForStream().read(Consumer.from("consumer-group-1", "consumer2"), StreamReadOptions.empty().count(2).block(Duration.of(1, ChronoUnit.MINUTES)),
                        StreamOffset.create(KEY2, ReadOffset.from(">")));

                if(mapRecords.size() == 0){
                    emptyRecordCount++;
                    continue;
                }

                for (MapRecord<String, Object, Object> mapRecord : mapRecords) {

                    Map<Object, Object> keyValuePairs = mapRecord.getValue();

                    Set<Map.Entry<Object, Object>> entrySet = keyValuePairs.entrySet();

                    //we can use enhanced for loop on sets, instead of iterators
                    for (Map.Entry<Object, Object> entry : entrySet) {

                        log.debug("Key in consumer2 " + entry.getKey());

                        log.debug("Value in consumer2 " + entry.getValue());

                    }

                }

            }


        }catch (Exception e){
            throw new Exception(e.getMessage());
        }

    }




    public void createStream2(){


        Map<Object,Object> data1 = new HashMap<>(){{
            put("a", "a-1");
            put("b", 15);
        }};


        Map<Object,Object> data2 = new HashMap<>(){{
            put("c", "c-2");
            put("d", 14);
        }};


        Map<Object,Object> data3 = new HashMap<>(){{
            put("e", "e-3");
            put("f", 149);
        }};

        Map<Object,Object> data4 = new HashMap<>(){{
            put("g", "g-4");
            put("h", 19);
        }};

        Map<Object,Object> data5 = new HashMap<>(){{
            put("i", "i-3");
            put("j", 49);
        }};


    }





    private void setOps(){
//        redisTemplate.opsForSet().add();
//        redisTemplate.opsForSet().intersect();
//        redisTemplate.opsForSet().difference();
//        redisTemplate.opsForSet().size();
//        redisTemplate.opsForSet().remove();
//
         //Zset is as sorted set, where we have score associated with entries, read in docs
//
//        redisTemplate.opsForZSet().add();
//        redisTemplate.opsForZSet().difference();
//        redisTemplate.opsForZSet().score();
//        redisTemplate.opsForZSet().intersect();
//
//        ..and so on

    }


    public void createStream3(){
        Map<Object,Object> data1 = new HashMap<>(){{
            put("k", "k-1");
            put("l", 95);
        }};


        Map<Object,Object> data2 = new HashMap<>(){{
            put("m", "m-2");
            put("n", 34);
        }};


        Map<Object,Object> data3 = new HashMap<>(){{
            put("o", "o-3");
            put("p", 129);
        }};

        Map<Object,Object> data4 = new HashMap<>(){{
            put("q", "q-4");
            put("r", 198);
        }};

        Map<Object,Object> data5 = new HashMap<>(){{
            put("s", "s-3");
            put("t", 9);
        }};

    }


}
