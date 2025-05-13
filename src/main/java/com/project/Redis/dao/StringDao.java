package com.project.Redis.dao;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;


import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Repository
@Slf4j
public class StringDao {

    @Autowired
    @Qualifier("genericBean")
    RedisTemplate<String,Object> redisTemplate;


    public void stringOperations() throws Exception{

        try {
            redisTemplate.opsForValue().setIfAbsent("CountA",1);
            redisTemplate.opsForValue().increment("CountA");
            redisTemplate.opsForValue().increment("CountA",2);

            //if we give any random key, res will be null
            log.debug("CountA value Here1:- " + redisTemplate.opsForValue().get("CountA"));


            //Multi Set
            HashMap<String,Object> multiSetHashMap = new HashMap<>();

            multiSetHashMap.put("CountB", 12);
            multiSetHashMap.put("CountC", 13);

            redisTemplate.opsForValue().multiSet(multiSetHashMap);


            //MultiGet
            List<Integer> multiGetValues =  Objects.requireNonNull(redisTemplate.opsForValue().multiGet(Arrays.asList("CountA", "CountB"))).stream().map(x -> (int) x).toList();

            log.debug("MultiGetValues " + multiGetValues);


            //Limit, Expire
            redisTemplate.opsForValue().getAndExpire("CountC",5, TimeUnit.SECONDS);


            //Redis template is used as driver for connecting to redis
            //if we want to execute exact command , and want more control, we may use jedis
            //or like below

            //we don't have some commands in RedisTemplate

            String substring = redisTemplate.execute(new RedisCallback<String>() {
                                                         @Override
                                                         public String doInRedis(RedisConnection connection) throws DataAccessException {
                                                             return new String(Objects.requireNonNull(connection.stringCommands().getRange("name".getBytes(StandardCharsets.UTF_8), 0, 1)), StandardCharsets.UTF_8);
                                                         }
                                                     }
            );

            System.out.println("Substring " + substring);


        }catch (Exception e){

            throw new Exception("Error in string ops");
        }



    }

}
