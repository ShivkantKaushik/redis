package com.project.Redis.dao;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Repository
@Slf4j
public class ListDao {



    @Autowired
            @Qualifier("genericBean")
    RedisTemplate<String, Object> redisTemplate;


    public static final String KEY = "my_list";

    public void listOps() throws Exception{

        try {

            //create list, push

            redisTemplate.opsForList().leftPush(KEY,1);
            redisTemplate.opsForList().leftPush(KEY,"ABC");
            redisTemplate.opsForList().rightPush(KEY,3);


            //pop

            String getLeftElement = (String) redisTemplate.opsForList().leftPop(KEY);

            log.debug("Left Element " + getLeftElement);

            Integer rightElement = (int) Objects.requireNonNullElse(redisTemplate.opsForList().rightPop(KEY), 0 );

            log.debug("Right Element " + rightElement);


            //pop with blocking
            //lets first empty the list, then execute pop with timeout command, then after some time add element from
            //cli

            int sizeOfList = Objects.requireNonNullElse(redisTemplate.opsForList().size(KEY),0).intValue();

            for(int i = 0; i < sizeOfList; i++){
                redisTemplate.opsForList().leftPop(KEY);
            }


            //trim means, list from start to end only will remain
//            redisTemplate.opsForList().trim(KEY,0, sizeOfList+1);


//            int firstElementInBPop = (int) Objects.requireNonNull(redisTemplate.opsForList().leftPop(KEY, Duration.ofSeconds(50)));
//
//            log.debug("poped element after bpop " + firstElementInBPop);


            //moving

            //adding again element in my_list
            redisTemplate.opsForList().leftPushAll(KEY,3,2,1);

            //creating new list
            redisTemplate.opsForList().leftPushAll("my_list2", 6,5,4);

            redisTemplate.opsForList().move(ListOperations.MoveFrom.fromTail(KEY), ListOperations.MoveTo.toHead("my_list2"));


            //now after this my_list should be 1,2 and my_list2 should be 3,4,5,6, check this in redis stack ui

            int list1Size = Objects.requireNonNull(redisTemplate.opsForList().size(KEY)).intValue();
            List<Integer> list1 = Objects.requireNonNull(redisTemplate.opsForList().range(KEY,0,list1Size)).stream().map(x -> (int)x).toList();

            log.debug("List1 after moving " + list1);







        }catch (Exception e){
            throw new Exception("Exception in listOps");
        }

    }


}
