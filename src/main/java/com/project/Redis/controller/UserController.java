package com.project.Redis.controller;

import com.project.Redis.dao.UserDao;
import com.project.Redis.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/users")
public class UserController {

    @Autowired
    private UserDao userDao;


    // create user
    @PostMapping
    public User createUser(@RequestBody User user
    ) {

        user.setUserId(UUID.randomUUID().toString());
        return userDao.save(user);


    }

    //get single user

    @GetMapping("/{userId}")
    public User getUser(@PathVariable("userId") String userId) {
        return userDao.get(userId);
    }

    //find all
    @GetMapping
    public List<User> getAll() {

        Map<Object, Object> all = userDao.getAll();
        Collection<Object> values = all.values();
        List<User> users = values.stream().map(value -> (User) value).collect(Collectors.toList());
        return users;

    }

    //delete  user
    @DeleteMapping("/{userId}")
    public void deleteUser(@PathVariable String userId) {
        userDao.delete(userId);
    }

}