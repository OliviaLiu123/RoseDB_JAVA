package com.example.roseDB_Example.service;

import com.example.roseDB_Example.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import main.java.com.rosedb.db.DB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class UserService {

    private final DB db;
    private final ObjectMapper objectMapper;

    @Autowired
    public UserService(DB db) {
        this.db = db;
        this.objectMapper = new ObjectMapper();
    }

    // 添加用户
    public void addUser(User user) throws Exception {
        String jsonString = serialize(user);
        db.put(user.getEmail().getBytes(), jsonString.getBytes());
    }

    // 根据名字获取用户 by email
    public User getUser(String email) throws Exception {
        byte[] userBytes = db.get(email.getBytes());
        if (userBytes != null) {
            return deserialize(new String(userBytes));
        }
        return null;
    }

    // 获取所有用户
    public List<User> getAllUsers() throws Exception {
        List<User> users = new ArrayList<>();

        // 假设 RoseDB 支持按键升序遍历
        db.ascend((key, value) -> {
            User user = null;
            try {
                user = deserialize(new String(value));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            users.add(user);
        });

        return users;
    }

    // 删除用户
    public void deleteUser(String name) throws Exception {
        db.delete(name.getBytes());
    }

    // 序列化用户对象为 JSON 字符串
    private String serialize(User user) throws IOException {
        return objectMapper.writeValueAsString(user);
    }

    // 反序列化 JSON 字符串为用户对象
    private User deserialize(String json) throws IOException {
        return objectMapper.readValue(json, User.class);
    }
}

