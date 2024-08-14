package com.example.roseDB_Example.controller;


import com.example.roseDB_Example.model.User;
import com.example.roseDB_Example.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/users")
public class UserController {
    private final UserService userService;
    @Autowired
    public UserController(UserService userService) {
        this.userService = userService;
    }

    // 添加用户

@PostMapping("add")
    public ResponseEntity<String> addUser(@RequestBody User user){
    System.out.println("Received request to add user: " + user);
        try{
            userService.addUser(user);
            return ResponseEntity.ok("User added successfully");
        }catch (Exception e){
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to add user");
        }
}
}
