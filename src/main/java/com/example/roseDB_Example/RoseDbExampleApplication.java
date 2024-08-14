package com.example.roseDB_Example;

import com.example.roseDB_Example.model.User;
import com.example.roseDB_Example.service.UserService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;



@SpringBootApplication
public class RoseDbExampleApplication {

	public static void main(String[] args) {
		ApplicationContext context =SpringApplication.run(RoseDbExampleApplication.class, args);
		// 获取 UserService Bean
		UserService userService = context.getBean(UserService.class);

		// 创建一个测试用户
		User testUser = new User("Test User", 30, "test@example.com");

		try {
			// 测试添加用户
			userService.addUser(testUser);
			System.out.println("User added successfully");

			// 读取用户，验证写入是否成功
//			User retrievedUser = userService.getUser("test@example.com");
//			System.out.println("Retrieved User: " + retrievedUser);

		} catch (Exception e) {
			System.err.println("Error occurred during testing: " + e.getMessage());
			e.printStackTrace();
		}
	}
	}


