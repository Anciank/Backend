package com.example.my;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonRoot;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.json.JSONObject;

@RestController
@RequestMapping("/api")
public class DataController {

    @PostMapping("/sendDataToBackend")
    public String sendDataToBackend(@RequestBody String data) throws Exception {
        // Process the received data (e.g., perform some business logic)
        // For this example, we'll simply return the received data + 1.

        // 加载模型文件
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:calcite:model=/Users/anciank/IdeaProjects/My/src/main/resources/model.json");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // 获取根架构
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // Process input data
        // 使用 org.json.JSONObject 解析 JSON 字符串
        JSONObject jsonObject = new JSONObject(data);

        // 提取 "sql" 字段的值
        String sqlQuery = jsonObject.getString("sql");

        // 执行 SQL 查询
        Statement statement = connection.createStatement();
        String sql = sqlQuery;
        ResultSet resultSet = statement.executeQuery(sql);


        String name = "";
        // 处理查询结果
        while (resultSet.next()) {
            // 处理查询结果行
            name += resultSet.getString("NAME");
            //int salary = resultSet.getInt("SALARY");
        }

        resultSet.close();
        statement.close();
        connection.close();

        return name;
    }
}
