package com.example.my;

import org.json.JSONArray;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonRoot;
import org.apache.calcite.schema.SchemaPlus;

import java.io.Reader;
import java.sql.*;

import org.json.JSONObject;

import org.json.JSONException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

@RestController
@RequestMapping("/api")
public class DataController {

    @PostMapping("/sendDataToBackend")
    public String sendDataToBackend(@RequestBody String data) throws Exception {
        // Process the received data (e.g., perform some business logic)
        // For this example, we'll simply return the received data + 1.
        // 加载模型文件
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:calcite:model=D:\\article\\Backend\\src\\main\\resources\\model.json");
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

        StringBuilder resultStringBuilder = new StringBuilder();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            // Iterate through columns
            for (int i = 1; i <= columnCount; i++) {
                // Modify this part based on the actual types of your columns
                String columnValue = resultSet.getString(i);
                resultStringBuilder.append(columnValue).append(", ");
            }
            // Move to the next line for the next row
            resultStringBuilder.append("\n");
        }

        resultSet.close();
        statement.close();
        connection.close();

        return resultStringBuilder.toString();
    }

    /*
        修改csv文件路径,filePath为"{ "fp": "D:-----"}"
    */

    @PostMapping("/CJF")
    public void ChangeJsonFile(@RequestBody String filePath) throws IOException, JSONException{
        String fp = "D:\\article\\Backend\\src\\main\\resources\\model.json";
        try (Reader reader = new FileReader(fp)) {
            StringBuilder stringBuilder = new StringBuilder();
            int c;
            while ((c = reader.read()) != -1) {
                stringBuilder.append((char) c);
            }
            JSONObject jsonObject =  new JSONObject(stringBuilder.toString());

            JSONObject jsonObject1 = new JSONObject(filePath);

            // 提取 "sql" 字段的值
            String fileP = jsonObject1.getString("fp");

            // 获取 "schemas" 数组
            JSONArray schemas = jsonObject.getJSONArray("schemas");

            // 获取 "operand" 对象
            JSONObject operand = schemas.getJSONObject(0).getJSONObject("operand");

            // 修改 "directory" 的值
            operand.put("directory", fileP);

            try (FileWriter writer = new FileWriter(fp)) {
                writer.write(jsonObject.toString(2)); // 2是缩进的空格数，可以根据需要调整
            }

        }

    }

}
