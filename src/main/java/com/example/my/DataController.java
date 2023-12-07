package com.example.my;

import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.json.JSONArray;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonRoot;
import org.apache.calcite.schema.SchemaPlus;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;

import org.json.JSONObject;

import org.json.JSONException;
import org.springframework.web.multipart.MultipartFile;

import java.util.EnumSet;

@RestController
@RequestMapping("/api")
public class DataController {

    private final String fpath = "D:\\article\\Backend\\src\\main\\resources";

    @PostMapping("/sendDataToBackend")
    public String sendDataToBackend(@RequestBody String data) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        String temp = "jdbc:calcite:model="+ this.fpath+"\\model.json";
        Connection connection = DriverManager.getConnection(temp);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        Statement statement = connection.createStatement();

        JSONObject jsonObject = new JSONObject(data);

        // 提取 "sql" 字段的值
        String sqlQuery = jsonObject.getString("sql");

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

//    保存文件
    @PostMapping("/addDataset")
    public ResponseEntity<String> handleFileUpload( @RequestParam("file") MultipartFile file){
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body("文件不能为空");
        }
        // 设置保存文件的目录，可以根据实际需求修改
        String uploadDir = this.fpath + "\\TempFile";

        try {
            // 获取文件的字节数组并保存到本地
            byte[] bytes = file.getBytes();
            Path filePath = Paths.get(uploadDir, file.getOriginalFilename());
            Files.write(filePath, bytes);

            // 返回成功的响应
            return ResponseEntity.ok().body("{ \"ack\": true }");
        } catch (IOException e) {
            e.printStackTrace();
            // 处理保存文件时的异常
            return ResponseEntity.status(500).body("{ \"ack\": false, \"error\": \"文件保存失败\" }");
        }
    }

    /*
        修改数据文件
    */

    @PostMapping("/changeJsonFile")
    public void changeJsonFile(@RequestBody String fileName) {
        // 输入文件夹路径和输出文件夹路径，根据实际情况修改
        String inputFolderPath = this.fpath + "\\TempFile";
        String outputFolderPath = this.fpath + "\\csvFile";

        try {
            // 在输入文件夹中找到与给定文件名相同的文件
            Files.walkFileTree(Path.of(inputFolderPath), EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            String fileWithoutExtension = removeFileExtension(file.getFileName().toString());
                            if (fileWithoutExtension.equals(fileName)) {
                                // 清空输出文件夹
                                FileUtils.cleanDirectory(new File(outputFolderPath));
                                // 复制文件到输出文件夹
                                Files.copy(file, Paths.get(outputFolderPath, file.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
                                // 找到文件后可以终止遍历
                                return FileVisitResult.TERMINATE;
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private String removeFileExtension(String fileName) {
        int lastDotIndex = fileName.lastIndexOf(".");
        if (lastDotIndex != -1) {
            return fileName.substring(0, lastDotIndex);
        }
        return fileName;
    }

}
