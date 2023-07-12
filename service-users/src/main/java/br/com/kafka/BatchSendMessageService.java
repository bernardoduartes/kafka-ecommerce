package br.com.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {
    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users (" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200))");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {

        var batchSendMessageService = new BatchSendMessageService();
        try (var consumer = new KafkaConsumer<>(
                CreateUserService.class.getSimpleName(),
                BatchSendMessageService.class.getSimpleName() + "_" + UUID.randomUUID(),
                "SEND_MASSAGE_TO_ALL_USERS",
                batchSendMessageService::parse,
                String.class,
                Map.of()
        )) {
            consumer.run();
        }
    }

    private final KafkaProducer<User> userDispatcher = new KafkaProducer<>();

    private void parse(ConsumerRecord<String, String> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("------------------------------------------");
        System.out.println("Processing new batch");
        System.out.println("Topic: + " + record.value());

        for (User user : getAllUsers()) {
            userDispatcher.send("USER_GENERATE_READING_REPORT", user.getUuid(), user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users ").executeQuery();

        List<User> users = new ArrayList<>();
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }

        return users;
    }
}