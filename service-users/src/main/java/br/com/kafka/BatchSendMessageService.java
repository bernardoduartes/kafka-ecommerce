package br.com.kafka;


import br.com.kafka.consumer.KafkaConsumer;
import br.com.kafka.producer.KafkaProducer;
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

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {

        var batchSendMessageService = new BatchSendMessageService();
        try (var consumer = new KafkaConsumer<>(
                CreateUserService.class.getSimpleName(),
                BatchSendMessageService.class.getSimpleName() + "_" + UUID.randomUUID(),
                "ECOMMERCE_SEND_MASSAGE_TO_ALL_USERS",
                batchSendMessageService::parse,
                Map.of()
        )) {
            consumer.run();
        }
    }

    private final KafkaProducer<User> userDispatcher = new KafkaProducer<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, SQLException {

        var message  = record.value();
        System.out.println("------------------------------------------");
        System.out.println("Processing new batch");
        System.out.println("Topic: + " + message.getPayload());

        if(true) throw new RuntimeException("erro que foi forçado para teste da deadletter");

        for (User user : getAllUsers()) {
            userDispatcher.sendAsync (
                    message.getPayload(),
                    user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user
            );
            System.out.println("Enviei para : " + user);
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