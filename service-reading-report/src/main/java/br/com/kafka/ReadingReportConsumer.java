package br.com.kafka;



import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ReadingReportConsumer {

    private static final Path SOURCE = new File("service-reading-report/src/main/resources/report.txt").toPath();

    public static void main(String[] args) {

        var fraudDetectorService = new ReadingReportConsumer();
        try (var consumer = new KafkaConsumer<User>(
                ReadingReportConsumer.class.getSimpleName(),
                ReadingReportConsumer.class.getSimpleName() + "_" + UUID.randomUUID(),
                "USER_GENERATE_READING_REPORT",
                fraudDetectorService::parse,
                User.class,
                Map.of()
        )) {
            consumer.run();
        }
    }
    
    void parse(ConsumerRecord<String, User> record) throws IOException {
        System.out.println("------------------------------------------");
        System.out.println("Processing report for " + record.value());
        System.out.println("Key: " + record.key());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        
        var user = record.value();
        var target = new File("service-reading-report/" + user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File Created" + target.getAbsolutePath());
    }
}