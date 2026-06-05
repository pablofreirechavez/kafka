import java.sql.*;
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DatabaseConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("group.id", "db-consumer-group-final");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("vehicle-positions"));

        String url = "jdbc:mysql://mysql-db:3306/vehicles_db";
        System.out.println("--- CONSUMIDOR LISTO Y ESPERANDO MENSAJES ---");
        try (Connection conn = DriverManager.getConnection(url, "root", "password123")) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Recibido: " + record.value());
                    try {
                        String json = record.value();
                        String id = json.split("\"vehicleId\": \"")[1].split("\"")[0];
                        double lat = Double.parseDouble(json.split("\"lat\": ")[1].split(",")[0]);
                        double lon = Double.parseDouble(json.split("\"lon\": ")[1].split(",")[0]);
                        String status = json.split("\"status\": \"")[1].split("\"")[0];
                        PreparedStatement stmt = conn.prepareStatement("INSERT INTO vehicle_positions (vehicleId, lat, lon, status) VALUES (?, ?, ?, ?)");
                        stmt.setString(1, id); stmt.setDouble(2, lat); stmt.setDouble(3, lon); stmt.setString(4, status);
                        stmt.executeUpdate();
                    } catch (Exception e) { System.out.println("Error procesando"); }
                }
            }
        } catch (SQLException e) { e.printStackTrace(); }
    }
}
