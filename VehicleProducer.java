import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class VehicleProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        System.out.println("--- PRODUCTOR INICIANDO ENVÍO ---");
        for (int i = 0; i < 3000; i++) {
            String json = String.format("{\"vehicleId\": \"CAR-%d\", \"lat\": 0.0, \"lon\": 0.0, \"status\": \"MOVING\"}", i);
            producer.send(new ProducerRecord<>("vehicle-positions", "CAR-" + i, json));
        }
        producer.flush();
        producer.close();
        System.out.println("--- PRODUCTOR FINALIZÓ ---");
    }
}
