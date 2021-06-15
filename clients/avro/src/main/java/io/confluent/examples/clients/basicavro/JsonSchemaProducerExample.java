package io.confluent.examples.clients.basicavro;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class JsonSchemaProducerExample {

    private static final Properties props = new Properties();
    private static String configFile;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        if (args.length < 1) {
          // Backwards compatibility, assume localhost
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
          props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8091,http://localhost:9081");
        } else {
          // Load properties from a local configuration file
          // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
          // to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
          // Documentation at https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/java.html
          configFile = args[0];
          if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
          } else {
            try (InputStream inputStream = new FileInputStream(configFile)) {
              props.load(inputStream);
            }
          }
        }

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", "fred:letmein");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);

        try (Producer<String, User> producer = new KafkaProducer<String, User>(props)) {

            String topic = "testjsonschema";
            String key = "testkey";
            User user = new User("John", "Doe", (short) 33);

            ProducerRecord<String, User> record
                    = new ProducerRecord<String, User>(topic, key, user);
            producer.send(record).get();
            producer.close();

            producer.flush();
            System.out.printf("Successfully produced 10 messages to a topic called %s%n", topic);

        } catch (final SerializationException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

}

