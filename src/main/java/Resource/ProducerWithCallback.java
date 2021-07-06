package Resource;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
        String bootstrapServers = "localhost:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(properties);
        for (int i=0; i<10; i++){


            // Create a producer recorde
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world" +  Integer.toString(i));


            //Send Data- asynchronus
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute evry time a record is successfully sent or exception is thrown
                    if (e == null) {
                        // the record was successfull sent
                        logger.info("recived new metadata.\n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while producing", e);

                    }


                }
            });
        }

        producer.flush();
        producer.close();




    }
}
