package com.epam.DataCollector.githubextract;

import com.epam.DataCollector.config.ConsumerCreator;
import com.epam.DataCollector.config.IKafkaConstants;
import com.epam.DataCollector.config.ProducerCreator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class GithHubTaskConsumer {
    private static String token="";
    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(IKafkaConstants.TOPIC_NAME_USER);
        Producer<Long, String> producer = ProducerCreator.createProducer();
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            //print each record.
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            headers.add(HttpHeaders.AUTHORIZATION,"token "+token);

            HttpEntity<String> entity = new HttpEntity<>("body", headers);


            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
                try{
                 String gitUrlFinal
                    = "https://api.github.com/users/"+record.value();
                System.out.println(gitUrlFinal);
            ResponseEntity<String> responseuser=restTemplate.exchange(gitUrlFinal, HttpMethod.GET, entity, String.class);
            System.out.println(responseuser.getBody());
            JSONObject email= new JSONObject(responseuser.getBody());

                ProducerRecord<Long, String> recordInsert = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME_GITUSERDetails, responseuser.getBody());

                    RecordMetadata metadata = producer.send(recordInsert).get();
                }
                catch (ExecutionException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                } catch (InterruptedException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                }
                catch(Exception e){
                    System.out.println("Exception "+e);
                }
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }



        consumer.close();
    }

    public static void main(String [] args){

        runConsumer();

    }

}
