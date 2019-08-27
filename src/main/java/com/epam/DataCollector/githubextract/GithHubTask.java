package com.epam.DataCollector.githubextract;

import com.epam.DataCollector.config.IKafkaConstants;
import com.epam.DataCollector.config.ProducerCreator;
import kafka.utils.Json;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class GithHubTask {

    private static String token="";

    public static void curlRequest(){


        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION,"token "+token);

        HttpEntity<String> entity = new HttpEntity<>("body", headers);
        String gitUrl = "https://api.github.com/search/repositories?q=language:java,scala&location=hyderabad&sort=stars&order=desc&page=2&per_page=1000";
        ResponseEntity<String> response=restTemplate.exchange(gitUrl, HttpMethod.GET, entity, String.class);
       // System.out.println(response.getBody());
        JSONObject json=new JSONObject(response.getBody());
        JSONArray items=json.getJSONArray("items");
        Producer<Long, String> producer = ProducerCreator.createProducer();
        System.out.println(items.length());
        items.forEach(item->{
            System.out.println("");
            JSONObject obj=new JSONObject(item.toString());
            String name=(String)obj.get("name");
            System.out.println(name);

            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME_USER, name);
            try{
            RecordMetadata metadata = producer.send(record).get();
        }
            catch (ExecutionException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
          /* String gitUrlFinal
                    = "https://api.github.com/users/"+name;
            ResponseEntity<String> responseuser=restTemplate.exchange(gitUrl, HttpMethod.GET, entity, String.class);
            System.out.println(responseuser.getBody());
            JSONObject email= new JSONObject(responseuser.getBody());*/

        });
        System.out.println(json.get("total_count"));



    }

    public static void main(String [] args){
        curlRequest();;
    }



}
