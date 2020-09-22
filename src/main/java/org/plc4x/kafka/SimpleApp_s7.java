package org.plc4x.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.plc4x.java.PlcDriverManager;
import org.apache.plc4x.java.scraper.config.triggeredscraper.JobConfigurationTriggeredImplBuilder;
import org.apache.plc4x.java.scraper.config.triggeredscraper.ScraperConfigurationTriggeredImpl;
import org.apache.plc4x.java.scraper.config.triggeredscraper.ScraperConfigurationTriggeredImplBuilder;
import org.apache.plc4x.java.scraper.exception.ScraperException;
import org.apache.plc4x.java.scraper.triggeredscraper.TriggeredScraperImpl;
import org.apache.plc4x.java.scraper.triggeredscraper.triggerhandler.collector.TriggerCollector;
import org.apache.plc4x.java.scraper.triggeredscraper.triggerhandler.collector.TriggerCollectorImpl;
import org.apache.plc4x.java.utils.connectionpool.PooledPlcDriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;


public class SimpleApp_s7 {



    private static final Logger logger = LoggerFactory.getLogger(SimpleApp_s7.class);

    static String[] S7_1500_addresses;
    static String[] S7_300_addresses;
    static String[] S7_addresses;
    static HashMap<String, String> source_ip_s7;
    static HashMap<String, String> source_model_s7;


    public static void main(String[] args) {

        // Create addresses by model
        S7_1500_addresses = createAddressesArray_s1500();
        S7_300_addresses = createAddressesArray_s300();

        // Generate all the S7 addresses
        S7_addresses = createAddressesArray_S7();

        // Initializate HashMaps
        source_ip_s7 = generateSourceIp_s7();
        source_model_s7 = generateSourceModel_s7();

        // Create Kafka Producer for S7
        KafkaProducer<String, String> producer = new Producer().getProducer();


        // Scrapper Configuration
        ScraperConfigurationTriggeredImplBuilder scrapperBuilderS7 = new ScraperConfigurationTriggeredImplBuilder();


        // Add S7 sources (at least one)

        for(int i=0; i<S7_addresses.length; i++){
            scrapperBuilderS7.addSource("S7_SourcePLC" + i, S7_addresses[i]);
        }


        // In order to configure a job we have to get an instance of a JobConfigurationTriggeredImplBuilder.
        JobConfigurationTriggeredImplBuilder S7_jobBuilder = scrapperBuilderS7.job("S7_Job","(SCHEDULED,500)");


        // Assign sources to a job
        for(int i=0; i<S7_addresses.length; i++) {
            S7_jobBuilder.source("S7_SourcePLC" + i);
        }


        // S7 1500 fields
        // So the last thing we need to configure our first Scraper job, is to add a few fields for it to collect.

        S7_jobBuilder.field("I0", "%I0:BYTE");
        S7_jobBuilder.field("I1", "%I1:BYTE");
        S7_jobBuilder.field("I2", "%I2:BYTE");
        S7_jobBuilder.field("I3", "%I3:BYTE");

        S7_jobBuilder.field("Q0", "%Q0:BYTE");
        S7_jobBuilder.field("Q1", "%Q1:BYTE");
        S7_jobBuilder.field("Q2", "%Q2:BYTE");
        S7_jobBuilder.field("Q3", "%Q3:BYTE");

        S7_jobBuilder.build();
        ScraperConfigurationTriggeredImpl S7_scraperConfig = scrapperBuilderS7.build();


        // To RUN the S7_Scrapper -----------------
        try {

            // Create a new PooledPlcDriverManager
            //PlcDriverManager S7_plcDriverManager = new PooledPlcDriverManager();
            PlcDriverManager S7_plcDriverManager = null;
            try {
                S7_plcDriverManager = new PooledPlcDriverManager();
//                S7_plcDriverManager = new PooledPlcDriverManager(pooledPlcConnectionFactory -> {
//                    GenericKeyedObjectPoolConfig<PlcConnection> poolConfig = new GenericKeyedObjectPoolConfig<>();
//                    // This makes it robust for PLCs which allow only one connection
//                    poolConfig.setMaxTotalPerKey(1);
//                    // When the connecton is in use and is requested block some time until a
//                    // NoSuchElementException is thrown
//                    poolConfig.setBlockWhenExhausted(true);
//                    poolConfig.setMaxWaitMillis(1_000);
//                    return new GenericKeyedObjectPool<>(pooledPlcConnectionFactory, poolConfig);
//                });
                logger.info("Connection is successful");
            } catch (Exception e) {
                logger.error("The connection has failed: " + e);
            }


            // Trigger Collector
            TriggerCollector S7_triggerCollector = new TriggerCollectorImpl(S7_plcDriverManager);

            // Messages counter
            AtomicInteger messagesCounter = new AtomicInteger();

            // Configure the scraper, by binding a Scraper Configuration, a ResultHandler and a TriggerCollector together
            TriggeredScraperImpl S7_scraper = new TriggeredScraperImpl(S7_scraperConfig, (jobName, sourceName, results) -> {
                LinkedList<Object> S7_results = new LinkedList<>();

                messagesCounter.getAndIncrement();

                S7_results.add(jobName);
                S7_results.add(sourceName);
                S7_results.add(results);

                logger.info("Array: " + String.valueOf(S7_results));
                //logger.info("MESSAGE number: " + messagesCounter);

                // Producer topics routing
                String topic = "s7" + S7_results.get(1).toString().substring(S7_results.get(1).toString().indexOf("S7_SourcePLC") + 9, S7_results.get(1).toString().length());
                String key = parseKey_S7("s7");
                String value = parseValue_S7(S7_results.getLast().toString(), S7_results.get(1).toString());
                logger.info("------- PARSED VALUE -------------------------------- " + value);

                // Create Kafka Producer
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

                // Send Data to Kafka - asynchronous
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            logger.info("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing", e);
                        }
                    }


                });

            }, S7_triggerCollector);


            S7_scraper.start();
            S7_triggerCollector.start();


        } catch (ScraperException e) {
                logger.error("Error starting the scraper (S7_scrapper)", e);
            }

    }




    private static String parseKey_S7(String kValue){
        String parsedString = kValue.replace("s7", "{\"protocol\":\"s7\"}");
        return parsedString;
    }

    private static String parseValue_S7(String vValue, String pSourceName){
        String partialParsedString = vValue.replace("=[", ":\"").replace("]", "\"")
                .replaceAll("Q0", "\"Q0\"").replaceAll("Q1","\"Q1\"")
                .replaceAll("Q2", "\"Q2\"").replaceAll("Q3", "\"Q3\"")
                .replaceAll("I0", "\"I0\"").replaceAll("I1", "\"I1\"")
                .replaceAll("I2", "\"I2\"").replaceAll("I3", "\"I3\"");

        TemplateJSON template = new TemplateJSON();
        String s7_parsed = template.fillTemplate(source_model_s7.get(pSourceName),pSourceName, source_ip_s7.get(pSourceName)
                ,getTimestamp(),partialParsedString);

        return s7_parsed;
    }


    static Timestamp getTimestamp(){
        Calendar calendar = Calendar.getInstance();
        Timestamp ourJavaTimestampObject = new Timestamp(calendar.getTime().getTime());

        return ourJavaTimestampObject;
    }





    // -----   Create 2 Maps (To pass the parameters to the TemplateJSON) ----------------------------
    // 1- Relations {sourceName - IP} for the s7
    static HashMap<String,String> generateSourceIp_s7(){
        source_ip_s7 = new HashMap<String, String>();

        for(int i=0; i<S7_addresses.length; i++){
            source_ip_s7.put("S7_SourcePLC" + i,S7_addresses[i].substring(5));
        }
        return source_ip_s7;
    }

    // 2- Relations {sourceName - model} for the s7
    static HashMap<String,String> generateSourceModel_s7(){
        source_model_s7 = new HashMap<String, String>();

        for(int i=0; i<S7_1500_addresses.length; i++){
            source_model_s7.put("S7_SourcePLC" + i,"s1500");
        }
        for(int j=S7_1500_addresses.length, i=0; i<S7_300_addresses.length; i++){
            source_model_s7.put("S7_SourcePLC" +(j+i),"s300");
        }
        return source_model_s7;
    }



    public static String[] createAddressesArray_S7(){
        // Create the Array
//        String[] s7Adresses = {"s7://10.172.19.120"};
        String[] s7Adresses = {"s7://10.172.19.120","s7://10.172.19.119","s7://10.172.19.118","s7://10.172.19.117",
                "s7://10.172.19.115","s7://10.172.19.114","s7://10.172.19.113","s7://10.172.19.112",
                "s7://10.172.19.111","s7://10.172.19.110","s7://10.172.19.109","s7://10.172.19.108",
                "s7://10.172.19.107","s7://10.172.19.106","s7://10.172.19.105","s7://10.172.19.104",
                "s7://10.172.19.103","s7://10.172.19.102","s7://10.172.19.101","s7://10.172.19.128", // S300 big room (only last IP)
                "s7://10.172.19.123","s7://10.172.19.124","s7://10.172.19.125","s7://10.172.19.126", // S300 big room
                "s7://10.172.19.121","s7://10.172.19.127",                                           // S300 big room
                "s7://10.172.19.129","s7://10.172.19.130","s7://10.172.19.131","s7://10.172.19.132", // S300 small room
                "s7://10.172.19.133","s7://10.172.19.134","s7://10.172.19.135","s7://10.172.19.136", // S300 small room
                "s7://10.172.19.137","s7://10.172.19.138","s7://10.172.19.139","s7://10.172.19.122"};// S300 small room
        return s7Adresses;
    }


    public static String[] createAddressesArray_s1500(){
        String[] s7_1500_Adresses = {"s7://10.172.19.120","s7://10.172.19.119","s7://10.172.19.118","s7://10.172.19.117",
                "s7://10.172.19.115","s7://10.172.19.114","s7://10.172.19.113","s7://10.172.19.112",
                "s7://10.172.19.111","s7://10.172.19.110","s7://10.172.19.109","s7://10.172.19.108",
                "s7://10.172.19.107","s7://10.172.19.106","s7://10.172.19.105","s7://10.172.19.104",
                "s7://10.172.19.103","s7://10.172.19.102","s7://10.172.19.101"};
        return s7_1500_Adresses;
    }

    public static String[] createAddressesArray_s300(){
        String[] s7_300_Adresses = {"s7://10.172.19.128", "s7://10.172.19.123","s7://10.172.19.124",  // S300 big room
                "s7://10.172.19.125","s7://10.172.19.126", "s7://10.172.19.121","s7://10.172.19.127", // S300 big room
                "s7://10.172.19.129","s7://10.172.19.130","s7://10.172.19.131","s7://10.172.19.132",  // S300 small room
                "s7://10.172.19.133","s7://10.172.19.134","s7://10.172.19.135","s7://10.172.19.136",  // S300 small room
                "s7://10.172.19.137","s7://10.172.19.138","s7://10.172.19.139","s7://10.172.19.122"}; //S300 small room
        return s7_300_Adresses;
    }

}
