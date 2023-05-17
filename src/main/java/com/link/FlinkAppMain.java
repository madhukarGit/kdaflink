package com.link;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class FlinkAppMain
{
    private static Logger logger = LoggerFactory.getLogger(FlinkAppMain.class);

    private static Map<String, Set<String>> propertiesMetaMap = loadMetaMap();
    public static void main(String[] args)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> applicationPropertiesMap = initPropertiesMap(env);
        Properties sourceKinesisProperties = getProperties(applicationPropertiesMap,"KinesisSourceProperties");
        DataStream<String> inputStream = createSource(env, sourceKinesisProperties);
        inputStream.print();
        env.execute();
    }

    private static Map<String, Properties> initPropertiesMap(StreamExecutionEnvironment sEnv) throws IOException {
        Map<String, Properties> applicationPropertiesMap = null;
        if(sEnv instanceof LocalStreamEnvironment){
            applicationPropertiesMap = KinesisAnalyticsRuntime.getApplicationProperties(
                    FlinkAppMain.class.getClassLoader()
                            .getResource("application-proerties-dev.json")
                            .getPath());
            logger.info("Read properties from resource folder.");
        }
        return applicationPropertiesMap;
    }


    private static Map<String, Set<String>> loadMetaMap(){
        Map<String, Set<String>> map = new HashMap<>();
        Set<String> kinesisSourceKeySet = new HashSet<>();
        kinesisSourceKeySet.add("aws.kinesis.stream.name");
        kinesisSourceKeySet.add("AggregationEnabled");
        kinesisSourceKeySet.add("aws.region");
        kinesisSourceKeySet.add("flink.stream.initpos");
        map.put("KinesisSourceProperties",kinesisSourceKeySet);
        return map;
    }
    private static Properties getProperties(Map<String, Properties> propertiesMap, String propertyGroupName){
        Properties properties = null;
        if(propertiesMap.get("kdaProperties") != null){
            String jsonPropertiesString = propertiesMap.get("kdaProperties")
                    .getProperty("ENV");
            JSONObject jsonObject = new JSONObject(jsonPropertiesString);
            properties = new Properties();
            Set<String> keys = propertiesMetaMap.get(propertyGroupName);
            for (String key : keys){
                properties.setProperty(key, jsonObject.getString(key));
            }
        }else {
            properties = propertiesMap.get(propertyGroupName);
        }
        return properties;
    }

    public static DataStream<String> createSource(StreamExecutionEnvironment env, Properties properties){
        return env.addSource(new FlinkKinesisConsumer<>(properties.get("aws.kinesis.stream.name").toString(),
                new SimpleStringSchema(), properties))
                .name("Kinesis Source")
                .uid("kinesis_source");
    }
}
