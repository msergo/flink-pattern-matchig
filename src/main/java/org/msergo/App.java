package org.msergo;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;
import org.msergo.sources.RabbitMQCustomSource;

import java.util.List;
import java.util.Map;

public class App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setVirtualHost("/")
                .setUserName("user")
                .setPassword("bitnami")
                .setPort(5672)
                .build();

        env.setParallelism(1);

        final DataStream<String> stream = env
                .addSource(new RabbitMQCustomSource(connectionConfig, "test-exchange", "test-queue"))
                .setParallelism(1);

        Pattern<String, ?> pattern = Pattern.<String>begin("start").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.equals("AAA");
                    }
                })
                .next("middle").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.equals("BBB");
                    }
                })
                .next("end").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.equals("CCC");
                    }
                });

        PatternStream<String> patternStream = CEP.pattern(stream, pattern);
        DataStream<String> matchedStream = patternStream
                .inProcessingTime()
                .process(new PatternProcessFunction<String, String>() {
                    @Override
                    public void processMatch(Map<String, List<String>> map, Context context, Collector<String> collector) throws Exception {
                        collector.collect(map.get("start").toString());
                        collector.collect(map.get("middle").toString());
                    }
                });

        matchedStream.print();

        env.execute("DemandSupply-CEP");
    }
}
