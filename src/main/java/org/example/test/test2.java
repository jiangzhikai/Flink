package org.example.test;


/*
1。窗口种类
（1）时间窗口
       滚动窗口
       滑动窗口
       会话窗口
（2）计数窗口
        滚动窗口
        滑动窗口

 时间窗口又可以分为事件时间窗口和处理时间窗口

 又可以分为有分区窗口和无分区窗口

2.基本聚合函数
  增量聚合函数：对之前的结果以及当前的数据进行聚合
  全窗口函数：基于全部的数据进行计算

3.（1）设置水位线的延迟时间
   (2) 设置窗口关闭延时
  （3）使用侧流接受迟到的数据
**/

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.example.pojo.Event;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class test2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //kafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("flink")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("topicA")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");


        SingleOutputStreamOperator<Tuple2<String, Long>> sum = ds.map(
                        s -> Tuple2.of(s.split(" ")[0].trim(), 1L)
                )
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(
                        t -> t.f0
                ).window(
                        TumblingProcessingTimeWindows.of(Time.seconds(5))
                ).sum(1);

        sum.print();


        SinkFunction<Tuple2<String,Long>> jdbcSink = JdbcSink.sink(
                "insert into clickSum (name,click_num) values (?,?)",
                new JdbcStatementBuilder<Tuple2<String,Long>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple2<String,Long> event) throws SQLException {
                        preparedStatement.setString(1, event.f0);
                        preparedStatement.setLong(2, event.f1);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        sum.addSink(jdbcSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
