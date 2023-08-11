package org.example.timeandwindow;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.function.ClickSource;
import org.example.pojo.Event;

import java.time.Duration;

public class Flink02_FlinkEmbeddedWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(5000);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        //乱序流
        ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                (event,ts) -> event.getTs()
                )
        );


        //乱序流
        ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                System.out.println(event.getTs());
                                return event.getTs();
                            }
                        }
                )
        );

        env.execute();


    }
}
