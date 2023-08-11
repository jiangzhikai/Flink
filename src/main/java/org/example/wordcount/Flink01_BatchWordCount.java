package org.example.wordcount;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1.准备执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.对接数据源
        DataSource<String> stringDS = env.readTextFile("test.txt");

        stringDS.flatMap(
                (String s, Collector<Tuple2<String, Long>> out) -> {
                    String[] s1 = s.split(" ");

                    for (String s2 : s1) {
                        out.collect(Tuple2.of(s2,1L));
                    }

                }
        );
    }
}
