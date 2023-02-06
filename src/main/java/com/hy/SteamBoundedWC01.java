package com.hy;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SteamBoundedWC01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> sourceDS = env.readTextFile("datas");

        SingleOutputStreamOperator<Tuple2<String, Long>> WordAndOneDs = sourceDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> keyedStream = WordAndOneDs.keyBy(tuple -> tuple.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> resultDS = keyedStream.sum(1);
        resultDS.print();
        env.execute();
    }
}
