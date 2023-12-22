package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //TODO 读取数据：从文件读
        String input_path = parameterTool.get("input");
        DataStreamSource<String> lineDS = env.readTextFile(input_path);
        //TODO 数据处理：拆分，转换，分组，聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOnes = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }

            }});
            KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOnes.keyBy(
                    new KeySelector<Tuple2<String, Integer>, String>() {
                        @Override
                        public String getKey(Tuple2<String, Integer> value) throws Exception {
                            return value.f0;
                        }
                    }
            );
            //TODO 聚合
            SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1);
            //TODO 数据输出

            sumDS.print();

            //TODO 启动执行环境 start 进行流处理
            env.execute();
        }
    }
