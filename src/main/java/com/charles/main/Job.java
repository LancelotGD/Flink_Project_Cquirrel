package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class Job {
    public static final OutputTag<Payload> lineitemTag = new OutputTag<Payload>("lineitem"){};
    public static final OutputTag<Payload> ordersTag = new OutputTag<Payload>("orders"){};
    public static final OutputTag<Payload> customerTag = new OutputTag<Payload>("customer"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

//        String inputPath = "D:/java/ideaProjects/flink_ip/resources/input_data_all.csv";
//        String outputPath = "D:/java/ideaProjects/flink_ip/resources/output_data_all.csv";
        String inputPath = parameterTool.get("input");
        String outputPath = parameterTool.get("output");

        DataStreamSource<String> data = env.readTextFile(inputPath).setParallelism(1);
        SingleOutputStreamOperator<Payload> inputStream = getStream(data);
        //get side output
        DataStream<Payload> orders = inputStream.getSideOutput(ordersTag);
        DataStream<Payload> lineitem = inputStream.getSideOutput(lineitemTag);
        DataStream<Payload> customer = inputStream.getSideOutput(customerTag);

        DataStream<Payload> customerS = customer.keyBy(i -> i.key)
                .process(new Q3CustomerProcessFunction());
        DataStream<Payload> ordersS = customerS.connect(orders)
                .keyBy(i -> i.key, i -> i.key)
                .process(new Q3OrdersProcessFunction());
        DataStream<Payload> lineitemS = ordersS.connect(lineitem)
                .keyBy(i -> i.key, i -> i.key)
                .process(new Q3LineitemProcessFunction());
        DataStream<Payload> result = lineitemS.keyBy(i -> i.key)
                .process(new Q3AggregateProcessFunction());
        DataStreamSink<Payload> output = result.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");
    }

    private static SingleOutputStreamOperator<Payload> getStream(DataStreamSource<String> data) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        SingleOutputStreamOperator<Payload> restDS = data.process(new ProcessFunction<String, Payload>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Payload> out) throws Exception {
                String header = value.substring(0,3);
                String[] cells = value.substring(3).split("\\|");
                String relation = "";
                String action = "";
                Long cnt = 0L;
                switch (header) {
                    case "+LI":
                        action = "Insert";
                        ctx.output(lineitemTag, new Payload(action, Long.valueOf(cells[0]),
                                new ArrayList<>(Arrays.asList("L_SHIPDATE", "LINENUMBER", "ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT")),
                                new ArrayList<>(Arrays.asList(format.parse(cells[10]), Integer.valueOf(cells[3]), Long.valueOf(cells[0]), Double.valueOf(cells[5]), Double.valueOf(cells[6])))
                                ));
                        break;
                    case "-LI":
                        action = "Delete";
                        ctx.output(lineitemTag, new Payload(action, Long.valueOf(cells[0]),
                                new ArrayList<>(Arrays.asList("L_SHIPDATE", "LINENUMBER", "ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT")),
                                new ArrayList<>(Arrays.asList(format.parse(cells[10]), Integer.valueOf(cells[3]), Long.valueOf(cells[0]), Double.valueOf(cells[5]), Double.valueOf(cells[6])))
                        ));
                        break;
                    case "+OR":
                        action = "Insert";
                        ctx.output(ordersTag, new Payload(action, Long.valueOf(cells[1]),
                                new ArrayList<>(Arrays.asList("CUSTKEY","ORDERKEY","O_ORDERDATE","O_SHIPPRIORITY")),
                                new ArrayList<>(Arrays.asList(Long.valueOf(cells[1]), Long.valueOf(cells[0]), format.parse(cells[4]),Integer.valueOf(cells[7])))
                        ));
                        break;
                    case "-OR":
                        action = "Delete";
                        ctx.output(ordersTag,
                                new Payload(action, Long.valueOf(cells[1]),
                                        new ArrayList<>(Arrays.asList("CUSTKEY","ORDERKEY","O_ORDERDATE","O_SHIPPRIORITY")),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(cells[1]), Long.valueOf(cells[0]), format.parse(cells[4]),Integer.valueOf(cells[7])))));
                        break;
                    case "+CU":
                        action = "Insert";
                        ctx.output(customerTag,
                                new Payload(action, Long.valueOf(cells[0]),
                                        new ArrayList<>(Arrays.asList("CUSTKEY","C_MKTSEGMENT")),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(cells[0]), cells[6]))));
                        break;
                    case "-CU":
                        action = "Delete";
                        ctx.output(customerTag,
                                new Payload(action, Long.valueOf(cells[0]),
                                        new ArrayList<>(Arrays.asList("CUSTKEY","C_MKTSEGMENT")),
                                        new ArrayList<>(Arrays.asList(Long.valueOf(cells[0]), cells[6]))));
                        break;
                }
            }
        }).setParallelism(1);

        return restDS;
    }
}