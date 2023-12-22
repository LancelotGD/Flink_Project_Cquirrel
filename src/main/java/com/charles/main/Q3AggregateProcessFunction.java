package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Q3AggregateProcessFunction extends KeyedProcessFunction<Object, Payload, Payload> {
    ValueState<Double> preValue;

    List<String> outputNames= Arrays.asList("ORDERKEY","O_ORDERDATE","O_SHIPPRIORITY");

    String aggregateName = "revenue";

    String nextKey = "ORDERKEY";

    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> preValueDescriptor = new ValueStateDescriptor<>(
                "Q3AggregateProcessFunction" + "PreValue", TypeInformation.of(Double.class));
        preValue = getRuntimeContext().getState(preValueDescriptor);
    }

    @Override
    public void processElement(Payload payload, KeyedProcessFunction<Object, Payload, Payload>.Context context, Collector<Payload> out) throws Exception {
        if(preValue.value() == null) {
            preValue.update(0.0);
        }

        Double delta = (Double)payload.getValueByColumnName("L_EXTENDEDPRICE")*(1.0-(Double)payload.getValueByColumnName("L_DISCOUNT"));

        double newValue = 0.0;

        if(payload.type.equals("Add")){
            newValue = preValue.value() + delta;
        }else if(payload.type.equals("Sub")){
            newValue = preValue.value() - delta;
        }

        preValue.update(newValue);
        List<Object> attribute_value = new ArrayList<>();
        List<String> attribute_name = new ArrayList<>();

        for(String attrName:outputNames){
            attribute_value.add(payload.getValueByColumnName(attrName));
            attribute_name.add(attrName);
        }

        attribute_value.add(newValue);
        attribute_name.add(aggregateName);

        payload.attribute_name = attribute_name;
        payload.attribute_value = attribute_value;
        payload.setKey(nextKey);
        payload.type = "Output";
        System.out.println("aggregate: "+payload.toString());
        out.collect(payload);
    }
}