package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Q3OrdersProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload>{

    public String nextKey = "ORDERKEY";

    public ValueState<Integer> aliveCount;
    public ValueState<Set<Payload>> aliveSet;

    public ValueState<Payload> alivePayload;
    public SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
                "Q3OrdersProcessFunction" + "Alive Count", Integer.class);
        aliveCount = getRuntimeContext().getState(countDescriptor);
        TypeInformation<Set<Payload>> typeInformation = TypeInformation.of(new TypeHint<Set<Payload>>() {});

        ValueStateDescriptor<Set<Payload>> aliveSetDescriptor = new ValueStateDescriptor<>(
                "Q3OrdersProcessFunction" + "Alive Set", typeInformation);
        aliveSet = getRuntimeContext().getState(aliveSetDescriptor);

        ValueStateDescriptor<Payload> alivePayloadDescriptor = new ValueStateDescriptor<Payload>(
                "Q3OrdersProcessFunction" + "Alive Payload", Payload.class);

        alivePayload = getRuntimeContext().getState(alivePayloadDescriptor);
    }

    @Override
    public void processElement1(Payload payload, KeyedCoProcessFunction<Object, Payload, Payload, Payload>.Context context, Collector<Payload> out) throws Exception {
        if(aliveSet.value() == null) {
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        if(payload.type.equals("Setlive")){
            Set<Payload> set = aliveSet.value();
            alivePayload.update(payload);
            aliveCount.update(aliveCount.value() + 1);

            for (Payload connectPayload : set){
                collectPayload(payload, connectPayload, out);
            }


        }else{
            aliveCount.update(aliveCount.value() - 1);

            Set<Payload> set = aliveSet.value();

            for (Payload connectPayload : set){
                collectPayload(payload, connectPayload, out);
            }
            alivePayload.update(null);alivePayload.update(null);
        }
    }

    @Override
    public void processElement2(Payload payload, KeyedCoProcessFunction<Object, Payload, Payload, Payload>.Context context, Collector<Payload> out) throws Exception {
        if (aliveSet.value() == null) {
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        Payload tmp = new Payload(payload);
        tmp.type = "Tmp";
        tmp.key = 0;

        if (isValid(payload)) {
            if(payload.type.equals("Insert")) {
                if(aliveCount.value() == 1) {
                    if(aliveSet.value().add(tmp)) {

                        payload.type = "Setlive";
                        collectPayload(payload, alivePayload.value(), out);
                    }
                }else{
                    aliveSet.value().add(tmp);
                }
            }
        }else if(payload.type.equals("Delete")){
            if(aliveCount.value() == 1) {
                if(aliveSet.value().contains(tmp)) {
                    aliveSet.value().remove(tmp);
                    payload.type = "SetDead";
                    collectPayload(payload, alivePayload.value(), out);
                }
            }else{
                aliveSet.value().remove(tmp);
            }
        }
    }



    public boolean isValid(Payload value) throws ParseException {
        Date date = format.parse("1995-03-15");
        return ((Date) value.getValueByColumnName("O_ORDERDATE")).compareTo(date) < 0;
    }

    public void collectPayload(Payload previousPaylod, Payload connectPayload, Collector<Payload> out) throws Exception {
        Payload tmp = new Payload(previousPaylod);

        Set<Payload> set = aliveSet.value();
        if(connectPayload != null) {
            for(int i = 0;i < connectPayload.attribute_name.size();i++){
                if(!tmp.attribute_value.contains(connectPayload.attribute_name.get(i))){
                    tmp.attribute_name.add(connectPayload.attribute_name.get(i));
                    tmp.attribute_value.add(connectPayload.attribute_value.get(i));
                }
            }
        }

        tmp.setKey(nextKey);
        out.collect(tmp);
    }
}