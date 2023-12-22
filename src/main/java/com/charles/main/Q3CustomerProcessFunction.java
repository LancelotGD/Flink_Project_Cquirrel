package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Q3CustomerProcessFunction extends KeyedProcessFunction<Object, Payload, Payload> {
    String nextKey = "CUSTKEY";
    ValueState<Set<Payload>> aliveSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Set<Payload>> typeInformation = TypeInformation.of(new TypeHint<Set<Payload>>() {});
        ValueStateDescriptor<Set<Payload>> aliveDescriptor = new ValueStateDescriptor<>(
                "Q3CustomerProcessFunction" + "Alive", typeInformation);
        aliveSet = getRuntimeContext().getState(aliveDescriptor);
    }

    public boolean isValid(Payload payload) {
        if(payload.getValueByColumnName("C_MKTSEGMENT").equals("BUILDING")) {
            return true;
        } else {
            return false;
        }
    }

    public String getNextKey() {
        return nextKey;
    }

    public void processElement(Payload value_raw, KeyedProcessFunction<Object, Payload, Payload>.Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value() == null) {
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
        }

        if (isValid(value_raw)) {
            Payload tmp = new Payload(value_raw);
            tmp.type = "Tmp";

            Set<Payload> set = aliveSet.value();
            if("Insert".equals(value_raw.type)){
                if(set.add(tmp)){
                    value_raw.type= "Setlive";
                    value_raw.setKey(nextKey);
                    out.collect(value_raw);
                }
            }else{
                if(set.remove(tmp)){
                    value_raw.type= "SetDead";
                    value_raw.setKey(nextKey);
                    out.collect(value_raw);
                    set.remove(tmp);
                }
            }

        }
    }
}
