package example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple7<Integer, Long, String, String, Double, String, Integer>, Tuple6<Integer, Long, String, String, Double, String>> {
    /**
     * The state that is maintained by this process function
     */

    private static int MINUTE = 5;
    Logger LOG = LoggerFactory.getLogger(CountWithTimeoutFunction.class);

//    private ValueState<List<Map<String, Integer>>> state;
    private ValueState<List<JsonObject>> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor("countState", Types.LIST(Types.GENERIC(JsonObject.class))));
    }

    @Override
    public void processElement(Tuple7<Integer, Long, String, String, Double, String, Integer> value, Context ctx, Collector<Tuple6<Integer, Long, String, String, Double, String>> out) throws Exception {

        // retrieve the current count
        List<JsonObject> current = state.value();
        if (current == null) {
            current = new ArrayList<JsonObject>();

            ////////////////// TIMER SETTING
            // schedule the next timer 5 minutes from the current processing time
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + (60000 * MINUTE));
        }

        JsonObject jsonObject = new JsonObject();
        jsonObject.TXN_ID = value.f0;
        jsonObject.TIMESTAMP = value.f1;
        jsonObject.CARD_TYPE = value.f2;
        jsonObject.CARD_STATUS = value.f3;
        jsonObject.TXN_AMT = value.f4;
        jsonObject.CARD_NUMBER = value.f5;
        current.add(jsonObject);

        // write the state back
        state.update(current);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple6<Integer, Long, String, String, Double, String>> out) throws Exception {

        // get the state for the key that scheduled the timer
        List<JsonObject> result = state.value();

        result = sortByValue(result);

        for(int i = 0; i < Math.floor(result.size() * 95 /100); i++){
            out.collect(new Tuple6<Integer, Long, String, String, Double, String>(result.get(i).TXN_ID, result.get(i).TIMESTAMP, result.get(i).CARD_TYPE, result.get(i).CARD_STATUS, result.get(i).TXN_AMT, result.get(i).CARD_NUMBER));
        }

        state.clear();
    }

    private static List<JsonObject> sortByValue(List<JsonObject> unsortList) {

        Collections.sort(unsortList, new Comparator<JsonObject>() {
            public int compare(JsonObject o1,
                               JsonObject o2) {
                return (o1.TXN_AMT).compareTo(o2.TXN_AMT);
            }
        });

        return unsortList;
    }
}
