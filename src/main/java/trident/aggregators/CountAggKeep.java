package trident.aggregators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * Not only Count aggregator but also keeps the input Fields.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class CountAggKeep implements Aggregator<CountAggKeep.State> {

    private static final long serialVersionUID = -9205017022388548713L;

    List<String> keepFields = new ArrayList<String>();

    static class State {
        long count = 0;
        Map<String, Object> fields = new HashMap<String, Object>();
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        keepFields = (List<String>) conf.get("countAggKeepFields");
    }

    @Override
    public void cleanup() {
    }

    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State val, TridentTuple tuple, TridentCollector collector) {
        val.count++;
        for (String field : keepFields)
            val.fields.put(field, tuple.getValueByField(field));
    }

    @Override
    public void complete(State val, TridentCollector collector) {
        Values values = new Values();
        values.add(val.count);
        for (String field : keepFields)
            values.add(val.fields.get(field));

        collector.emit(values);
    }

}
