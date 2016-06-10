package trident.utils;

import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Sorts the incoming tuples on the sort field.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 */
public class FirstNAggregator extends BaseAggregator<PriorityQueue> {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 7519318517258264545L;

    /** The _n. */
    int _n;

    /** The _sort field. */
    String _sortField;

    /** The _reverse. */
    boolean _reverse;

    /**
     * Instantiates a new first n aggregator.
     *
     * @param n
     *            the n
     * @param sortField
     *            the sort field
     * @param reverse
     *            the reverse
     */
    public FirstNAggregator(int n, String sortField, boolean reverse) {
        _n = n;
        _sortField = sortField;
        _reverse = reverse;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Aggregator#init(java.lang.Object, org.apache.storm.trident.operation.TridentCollector)
     */
    @Override
    public PriorityQueue init(Object batchId, TridentCollector collector) {
        return new PriorityQueue(_n, new Comparator<TridentTuple>() {
            @Override
            public int compare(TridentTuple t1, TridentTuple t2) {
                Comparable c1 = (Comparable) t1.getValueByField(_sortField);
                Comparable c2 = (Comparable) t2.getValueByField(_sortField);
                int ret = c1.compareTo(c2);
                if (_reverse)
                    ret *= -1;
                return ret;
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Aggregator#aggregate(java.lang.Object, org.apache.storm.trident.tuple.TridentTuple,
     * org.apache.storm.trident.operation.TridentCollector)
     */
    @Override
    public void aggregate(PriorityQueue state, TridentTuple tuple, TridentCollector collector) {
        state.add(tuple);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Aggregator#complete(java.lang.Object, org.apache.storm.trident.operation.TridentCollector)
     */
    @Override
    public void complete(PriorityQueue val, TridentCollector collector) {
        int total = val.size();
        for (int i = 0; i < _n && i < total; i++) {
            TridentTuple t = (TridentTuple) val.remove();
            collector.emit(t);
        }
    }
}
