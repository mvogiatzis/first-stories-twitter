package trident.utils;

import java.util.Comparator;
import java.util.PriorityQueue;

import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Sorts the incoming tuples on the sort field.
 *
 */
	public class FirstNAggregator extends BaseAggregator<PriorityQueue> {

        int _n;
        String _sortField;
        boolean _reverse;
        
        public FirstNAggregator(int n, String sortField, boolean reverse) {
            _n = n;
            _sortField = sortField;
            _reverse = reverse;
        }

        @Override
        public PriorityQueue init(Object batchId, TridentCollector collector) {
            return new PriorityQueue(_n, new Comparator<TridentTuple>() {
                @Override
                public int compare(TridentTuple t1, TridentTuple t2) {
                    Comparable c1 = (Comparable) t1.getValueByField(_sortField);
                    Comparable c2 = (Comparable) t2.getValueByField(_sortField);
                    int ret = c1.compareTo(c2);
                    if(_reverse) ret *= -1;
                    return ret;
                }                
            });
        }

        @Override
        public void aggregate(PriorityQueue state, TridentTuple tuple, TridentCollector collector) {
            state.add(tuple);
        }

        @Override
        public void complete(PriorityQueue val, TridentCollector collector) {
            int total = val.size();
            for(int i=0; i<_n && i < total; i++) {
                TridentTuple t = (TridentTuple) val.remove();
                collector.emit(t);
            }
        }
    } 

