package trident.functions;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import entities.Tweet;

	public class ExpandList extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			@SuppressWarnings("rawtypes")
			List<Tweet> l = (List<Tweet>) tuple.getValue(0);
			if (l != null) {
				for (Tweet o : l) {
					collector.emit(new Values(o, o.getID()));
				}
			}
		}

	}


