package trident.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;

public class UpdateFreqs implements QueryFunction<FreqsState, Object> {

	private static final Logger LOG = Logger.getLogger(UpdateFreqs.class);

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Object> batchRetrieve(FreqsState state, List<TridentTuple> args) {
		List<Object> ret = new ArrayList<Object>();

		for (TridentTuple input : args) {
			// ret.add(state.getURLs(domain.getName()));
		}

		return ret;
	}

	@Override
	public void execute(TridentTuple tuple, Object result,
			TridentCollector collector) {

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
