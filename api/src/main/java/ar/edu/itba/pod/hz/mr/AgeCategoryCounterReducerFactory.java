package ar.edu.itba.pod.hz.mr;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class AgeCategoryCounterReducerFactory implements ReducerFactory<String, Integer, Integer> {
	private static final long serialVersionUID = 7760070699178320492L;

	@Override
	public Reducer<Integer, Integer> newReducer(final String category) {
		return new Reducer<Integer, Integer>() {
			private int count;

			@Override
			public void beginReduce() // una sola vez en cada instancia
			{
				count = 0;
			}

			@Override
			public void reduce(final Integer value) {
				count++;
			}

			@Override
			public Integer finalizeReduce() {
//				System.out.println(String.format("FinalReduce for %s = %s", category, count));
				return count;
			}
		};
	}
}
