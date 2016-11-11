package ar.edu.itba.pod.hz.mr.query2;

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import ar.edu.itba.pod.hz.model.Data;

public class AverageHabitantsPerHouseReducerFactory implements ReducerFactory<Integer, Data, Double> {
	private static final long serialVersionUID = 1L;

	@Override
	public Reducer<Data, Double> newReducer(final Integer typeOfHouse) {
		return new Reducer<Data, Double>() {
			private Set<Integer> hogarids;
			private int count;

			@Override
			public void beginReduce() // una sola vez en cada instancia
			{
				hogarids = new HashSet<Integer>();
				count = 0;
			}

			@Override
			public void reduce(final Data value) {
				hogarids.add(value.getHogarid());
				count++;
			}

			@Override
			public Double finalizeReduce() {
				return ((double)count/hogarids.size());
			}
		};
	}
}
