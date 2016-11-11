package ar.edu.itba.pod.hz.mr;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import ar.edu.itba.pod.hz.model.Data;

public class AverageHabitantsPerHouseReducerFactory implements ReducerFactory<Integer, Data, Double> {
	private static final long serialVersionUID = 1L;
	private Map<Integer, Integer> quantityPerHouse;
	@Override
	public Reducer<Data, Double> newReducer(final Integer typeOfHouse) {
		return new Reducer<Data, Double>() {
			private int count;
			private int sum;

			@Override
			public void beginReduce() // una sola vez en cada instancia
			{
				quantityPerHouse = new HashMap<Integer,Integer>();
				sum = 0;
				count = 0;
			}

			@Override
			public void reduce(final Data value) {
				if (!quantityPerHouse.containsKey(value.getHogarid())) {
					quantityPerHouse.put(value.getHogarid(), 1);
				} else {
					quantityPerHouse.put(value.getHogarid(), quantityPerHouse.get(value.getHogarid())+1);
				}
				count ++;
			}

			@Override
			public Double finalizeReduce() {
				for (Integer habitants : quantityPerHouse.values()) {
					sum += habitants;
				}
				System.out.println(String.format("FinalReduce for %s = %f", typeOfHouse, (double)sum/count));
				return (double) (sum/count);
			}
		};
	}
}
