package ar.edu.itba.pod.hz.mr.query3;

import java.util.Map;
import java.util.TreeMap;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import ar.edu.itba.pod.hz.model.Data;

public class AnalphabetPerDepartmentReducerFactory implements ReducerFactory<String, Integer, Double> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Reducer<Integer, Double> newReducer(final String department) {
		return new Reducer<Integer, Double>() {
			private int totalHabitantsOfDepartment;
			private int analphabetsInDepartment;

			@Override
			public void beginReduce() // una sola vez en cada instancia
			{
				totalHabitantsOfDepartment = 0;
				analphabetsInDepartment = 0;
				// indexPerDepartment = new TreeMap<String, Double>();
			}

			@Override
			public void reduce(final Integer value) {
				if (value == 0) {
					analphabetsInDepartment++;
				}
				totalHabitantsOfDepartment++;
			}

			@Override
			public Double finalizeReduce() {
				return (double) analphabetsInDepartment / totalHabitantsOfDepartment;
			}
		};
	}
}
