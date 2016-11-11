package ar.edu.itba.pod.hz.mr;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import ar.edu.itba.pod.hz.model.Data;

public class AnalphabetPerDepartmentReducerFactory implements ReducerFactory<String, Data, Double> {

	private Map<String, Double> indexPerDepartment;
	@Override
	public Reducer<Data, Double> newReducer(final String department) {
		return new Reducer<Data, Double>() {
			private int totalHabitantsOfDepartment;
			private int analphabetsInDepartment;
			

			@Override
			public void beginReduce() // una sola vez en cada instancia
			{
				totalHabitantsOfDepartment = 0;
				analphabetsInDepartment = 0;
				indexPerDepartment = new TreeMap<String,Double>();
			}

			@Override
			public void reduce(final Data value) {
				if (value.getAlfabetismo() == 1) {
					analphabetsInDepartment++;
				}
				totalHabitantsOfDepartment++;			
			}

			@Override
			public Double finalizeReduce() {
				
				indexPerDepartment.put(department, (double)analphabetsInDepartment/totalHabitantsOfDepartment);
				
				//le llega un N aca
				
//				indexPerDepartment.entrySet()
//	              				  .stream().max(new Comparator<Double>() {
//									@Override
//									public int compare(Double o1, Double o2) {
//										return (int) (o1 - o2 == 0 ? 0: Math.signum(o1 - o2));
//									}
//								})
//	              				  .map(Map.Entry::getKey)
//	              				  	;
				
				System.out.println(String.format("FinalReduce for %s = %f", department, (double)analphabetsInDepartment/totalHabitantsOfDepartment));
				return (double)analphabetsInDepartment/totalHabitantsOfDepartment;
			
			}
		};
	}
}
