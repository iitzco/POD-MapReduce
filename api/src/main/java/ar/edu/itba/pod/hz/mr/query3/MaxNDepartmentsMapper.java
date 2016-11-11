package ar.edu.itba.pod.hz.mr.query3;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.DepartmentValueTuple;

public class MaxNDepartmentsMapper implements Mapper<String, Double, Integer, DepartmentValueTuple> {

	private static final long serialVersionUID = 1L;

	private int N;

	public MaxNDepartmentsMapper(int N) {
		this.N = N;
	}

	@Override
	public void map(final String keyinput, final Double valueinput,
			final Context<Integer, DepartmentValueTuple> context) {
		context.emit(N, new DepartmentValueTuple(keyinput, valueinput));
	}
}
