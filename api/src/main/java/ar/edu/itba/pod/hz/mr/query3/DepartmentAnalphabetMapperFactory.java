package ar.edu.itba.pod.hz.mr.query3;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.Data;
import ar.edu.itba.pod.hz.model.ProvinceDepartmentTuple;

public class DepartmentAnalphabetMapperFactory implements Mapper<Integer, Data, ProvinceDepartmentTuple, Integer> {

	private static final long serialVersionUID = 1L;

	@Override
	public void map(final Integer keyinput, final Data valueinput,
			final Context<ProvinceDepartmentTuple, Integer> context) {
		context.emit(new ProvinceDepartmentTuple(valueinput.getNombreprov(), valueinput.getNombredepto()),
				valueinput.getAlfabetismo());
	}
}