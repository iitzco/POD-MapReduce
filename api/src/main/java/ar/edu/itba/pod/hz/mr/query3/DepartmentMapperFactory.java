package ar.edu.itba.pod.hz.mr.query3;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.Data;

public class DepartmentMapperFactory implements Mapper<Integer, Data, String, Integer> {

	private static final long serialVersionUID = 1L;

	@Override
	public void map(final Integer keyinput, final Data valueinput, final Context<String, Integer> context) {
		context.emit(valueinput.getNombredepto(), valueinput.getAlfabetismo());
	}
}