package ar.edu.itba.pod.hz.mr;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.Data;

public class DepartmentMapperFactory implements Mapper<Integer, Data, String, Data> {

	private static final long serialVersionUID = 1L;

	@Override
	public void map(final Integer keyinput, final Data valueinput, final Context<String, Data> context) {
		System.out.println(String.format("Llega KeyInput: %s con ValueInput: %s", keyinput, valueinput));

		context.emit(valueinput.getNombredepto(), valueinput);

		System.out.println(String.format("Se emite (%s, %s)", valueinput.getNombredepto(), valueinput));
	}
}