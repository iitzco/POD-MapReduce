package ar.edu.itba.pod.hz.model.query4;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.Data;

public class DepartmentUnitMapperFactory implements Mapper<Integer, Data, String, Integer> {
	private static final long serialVersionUID = -3713325164465665033L;

	private String prov;

	public DepartmentUnitMapperFactory(String prov) {
		this.prov = prov;
	}

	@Override
	public void map(final Integer keyinput, final Data valueinput, final Context<String, Integer> context) {

		if (valueinput.getNombreprov().equals(this.prov))
			context.emit(valueinput.getNombredepto(), 1);

	}
}