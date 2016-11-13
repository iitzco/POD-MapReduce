package ar.edu.itba.pod.hz.mr.query4;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.Data;

public class DepartmentByProvUnitMapperFactory implements Mapper<Integer, Data, String, Integer> {
	private static final long serialVersionUID = -3713325164465665033L;

	private String prov;

	public DepartmentByProvUnitMapperFactory(String prov) {
		this.prov = prov;
	}

	@Override
	public void map(final Integer keyinput, final Data valueinput, final Context<String, Integer> context) {

		if (valueinput.getNombreprov().equals(this.prov))
			context.emit(valueinput.getNombredepto(), 1);

	}
}