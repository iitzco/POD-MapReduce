package ar.edu.itba.pod.hz.model.query4;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class FilterTopeDepartmentMapper implements Mapper<String, Integer, String, Integer> {
	private static final long serialVersionUID = -3713325164465665033L;

	@Override
	public void map(final String keyinput, final Integer valueinput, final Context<String, Integer> context) {
		if (valueinput >= 0) {
			context.emit(keyinput, valueinput);
		}
	}
}