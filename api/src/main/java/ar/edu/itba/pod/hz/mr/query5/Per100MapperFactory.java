package ar.edu.itba.pod.hz.mr.query5;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Per100MapperFactory implements Mapper<String, Integer, Integer, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public void map(final String keyinput, final Integer valueinput, final Context<Integer, String> context) {
		context.emit(valueinput, keyinput);
	}
}