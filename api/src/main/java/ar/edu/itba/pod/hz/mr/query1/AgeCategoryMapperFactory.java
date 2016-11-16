package ar.edu.itba.pod.hz.mr.query1;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.Data;

public class AgeCategoryMapperFactory implements Mapper<Integer, Data, String, Integer> {
	private static final long serialVersionUID = -3713325164465665033L;

	@Override
	public void map(final Integer keyinput, final Data valueinput, final Context<String, Integer> context) {
		int age = valueinput.getEdad();
		String category = "";

		if (age <= 14)
			category = "0-14";
		else if (age <= 64)
			category = "14-64";
		else
			category = "65-?";

		context.emit(category, 1);
	}
}