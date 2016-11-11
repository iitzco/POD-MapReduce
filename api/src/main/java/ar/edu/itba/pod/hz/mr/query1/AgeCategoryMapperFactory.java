package ar.edu.itba.pod.hz.mr.query1;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.Data;

// Parametrizar con los tipos de keyInput, ,valueInput, keyoutput, valueOutput
public class AgeCategoryMapperFactory implements Mapper<Integer, Data, String, Integer> {
	private static final long serialVersionUID = -3713325164465665033L;

	@Override
	public void map(final Integer keyinput, final Data valueinput, final Context<String, Integer> context) {
//		System.out.println(String.format("Llega KeyInput: %s con ValueInput: %s", keyinput, valueinput));

		int age = valueinput.getEdad();
		String category = "";

		if (age < 14)
			category = "0 a 14";
		else if (age < 64)
			category = "14 a 64";
		else
			category = "65 o mas";

		context.emit(category, 1);

//		System.out.println(String.format("Se emite (%s, %s)", category, 1));
	}
}