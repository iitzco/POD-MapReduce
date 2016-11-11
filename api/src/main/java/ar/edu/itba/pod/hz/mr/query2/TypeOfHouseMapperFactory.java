package ar.edu.itba.pod.hz.mr.query2;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.Data;

// Parametrizar con los tipos de keyInput, ,valueInput, keyoutput, valueOutput
public class TypeOfHouseMapperFactory implements Mapper<Integer, Data, Integer, Data> {
	private static final long serialVersionUID = -3713325164465665033L;

	@Override
	public void map(final Integer keyinput, final Data valueinput, final Context<Integer, Data> context) {
//		System.out.println(String.format("Llega KeyInput: %s con ValueInput: %s", keyinput, valueinput));

		int tipoVivienda = valueinput.getTipovivienda();
		context.emit(tipoVivienda, valueinput);

//		System.out.println(String.format("Se emite (%s, %s)", tipoVivienda, valueinput));
	}
}