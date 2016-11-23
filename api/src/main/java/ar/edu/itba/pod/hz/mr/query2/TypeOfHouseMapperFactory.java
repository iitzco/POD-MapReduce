package ar.edu.itba.pod.hz.mr.query2;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.Data;

public class TypeOfHouseMapperFactory implements Mapper<Integer, Data, Integer, Integer> {
	private static final long serialVersionUID = -3713325164465665033L;

	@Override
	public void map(final Integer keyinput, final Data valueinput, final Context<Integer, Integer> context) {

		int tipoVivienda = valueinput.getTipovivienda();
		context.emit(tipoVivienda, valueinput.getHogarid());
	}
}