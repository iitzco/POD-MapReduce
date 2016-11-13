package ar.edu.itba.pod.hz.mr.query5;

import java.util.ArrayList;
import java.util.List;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import ar.edu.itba.pod.hz.model.DepartmentDepartmentTuple;

public class TupleReducerFactory implements ReducerFactory<Integer, String, List<DepartmentDepartmentTuple>> {
	private static final long serialVersionUID = 7760070699178320492L;

	@Override
	public Reducer<String, List<DepartmentDepartmentTuple>> newReducer(final Integer per100) {
		return new Reducer<String, List<DepartmentDepartmentTuple>>() {
			List<DepartmentDepartmentTuple> ret;
			List<String> list;

			@Override
			public void beginReduce() {
				list = new ArrayList<>();
				ret = new ArrayList<>();
			}

			@Override
			public void reduce(final String value) {
				list.add(value);
			}

			@Override
			public List<DepartmentDepartmentTuple> finalizeReduce() {
				for (int i = 0; i < list.size() - 1; i++) {
					for (int j = i + 1; j < list.size(); j++) {
						ret.add(new DepartmentDepartmentTuple(list.get(i), list.get(j)));
					}
				}
				return ret;
			}
		};
	}
}
