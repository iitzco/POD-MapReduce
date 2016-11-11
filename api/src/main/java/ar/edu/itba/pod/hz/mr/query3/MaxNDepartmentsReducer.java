package ar.edu.itba.pod.hz.mr.query3;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import ar.edu.itba.pod.hz.model.DepartmentValueTuple;

public class MaxNDepartmentsReducer
		implements ReducerFactory<Integer, DepartmentValueTuple, List<DepartmentValueTuple>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Reducer<DepartmentValueTuple, List<DepartmentValueTuple>> newReducer(final Integer n) {
		return new Reducer<DepartmentValueTuple, List<DepartmentValueTuple>>() {
			List<DepartmentValueTuple> list;

			@Override
			public void beginReduce() {
				list = new LinkedList<>();
			}

			@Override
			public void reduce(DepartmentValueTuple param) {
				int index = 0;
				while (index < list.size() && list.get(index).getValue() > param.getValue())
					index++;

				if (index == list.size())
					list.add(param);
				else
					list.add(index, param);
			}

			@Override
			public List<DepartmentValueTuple> finalizeReduce() {
				List<DepartmentValueTuple> ret = new ArrayList<>();

				for (int i = 0; i < n; i++) {
					ret.add(list.get(i));

				}
				return ret;
			}
		};
	}

}
