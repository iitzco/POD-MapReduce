package ar.edu.itba.pod.hz.mr.query5;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.mapreduce.Collator;

import ar.edu.itba.pod.hz.model.DepartmentDepartmentTuple;

public class EmptyListCollator implements
		Collator<Entry<Integer, List<DepartmentDepartmentTuple>>, Map<Integer, List<DepartmentDepartmentTuple>>> {

	@Override
	public Map<Integer, List<DepartmentDepartmentTuple>> collate(
			Iterable<Entry<Integer, List<DepartmentDepartmentTuple>>> in) {

		Map<Integer, List<DepartmentDepartmentTuple>> ret = new HashMap<>();
		for (Entry<Integer, List<DepartmentDepartmentTuple>> entry : in) {
			if (!entry.getValue().isEmpty())
				ret.put(entry.getKey(), entry.getValue());
		}
		return ret;
	}

}
