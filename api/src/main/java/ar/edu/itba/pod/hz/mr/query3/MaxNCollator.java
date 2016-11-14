package ar.edu.itba.pod.hz.mr.query3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.mapreduce.Collator;

public class MaxNCollator implements Collator<Entry<String, Double>, Map<String, Double>> {

	private int N;

	public MaxNCollator(int N) {
		this.N = N;
	}

	@Override
	public Map<String, Double> collate(Iterable<Entry<String, Double>> in) {
		List<Entry<String, Double>> orderedList = new ArrayList<>();

		for (Entry<String, Double> entry : in) {
			int index = 0;
			while (index < (orderedList.size() - 1) && orderedList.get(index).getValue() > entry.getValue())
				index++;

			if (index == orderedList.size())
				orderedList.add(entry);
			else
				orderedList.add(index, entry);
		}

		Map<String, Double> ret = new HashMap<>();
		for (int i = 0; i < orderedList.size() && i < N; i++) {
			Entry<String, Double> en = orderedList.get(i);
			ret.put(en.getKey(), en.getValue());
		}
		return ret;
	}

}
