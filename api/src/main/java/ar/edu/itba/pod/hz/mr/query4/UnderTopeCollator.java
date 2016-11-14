package ar.edu.itba.pod.hz.mr.query4;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.mapreduce.Collator;

public class UnderTopeCollator implements Collator<Entry<String, Integer>, Map<String, Integer>> {

	private int tope;

	public UnderTopeCollator(int tope) {
		this.tope = tope;
	}

	@Override
	public Map<String, Integer> collate(Iterable<Entry<String, Integer>> in) {
		Map<String, Integer> ret = new HashMap<>();

		for (Entry<String, Integer> entry : in) {
			if (entry.getValue() < tope)
				ret.put(entry.getKey(), entry.getValue());

		}
		return ret;
	}

}
