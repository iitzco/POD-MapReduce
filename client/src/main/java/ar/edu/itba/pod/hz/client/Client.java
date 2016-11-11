package ar.edu.itba.pod.hz.client;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import ar.edu.itba.pod.hz.client.reader.DataSetReader;
import ar.edu.itba.pod.hz.model.Data;
import ar.edu.itba.pod.hz.model.DepartmentValueTuple;
import ar.edu.itba.pod.hz.mr.query1.AgeCategoryCounterReducerFactory;
import ar.edu.itba.pod.hz.mr.query1.AgeCategoryMapperFactory;
import ar.edu.itba.pod.hz.mr.query2.AverageHabitantsPerHouseReducerFactory;
import ar.edu.itba.pod.hz.mr.query2.TypeOfHouseMapperFactory;
import ar.edu.itba.pod.hz.mr.query3.AnalphabetPerDepartmentReducerFactory;
import ar.edu.itba.pod.hz.mr.query3.DepartmentMapperFactory;
import ar.edu.itba.pod.hz.mr.query3.MaxNDepartmentsMapper;
import ar.edu.itba.pod.hz.mr.query3.MaxNDepartmentsReducer;

public class Client {
	private static final String MAP_NAME = "mapa";
	// private static Logger logger = LoggerFactory.getLogger(Client.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		String name = System.getProperty("name");
		String pass = System.getProperty("pass");
		if (pass == null) {
			pass = "dev-pass";
		}

		// System.out.println(String.format("Connecting with cluster dev-name
		// [%s]", name));

		ClientConfig ccfg = new ClientConfig();
		ccfg.getGroupConfig().setName(name).setPassword(pass);

		// no hay descubrimiento automatico,
		// pero si no decimos nada intentará usar LOCALHOST
		String addresses = System.getProperty("addresses");
		if (addresses != null) {
			String[] arrayAddresses = addresses.split("[,;]");
			ClientNetworkConfig net = new ClientNetworkConfig();
			net.addAddress(arrayAddresses);
			ccfg.setNetworkConfig(net);
		}
		HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

		IMap<Integer, Data> myMap = client.getMap(MAP_NAME);
		try {
			DataSetReader.readDataSet(myMap);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		JobTracker tracker = client.getJobTracker("default");

		KeyValueSource<Integer, Data> source = KeyValueSource.fromMap(myMap);
		Job<Integer, Data> job = tracker.newJob(source);

		ICompletableFuture<Map<String, Integer>> futureQuery1 = job.mapper(new AgeCategoryMapperFactory())
				.reducer(new AgeCategoryCounterReducerFactory()).submit();

		Map<String, Integer> rtaQuery1 = futureQuery1.get();

		System.out.println("QUERY 1");
		for (Entry<String, Integer> e : rtaQuery1.entrySet()) {
			System.out.println(String.format("%s => %s", e.getKey(), e.getValue()));
		}

		job = tracker.newJob(source);
		ICompletableFuture<Map<Integer, Double>> futureQuery2 = job.mapper(new TypeOfHouseMapperFactory())
				.reducer(new AverageHabitantsPerHouseReducerFactory()).submit();

		Map<Integer, Double> rtaQuery2 = futureQuery2.get();

		System.out.println("QUERY 2");
		for (Entry<Integer, Double> e : rtaQuery2.entrySet()) {
			System.out.println(String.format("%s => %s", e.getKey(), e.getValue()));
		}

		int n = 3;

		job = tracker.newJob(source);
		ICompletableFuture<Map<String, Double>> futureQuery3 = job.mapper(new DepartmentMapperFactory())
				.reducer(new AnalphabetPerDepartmentReducerFactory()).submit();

		IMap<String, Double> partialMap = client.getMap("aux");
		Map<String, Double> rtaParcialQuery3 = futureQuery3.get();

		for (Entry<String, Double> entry : rtaParcialQuery3.entrySet()) {
			partialMap.put(entry.getKey(), entry.getValue());
		}

		KeyValueSource<String, Double> auxSource = KeyValueSource.fromMap(partialMap);
		Job<String, Double> auxJob = tracker.newJob(auxSource);
		ICompletableFuture<Map<Integer, List<DepartmentValueTuple>>> finalFutureQuery3 = auxJob
				.mapper(new MaxNDepartmentsMapper(n)).reducer(new MaxNDepartmentsReducer()).submit();

		System.out.println("QUERY 3");
		Map<Integer, List<DepartmentValueTuple>> finalQuery3 = finalFutureQuery3.get();
		for (Entry<Integer, List<DepartmentValueTuple>> e : finalQuery3.entrySet()) {
			System.out.println(String.format("%s Departamentos mas analfabetos", e.getKey()));
			for (DepartmentValueTuple each : e.getValue())
				System.out.println(String.format("%s => %s", each.getNombredepto(), each.getValue()));
		}

		System.exit(0);

	}
}
