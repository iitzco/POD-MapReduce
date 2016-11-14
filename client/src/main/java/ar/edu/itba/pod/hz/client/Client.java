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
import ar.edu.itba.pod.hz.model.DepartmentDepartmentTuple;
import ar.edu.itba.pod.hz.mr.query1.AgeCategoryCounterReducerFactory;
import ar.edu.itba.pod.hz.mr.query1.AgeCategoryMapperFactory;
import ar.edu.itba.pod.hz.mr.query2.AverageHabitantsPerHouseReducerFactory;
import ar.edu.itba.pod.hz.mr.query2.TypeOfHouseMapperFactory;
import ar.edu.itba.pod.hz.mr.query3.AnalphabetPerDepartmentReducerFactory;
import ar.edu.itba.pod.hz.mr.query3.DepartmentAnalphabetMapperFactory;
import ar.edu.itba.pod.hz.mr.query3.MaxNCollator;
import ar.edu.itba.pod.hz.mr.query4.DepartmentByProvUnitMapperFactory;
import ar.edu.itba.pod.hz.mr.query4.DepartmentFilterCounterReducerFactory;
import ar.edu.itba.pod.hz.mr.query4.UnderTopeCollator;
import ar.edu.itba.pod.hz.mr.query5.DepartmentPer100CounterReducerFactory;
import ar.edu.itba.pod.hz.mr.query5.DepartmentUnitMapperFactory;
import ar.edu.itba.pod.hz.mr.query5.Per100MapperFactory;
import ar.edu.itba.pod.hz.mr.query5.TupleReducerFactory;

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

		// ----------------------- QUERY 1 ----------------------------------

		ICompletableFuture<Map<String, Integer>> futureQuery1 = job.mapper(new AgeCategoryMapperFactory())
				.reducer(new AgeCategoryCounterReducerFactory()).submit();

		Map<String, Integer> rtaQuery1 = futureQuery1.get();

		System.out.println("QUERY 1");
		for (Entry<String, Integer> e : rtaQuery1.entrySet()) {
			System.out.println(String.format("%s => %s", e.getKey(), e.getValue()));
		}
		// ---------------------------------------------------------------------

		// ----------------------- QUERY 2 ----------------------------------

		job = tracker.newJob(source);
		ICompletableFuture<Map<Integer, Double>> futureQuery2 = job.mapper(new TypeOfHouseMapperFactory())
				.reducer(new AverageHabitantsPerHouseReducerFactory()).submit();

		Map<Integer, Double> rtaQuery2 = futureQuery2.get();

		System.out.println("QUERY 2");
		for (Entry<Integer, Double> e : rtaQuery2.entrySet()) {
			System.out.println(String.format("%s => %s", e.getKey(), e.getValue()));
		}
		// ---------------------------------------------------------------------

		// ----------------------- QUERY 3 ----------------------------------

		int n = 4;

		job = tracker.newJob(source);
		ICompletableFuture<Map<String, Double>> futureQuery3 = job.mapper(new DepartmentAnalphabetMapperFactory())
				.reducer(new AnalphabetPerDepartmentReducerFactory()).submit(new MaxNCollator(n));

		Map<String, Double> rtaQuery3 = futureQuery3.get();

		System.out.println("QUERY 3");
		for (Entry<String, Double> e : rtaQuery3.entrySet()) {
			System.out.println(String.format("%s => %s", e.getKey(), e.getValue()));
		}
		// ---------------------------------------------------------------------

		// ----------------------- QUERY 4 ----------------------------------

		String nombreProv = "Buenos Aires";
		int tope = 10;

		job = tracker.newJob(source);
		ICompletableFuture<Map<String, Integer>> futureQuery4 = job
				.mapper(new DepartmentByProvUnitMapperFactory(nombreProv))
				.reducer(new DepartmentFilterCounterReducerFactory()).submit(new UnderTopeCollator(tope));

		Map<String, Integer> rtaQuery4 = futureQuery4.get();

		System.out.println("QUERY 4");
		for (Entry<String, Integer> e : rtaQuery4.entrySet()) {
			System.out.println(String.format("%s => %s", e.getKey(), e.getValue()));
		}

		// ---------------------------------------------------------------------

		// ----------------------- QUERY 5 ----------------------------------

		job = tracker.newJob(source);
		ICompletableFuture<Map<String, Integer>> auxQuery5 = job.mapper(new DepartmentUnitMapperFactory())
				.reducer(new DepartmentPer100CounterReducerFactory()).submit();

		IMap<String, Integer> partialMapForQuery5 = client.getMap("auxForQuery5");
		Map<String, Integer> rtaParcialQuery5 = auxQuery5.get();

		for (Entry<String, Integer> entry : rtaParcialQuery5.entrySet()) {
			partialMapForQuery5.put(entry.getKey(), entry.getValue());
		}

		KeyValueSource<String, Integer> auxSourceForQuery5 = KeyValueSource.fromMap(partialMapForQuery5);
		Job<String, Integer> auxJobForQuery5 = tracker.newJob(auxSourceForQuery5);

		ICompletableFuture<Map<Integer, List<DepartmentDepartmentTuple>>> finalFutureQuery5 = auxJobForQuery5
				.mapper(new Per100MapperFactory()).reducer(new TupleReducerFactory()).submit();

		System.out.println("QUERY 5");
		Map<Integer, List<DepartmentDepartmentTuple>> finalQuery5 = finalFutureQuery5.get();
		for (Entry<Integer, List<DepartmentDepartmentTuple>> e : finalQuery5.entrySet()) {
			System.out.println(String.format("%s", e.getKey()));
			for (DepartmentDepartmentTuple each : e.getValue())
				System.out.println(each);
		}

		// ---------------------------------------------------------------------

		System.exit(0);

	}
}
