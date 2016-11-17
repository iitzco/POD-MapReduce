package ar.edu.itba.pod.hz.client;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import ar.edu.itba.pod.hz.mr.query5.EmptyListCollator;
import ar.edu.itba.pod.hz.mr.query5.Per100MapperFactory;
import ar.edu.itba.pod.hz.mr.query5.TupleReducerFactory;

public class Client {

	private HazelcastInstance client;

	private String fileNameIn;

	PrintWriter writer;

	public Client(ClientConfig ccfg, String fileNameIn, String fileNameOut)
			throws FileNotFoundException, UnsupportedEncodingException {
		super();
		this.client = HazelcastClient.newHazelcastClient(ccfg);
		this.fileNameIn = fileNameIn;
		this.writer = new PrintWriter(fileNameOut, "UTF-8");
	}

	private static final String MAP_NAME = "52539-53891-main";

	private static Logger logger = LoggerFactory.getLogger(Client.class);

	public static void main(String[] args) {

		try {
			Parameters p = Parameters.loadParameters();

			ClientConfig ccfg = new ClientConfig();
			ccfg.getGroupConfig().setName(p.getName()).setPassword(p.getPass());

			ClientNetworkConfig net = new ClientNetworkConfig();
			net.addAddress(p.getAddresses());
			ccfg.setNetworkConfig(net);

			Client queryClient = new Client(ccfg, p.getPathIn(), p.getPathOut());

			switch (p.getQuery()) {
			case 1:
				queryClient.query1();
				break;
			case 2:
				queryClient.query2();
				break;
			case 3:
				queryClient.query3(p.getN());
				break;
			case 4:
				queryClient.query4(p.getProv(), p.getTope());
				break;
			case 5:
				queryClient.query5();
				break;
			}
			queryClient.writer.close();
			System.exit(0);

		} catch (Exception e) {
			System.out.println("An unexpected error occured.");
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void query1()
			throws InterruptedException, ExecutionException, FileNotFoundException, UnsupportedEncodingException {
		IMap<Integer, Data> myMap = client.getMap(MAP_NAME);
		try {
			logger.info("Inicio de la lectura del archivo");
			DataSetReader.readDataSet(myMap, fileNameIn);
			logger.info("Fin de lectura del archivo");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		JobTracker tracker = client.getJobTracker("default");

		KeyValueSource<Integer, Data> source = KeyValueSource.fromMap(myMap);
		Job<Integer, Data> job = tracker.newJob(source);

		logger.info("Inicio del trabajo map/reduce");

		ICompletableFuture<Map<String, Integer>> futureQuery1 = job.mapper(new AgeCategoryMapperFactory())
				.reducer(new AgeCategoryCounterReducerFactory()).submit();

		Map<String, Integer> rtaQuery1 = futureQuery1.get();
		logger.info("Fin del trabajo map/reduce");

		writer.println(String.format("0-14 = %s", rtaQuery1.getOrDefault("0-14", 0)));
		writer.println(String.format("15-64 = %s", rtaQuery1.getOrDefault("15-64", 0)));
		writer.println(String.format("65-? = %s", rtaQuery1.getOrDefault("65-?", 0)));

	}

	public void query2() throws InterruptedException, ExecutionException {
		IMap<Integer, Data> myMap = client.getMap(MAP_NAME);
		try {
			logger.info("Inicio de la lectura del archivo");
			DataSetReader.readDataSet(myMap, fileNameIn);
			logger.info("Fin de lectura del archivo");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		JobTracker tracker = client.getJobTracker("default");

		KeyValueSource<Integer, Data> source = KeyValueSource.fromMap(myMap);
		Job<Integer, Data> job = tracker.newJob(source);

		job = tracker.newJob(source);
		logger.info("Inicio del trabajo map/reduce");
		ICompletableFuture<Map<Integer, Double>> futureQuery2 = job.mapper(new TypeOfHouseMapperFactory())
				.reducer(new AverageHabitantsPerHouseReducerFactory()).submit();

		Map<Integer, Double> rtaQuery2 = futureQuery2.get();
		logger.info("Fin del trabajo map/reduce");

		List<Entry<Integer, Double>> ret = new ArrayList<>();

		for (Entry<Integer, Double> e : rtaQuery2.entrySet()) {
			int i = 0;
			while (i < ret.size() && ret.get(i).getKey() < e.getKey())
				i++;
			ret.add(i, e);
		}

		for (Entry<Integer, Double> e : ret) {
			writer.println(String.format("%s = %.02f", e.getKey(), e.getValue()));
		}
	}

	public void query3(int n) throws InterruptedException, ExecutionException {
		IMap<Integer, Data> myMap = client.getMap(MAP_NAME);
		try {
			logger.info("Inicio de la lectura del archivo");
			DataSetReader.readDataSet(myMap, fileNameIn);
			logger.info("Fin de lectura del archivo");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		JobTracker tracker = client.getJobTracker("default");

		KeyValueSource<Integer, Data> source = KeyValueSource.fromMap(myMap);
		Job<Integer, Data> job = tracker.newJob(source);

		job = tracker.newJob(source);

		logger.info("Inicio del trabajo map/reduce");
		ICompletableFuture<Map<String, Double>> futureQuery3 = job.mapper(new DepartmentAnalphabetMapperFactory())
				.reducer(new AnalphabetPerDepartmentReducerFactory()).submit(new MaxNCollator(n));

		Map<String, Double> rtaQuery3 = futureQuery3.get();
		logger.info("Fin del trabajo map/reduce");

		List<Entry<String, Double>> ret = new ArrayList<>();
		ret.addAll(rtaQuery3.entrySet());
		Collections.sort(ret, new Comparator<Entry<String, Double>>() {
			@Override
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				return Double.compare(o2.getValue(), o1.getValue());
			}
		});

		for (Entry<String, Double> e : ret) {
			writer.println(String.format("%s = %.02f", e.getKey(), e.getValue()));
		}
	}

	public void query4(String nombreProv, int tope) throws InterruptedException, ExecutionException {
		IMap<Integer, Data> myMap = client.getMap(MAP_NAME);
		try {
			logger.info("Inicio de la lectura del archivo");
			DataSetReader.readDataSet(myMap, fileNameIn);
			logger.info("Fin de lectura del archivo");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		JobTracker tracker = client.getJobTracker("default");

		KeyValueSource<Integer, Data> source = KeyValueSource.fromMap(myMap);
		Job<Integer, Data> job = tracker.newJob(source);

		job = tracker.newJob(source);

		logger.info("Inicio del trabajo map/reduce");
		ICompletableFuture<Map<String, Integer>> futureQuery4 = job
				.mapper(new DepartmentByProvUnitMapperFactory(nombreProv))
				.reducer(new DepartmentFilterCounterReducerFactory()).submit(new UnderTopeCollator(tope));

		Map<String, Integer> rtaQuery4 = futureQuery4.get();
		logger.info("Fin del trabajo map/reduce");

		List<Entry<String, Integer>> ret = new ArrayList<>();
		ret.addAll(rtaQuery4.entrySet());
		Collections.sort(ret, new Comparator<Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return Double.compare(o2.getValue(), o1.getValue());
			}
		});

		for (Entry<String, Integer> e : ret) {
			writer.println(String.format("%s = %s", e.getKey(), e.getValue()));
		}

	}

	public void query5() throws InterruptedException, ExecutionException {
		IMap<Integer, Data> myMap = client.getMap(MAP_NAME);
		try {
			logger.info("Inicio de la lectura del archivo");
			DataSetReader.readDataSet(myMap, fileNameIn);
			logger.info("Fin de lectura del archivo");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		JobTracker tracker = client.getJobTracker("default");

		KeyValueSource<Integer, Data> source = KeyValueSource.fromMap(myMap);
		Job<Integer, Data> job = tracker.newJob(source);

		job = tracker.newJob(source);

		logger.info("Inicio del trabajo map/reduce");

		ICompletableFuture<Map<String, Integer>> auxQuery5 = job.mapper(new DepartmentUnitMapperFactory())
				.reducer(new DepartmentPer100CounterReducerFactory()).submit();

		IMap<String, Integer> partialMapForQuery5 = client.getMap("52539-53891-aux");
		Map<String, Integer> rtaParcialQuery5 = auxQuery5.get();

		for (Entry<String, Integer> entry : rtaParcialQuery5.entrySet()) {
			partialMapForQuery5.put(entry.getKey(), entry.getValue());
		}

		KeyValueSource<String, Integer> auxSourceForQuery5 = KeyValueSource.fromMap(partialMapForQuery5);
		Job<String, Integer> auxJobForQuery5 = tracker.newJob(auxSourceForQuery5);

		ICompletableFuture<Map<Integer, List<DepartmentDepartmentTuple>>> finalFutureQuery5 = auxJobForQuery5
				.mapper(new Per100MapperFactory()).reducer(new TupleReducerFactory()).submit(new EmptyListCollator());

		Map<Integer, List<DepartmentDepartmentTuple>> finalQuery5 = finalFutureQuery5.get();

		logger.info("Fin del trabajo map/reduce");
		for (Entry<Integer, List<DepartmentDepartmentTuple>> e : finalQuery5.entrySet()) {
			writer.println(e.getKey() * 100);
			for (DepartmentDepartmentTuple each : e.getValue())
				writer.println(each);
		}
	}

}
