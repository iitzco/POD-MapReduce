package ar.edu.itba.pod.hz.client;

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

import ar.edu.itba.pod.hz.client.reader.VotacionReader;
import ar.edu.itba.pod.hz.model.FormulaTupla;
import ar.edu.itba.pod.hz.model.Votacion;
import ar.edu.itba.pod.hz.mr.ComunaFormulaVotesMapperFactory;
import ar.edu.itba.pod.hz.mr.WinningFormulaReducerFactory;

public class VotacionClient {
    private static final String MAP_NAME = "paso2015";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String name = System.getProperty("name");
        String pass = System.getProperty("pass");
        if (pass == null) {
            pass = "dev-pass";
        }
        System.out.println(String.format("Connecting with cluster dev-name [%s]", name));

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

        System.out.println(client.getCluster());

        // Preparar la particion de datos y distribuirla en el cluster a trav�s
        // del IMap
        IMap<String, Votacion> myMap = client.getMap(MAP_NAME);
        try {
            VotacionReader.readVotacion(myMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Ahora el JobTracker y los Workers!
        JobTracker tracker = client.getJobTracker("default");

        // Ahora el Job desde los pares(key, Value) que precisa MapReduce
        KeyValueSource<String, Votacion> source = KeyValueSource.fromMap(myMap);
        Job<String, Votacion> job = tracker.newJob(source);

        // // Orquestacion de Jobs y lanzamiento
        ICompletableFuture<Map<String, FormulaTupla>> future = job
                                                                  .mapper(new ComunaFormulaVotesMapperFactory())
                                                                  .reducer(new WinningFormulaReducerFactory())
                                                                  .submit();

        // Tomar resultado e Imprimirlo
        Map<String, FormulaTupla> rta = future.get();

        for (Entry<String, FormulaTupla> e : rta.entrySet()) {
            System.out.println(String.format("Distrito %s => Ganador %s", e.getKey(), e.getValue()));
        }

        System.exit(0);

    }
}
