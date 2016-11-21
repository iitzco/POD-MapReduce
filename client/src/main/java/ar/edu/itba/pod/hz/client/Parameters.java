package ar.edu.itba.pod.hz.client;

public class Parameters {
	private String name;
	private String pass;
	private String[] addresses;
	private int query;
	private String pathIn;
	private String pathOut;
	private int n;
	private String prov;
	private int tope;
	private boolean loadMap;

	public static Parameters loadParameters() {
		try {
			Parameters ret = new Parameters();

			ret.name = System.getProperty("name", "52539-53891");
			ret.pass = System.getProperty("pass", "pass");

			String addrs = System.getProperty("addresses", "127.0.0.1");
			ret.addresses = addrs.split("[,;]");

			String queryNumber = System.getProperty("query");
			ret.query = Integer.valueOf(queryNumber);

			if (ret.query < 1 || ret.query > 5)
				throw new Exception();

			ret.pathIn = System.getProperty("inPath");
			ret.pathOut = System.getProperty("outPath");

			ret.loadMap = Boolean.valueOf(System.getProperty("loadMap", "true").toLowerCase());

			if (ret.query == 3) {
				ret.n = Integer.valueOf(System.getProperty("n"));
			} else if (ret.query == 4) {
				ret.tope = Integer.valueOf(System.getProperty("tope"));
				ret.prov = System.getProperty("prov");
			}
			return ret;
		} catch (Exception e) {
			System.out.println("Error in parameters.");
			System.exit(1);
		}
		return null;

	}

	public String getName() {
		return name;
	}

	public String getPass() {
		return pass;
	}

	public String[] getAddresses() {
		return addresses;
	}

	public int getQuery() {
		return query;
	}

	public String getPathIn() {
		return pathIn;
	}

	public String getPathOut() {
		return pathOut;
	}

	public int getN() {
		return n;
	}

	public String getProv() {
		return prov;
	}

	public int getTope() {
		return tope;
	}

	public boolean isLoadMap() {
		return loadMap;
	}

}
