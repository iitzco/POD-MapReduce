package ar.edu.itba.pod.hz.client.reader;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.Trim;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import com.hazelcast.core.IMap;

import ar.edu.itba.pod.hz.model.Data;

public class DataSetReader {

	// private static final String FILENAME = "files/dataset-1000.csv";

	private static CellProcessor[] getProcessors() {
		return new CellProcessor[] { new ParseInt(new NotNull()), // tipovivienda
				new ParseInt(new NotNull()), // calidadservicios
				new ParseInt(new NotNull()), // sexo
				new ParseInt(new NotNull()), // edad
				new ParseInt(new NotNull()), // alfabetismo
				new ParseInt(new NotNull()), // actividad
				new NotNull(new Trim()), // nombredepto
				new NotNull(new Trim()), // nombreprov
				new ParseInt(new NotNull()) // hogarid
		};
	}

	public static void readDataSet(final IMap<Integer, Data> theIMap, String fileNameIn) throws Exception {
		ICsvBeanReader beanReader = null;
		try {
			final FileInputStream is = new FileInputStream(fileNameIn);
			final Reader aReader = new InputStreamReader(is);
			beanReader = new CsvBeanReader(aReader, CsvPreference.EXCEL_PREFERENCE);

			final String[] header = beanReader.getHeader(true);
			final CellProcessor[] processors = getProcessors();

			Data aD;
			while ((aD = beanReader.read(Data.class, header, processors)) != null) {
				int row_id = beanReader.getRowNumber();
				theIMap.set(row_id, aD);
			}
		} finally {
			if (beanReader != null) {
				beanReader.close();
			}
		}
	}
}
