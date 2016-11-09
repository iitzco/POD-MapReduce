package ar.edu.itba.pod.hz.client.reader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import com.hazelcast.core.IMap;

import ar.edu.itba.pod.hz.model.Votacion;

public class VotacionReader {
    // el directorio wc dentro en un directorio llamado "resources"
    // al mismo nivel que la carpeta src, etc.
    private static final String FILENAME = "files/pasopresidenciales2015.csv";

    private static CellProcessor[] getProcessors() {
        return new CellProcessor[] { new NotNull(), // provincia
                new NotNull(), // comuna
                new NotNull(), // asociacion politica
                new NotNull(), // candidato
                new ParseInt(new NotNull()) // cantidad de votos
        };
    }

    public static void readVotacion(final IMap<String, Votacion> theIMap) throws Exception {
        ICsvBeanReader beanReader = null;
        try {
            final InputStream is = VotacionReader.class.getClassLoader().getResourceAsStream(FILENAME);
            final Reader aReader = new InputStreamReader(is);
            beanReader = new CsvBeanReader(aReader, CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE); // separador
                                                                                                  // ;

            // the header elements are used to map the values to the bean (names
            // must match)
            final String[] header = beanReader.getHeader(true);
            final CellProcessor[] processors = getProcessors();

            Votacion aV;
            while ((aV = beanReader.read(Votacion.class, header, processors)) != null) {
                System.out.println(String.format("lineNo=%s, rowNo=%s, customer=%s",
                        beanReader.getLineNumber(), beanReader.getRowNumber(), aV));
                theIMap.set(aV.getDistrito() + "-" + aV.getFormula(), aV);
            }
        } finally {
            if (beanReader != null) {
                beanReader.close();
            }
        }
    }
}
