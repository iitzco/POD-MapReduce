package ar.edu.itba.pod.hz.mr;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import ar.edu.itba.pod.hz.model.FormulaTupla;
import ar.edu.itba.pod.hz.model.Votacion;

// Parametrizar con los tipos de keyInput, ,valueInput, keyoutput, valueOutput
public class ComunaFormulaVotesMapperFactory implements Mapper<String, Votacion, String, FormulaTupla> {
    private static final long serialVersionUID = -3713325164465665033L;

    @Override
    public void map(final String keyinput, final Votacion valueinput,
            final Context<String, FormulaTupla> context) {
        System.out.println(String.format("Llega KeyInput: %s con ValueInput: %s", keyinput, valueinput));

        final FormulaTupla valueoutput = new FormulaTupla(valueinput.getVotos(), valueinput.getFormula());
        context.emit(valueinput.getDistrito(), valueoutput);

        System.out.println(String.format("Se emite (%s, %s)", valueinput.getDistrito(), valueoutput));
    }
}