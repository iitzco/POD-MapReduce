package ar.edu.itba.pod.hz.mr;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import ar.edu.itba.pod.hz.model.FormulaTupla;

public class WinningFormulaReducerFactory implements ReducerFactory<String, FormulaTupla, FormulaTupla> {
    private static final long serialVersionUID = 7760070699178320492L;

    @Override
    public Reducer<FormulaTupla, FormulaTupla> newReducer(final String distrito) {
        return new Reducer<FormulaTupla, FormulaTupla>() {
            private FormulaTupla max;

            @Override
            public void beginReduce() // una sola vez en cada instancia
            {
                max = new FormulaTupla(-1, "");
            }

            @Override
            public void reduce(final FormulaTupla value) {
                if (max.compareTo(value) < 1) {
                    // si el nuevo es mayor me quedo con ese.
                    max = value;
                }
            }

            @Override
            public FormulaTupla finalizeReduce() {
                System.out.println(String.format("FinalReduce for %s = %s", distrito, max));
                return max;
            }
        };
    }
}
