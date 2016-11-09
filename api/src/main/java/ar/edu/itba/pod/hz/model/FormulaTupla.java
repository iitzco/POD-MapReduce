package ar.edu.itba.pod.hz.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class FormulaTupla implements DataSerializable, Comparable<FormulaTupla> {
    private int votos;
    private String formula;

    public FormulaTupla() {
    }

    public FormulaTupla(final int theVotos, final String theFormula) {
        votos = theVotos;
        formula = theFormula;
    }

    public int getVotos() {
        return votos;
    }

    public void setVotos(final int theVotos) {
        votos = theVotos;
    }

    public String getFormula() {
        return formula;
    }

    public void setFormula(final String theFormula) {
        formula = theFormula;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {

        out.writeInt(votos);
        out.writeUTF(formula);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {

        votos = in.readInt();
        formula = in.readUTF();
    }

    @Override
    public String toString() {

        return String.format("Formula: %s con %d votos", formula, votos);
    }

    @Override
    public int compareTo(final FormulaTupla other) {
        return getVotos() - other.getVotos();
    }
}
