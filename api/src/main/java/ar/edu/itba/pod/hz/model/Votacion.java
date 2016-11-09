package ar.edu.itba.pod.hz.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class Votacion implements DataSerializable {
    private String provincia;
    private String distrito;
    private String partido;
    private String formula;
    private int votos;

    // mantener el orden que se hizo en el write!
    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        provincia = in.readUTF();
        distrito = in.readUTF();
        partido = in.readUTF();
        formula = in.readUTF();
        votos = in.readInt();
    }

    // mantener el orden que se hizo en el read!
    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeUTF(provincia);
        out.writeUTF(distrito);
        out.writeUTF(partido);
        out.writeUTF(formula);
        out.writeInt(votos);
    }

    @Override
    public String toString() {
        return String.format("Province %s, Distrito %s, Partido %s, Formula %s, Votos %d", provincia,
                distrito, partido, formula, votos);
    }

    // getter/setter para el Bean Pojo que usa la clase CsvBeanReader
    public String getProvincia() {
        return provincia;
    }

    public void setProvincia(String provincia) {
        this.provincia = provincia;
    }

    public String getDistrito() {
        return distrito;
    }

    public void setDistrito(String distrito) {
        this.distrito = distrito;
    }

    public String getPartido() {
        return partido;
    }

    public void setPartido(String partido) {
        this.partido = partido;
    }

    public String getFormula() {
        return formula;
    }

    public void setFormula(String formula) {
        this.formula = formula;
    }

    public int getVotos() {
        return votos;
    }

    public void setVotos(int votos) {
        this.votos = votos;
    }

}
