package ar.edu.itba.pod.hz.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class DepartmentValueTuple implements DataSerializable {

	private String nombredepto;
	private double value;

	public DepartmentValueTuple() {
	}

	public DepartmentValueTuple(String nombredepto, double value) {
		super();
		this.nombredepto = nombredepto;
		this.value = value;
	}

	public String getNombredepto() {
		return nombredepto;
	}

	public void setNombredepto(String nombredepto) {
		this.nombredepto = nombredepto;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		nombredepto = in.readUTF();
		value = in.readDouble();
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(nombredepto);
		out.writeDouble(value);
	}

}
