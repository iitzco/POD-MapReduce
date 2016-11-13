package ar.edu.itba.pod.hz.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class DepartmentDepartmentTuple implements DataSerializable {

	private String nombreDepto1;
	private String nombreDepto2;

	public DepartmentDepartmentTuple() {
	}

	public DepartmentDepartmentTuple(String nombreDepto1, String nombreDepto2) {
		super();
		this.nombreDepto1 = nombreDepto1;
		this.nombreDepto2 = nombreDepto2;
	}

	@Override
	public String toString() {
		return nombreDepto1 + " +  " + nombreDepto2;
	}

	public String getNombreDepto1() {
		return nombreDepto1;
	}

	public void setNombreDepto1(String nombreDepto1) {
		this.nombreDepto1 = nombreDepto1;
	}

	public String getNombreDepto2() {
		return nombreDepto2;
	}

	public void setNombreDepto2(String nombreDepto2) {
		this.nombreDepto2 = nombreDepto2;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		nombreDepto1 = in.readUTF();
		nombreDepto2 = in.readUTF();
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(nombreDepto1);
		out.writeUTF(nombreDepto2);
	}

}
