package ar.edu.itba.pod.hz.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class ProvinceDepartmentTuple implements DataSerializable {

	private String nombreProv;
	private String nombreDepto;

	public ProvinceDepartmentTuple() {
	}

	public ProvinceDepartmentTuple(String nombreProv, String nombreDepto) {
		super();
		this.nombreProv = nombreProv;
		this.nombreDepto = nombreDepto;
	}

	@Override
	public String toString() {
		return nombreDepto + " (" + nombreProv + ")";
	}

	public String getNombreDepto() {
		return nombreDepto;
	}

	public String getNombreProv() {
		return nombreProv;
	}

	public void setNombreDepto(String nombreDepto) {
		this.nombreDepto = nombreDepto;
	}

	public void setNombreProv(String nombreProv) {
		this.nombreProv = nombreProv;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		nombreProv = in.readUTF();
		nombreDepto = in.readUTF();
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(nombreProv);
		out.writeUTF(nombreDepto);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((nombreDepto == null) ? 0 : nombreDepto.hashCode());
		result = prime * result + ((nombreProv == null) ? 0 : nombreProv.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProvinceDepartmentTuple other = (ProvinceDepartmentTuple) obj;
		if (nombreDepto == null) {
			if (other.nombreDepto != null)
				return false;
		} else if (!nombreDepto.equals(other.nombreDepto))
			return false;
		if (nombreProv == null) {
			if (other.nombreProv != null)
				return false;
		} else if (!nombreProv.equals(other.nombreProv))
			return false;
		return true;
	}

}
