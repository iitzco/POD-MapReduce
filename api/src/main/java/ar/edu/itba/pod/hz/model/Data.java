package ar.edu.itba.pod.hz.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class Data implements DataSerializable {
	
	private int tipovivienda;
	private int calidadservicios;
	private int sexo;
	private int edad;
	private int alfabetismo;
	private int actividad;
	private String nombredepto;
	private String nombreprov;
	private int hogarid;

    // mantener el orden que se hizo en el write!
    @Override
    public void readData(final ObjectDataInput in) throws IOException {
    	tipovivienda = in.readInt();
    	calidadservicios = in.readInt();
    	sexo = in.readInt();
    	edad = in.readInt();
    	alfabetismo = in.readInt();
    	actividad = in.readInt();
    	nombredepto = in.readUTF();
    	nombreprov = in.readUTF();
    	hogarid = in.readInt();
    }

    // mantener el orden que se hizo en el read!
    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
    	out.writeInt(tipovivienda);
    	out.writeInt(calidadservicios);
    	out.writeInt(sexo);
    	out.writeInt(edad);
    	out.writeInt(alfabetismo);
    	out.writeInt(actividad);
        out.writeUTF(nombredepto);
        out.writeUTF(nombreprov);
    	out.writeInt(hogarid);
    }
    

	@Override
	public String toString() {
		return "Data [tipovivienda=" + tipovivienda + ", calidadservicios=" + calidadservicios + ", sexo=" + sexo
				+ ", edad=" + edad + ", alfabetismo=" + alfabetismo + ", actividad=" + actividad + ", nombredepto="
				+ nombredepto + ", nombreprov=" + nombreprov + ", hogarid=" + hogarid + "]";
	}

	public int getTipovivienda() {
		return tipovivienda;
	}

	public void setTipovivienda(int tipovivienda) {
		this.tipovivienda = tipovivienda;
	}

	public int getCalidadservicios() {
		return calidadservicios;
	}

	public void setCalidadservicios(int calidadservicios) {
		this.calidadservicios = calidadservicios;
	}

	public int getSexo() {
		return sexo;
	}

	public void setSexo(int sexo) {
		this.sexo = sexo;
	}

	public int getEdad() {
		return edad;
	}

	public void setEdad(int edad) {
		this.edad = edad;
	}

	public int getAlfabetismo() {
		return alfabetismo;
	}

	public void setAlfabetismo(int alfabetismo) {
		this.alfabetismo = alfabetismo;
	}

	public int getActividad() {
		return actividad;
	}

	public void setActividad(int actividad) {
		this.actividad = actividad;
	}

	public String getNombredepto() {
		return nombredepto;
	}

	public void setNombredepto(String nombredepto) {
		this.nombredepto = nombredepto;
	}

	public String getNombreprov() {
		return nombreprov;
	}

	public void setNombreprov(String nombreprov) {
		this.nombreprov = nombreprov;
	}

	public int getHogarid() {
		return hogarid;
	}

	public void setHogarid(int hogarid) {
		this.hogarid = hogarid;
	}
    
    

}
