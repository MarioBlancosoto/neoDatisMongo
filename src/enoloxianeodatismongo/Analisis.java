/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package enoloxianeodatismongo;

/**
 *
 * @author oracle
 */
public class Analisis {
 String codigoa;
 int acidez;
 String tipouva;
 int cantidade;
 String dni;

    public Analisis(String codigoa, int acidez, String tipouva, int cantidade, String dni) {
        this.codigoa = codigoa;
        this.acidez = acidez;
        this.tipouva = tipouva;
        this.cantidade = cantidade;
        this.dni = dni;
    }

    public String getCodigoa() {
        return codigoa;
    }

    public void setCodigoa(String codigoa) {
        this.codigoa = codigoa;
    }

    public int getAcidez() {
        return acidez;
    }

    public void setAcidez(int acidez) {
        this.acidez = acidez;
    }

    public String getTipouva() {
        return tipouva;
    }

    public void setTipouva(String tipouva) {
        this.tipouva = tipouva;
    }

    public int getCantidade() {
        return cantidade;
    }

    public void setCantidade(int cantidade) {
        this.cantidade = cantidade;
    }

    public String getDni() {
        return dni;
    }

    public void setDni(String dni) {
        this.dni = dni;
    }

    @Override
    public String toString() {
        return "Analisis{" + "codigoa=" + codigoa + ", acidez=" + acidez + ", tipouva=" + tipouva + ", cantidade=" + cantidade + ", dni=" + dni + '}';
    }
 
}
