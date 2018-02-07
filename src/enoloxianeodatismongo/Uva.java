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
public class Uva {
 String tipouva ;
 String nomeu;
 int acidezmin;
 int acidezmax;

    public Uva(String tipo, String nomeu, int acidezmin, int acidezmax) {
        this.tipouva = tipo;
        this.nomeu = nomeu;
        this.acidezmin = acidezmin;
        this.acidezmax = acidezmax;
    }

    public String getTipo() {
        return tipouva;
    }

    public void setTipo(String tipo) {
        this.tipouva = tipo;
    }

    public String getNomeu() {
        return nomeu;
    }

    public void setNomeu(String nomeu) {
        this.nomeu = nomeu;
    }

    public int getAcidezmin() {
        return acidezmin;
    }

    public void setAcidezmin(int acidezmin) {
        this.acidezmin = acidezmin;
    }

    public int getAcidezmax() {
        return acidezmax;
    }

    public void setAcidezmax(int acidezmax) {
        this.acidezmax = acidezmax;
    }

    @Override
    public String toString() {
        return "Uva{" + "tipo=" + tipouva + ", nomeu=" + nomeu + ", acidezmin=" + acidezmin + ", acidezmax=" + acidezmax + '}';
    }
 
 
}
