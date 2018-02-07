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
public class Cliente {
  String dni;
  String nome;
  int telf;
  int numerodeanalisis;

    public Cliente(String dni, String nome, int telf, int numerodeanalisis) {
        this.dni = dni;
        this.nome = nome;
        this.telf = telf;
        this.numerodeanalisis = numerodeanalisis;
    }

    public String getDni() {
        return dni;
    }

    public void setDni(String dni) {
        this.dni = dni;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public int getTelf() {
        return telf;
    }

    public void setTelf(int telf) {
        this.telf = telf;
    }

    public int getNumerodeanalisis() {
        return numerodeanalisis;
    }

    public void setNumerodeanalisis(int numerodeanalisis) {
        this.numerodeanalisis = numerodeanalisis;
    }

    @Override
    public String toString() {
        return "Cliente{" + "dni=" + dni + ", nome=" + nome + ", telf=" + telf + ", numerodeanalisis=" + numerodeanalisis + '}';
    }
  
}
