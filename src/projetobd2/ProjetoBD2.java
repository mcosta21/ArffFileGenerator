/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package projetobd2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author Usuario
 */
public class ProjetoBD2 {
    
    public static void main(String[] args) throws SQLException {
       
       String banco = "unico";
       String usuario = "postgres";
       String senha = "postgres";       
       
       String tabelaFato = "notafiscalitem";
       String idTabelaFato = "idnotafiscal";
       //String condicao = "valor between 60 and 100"; //Condição para inserir colunas (limite de 1590)
       String idTabelaDimensaoRelacionamento = "idproduto";
               
       String tabelaDimensao = "produto_dw";
       String colunaDimensao = "classificacao";       
       String idTabelaDimensao = "idproduto";
       
       Connector connector = new Connector();
       connector.setTabelaFato(tabelaFato);
       connector.setTabelaDimensao(tabelaDimensao);
       connector.setColunaFato(colunaDimensao);
       connector.setIdTabelaFato(idTabelaFato);
       connector.setIdTabelaDimensao(idTabelaDimensao);
       connector.setIdTabelaDimensaoRelacionamento(idTabelaDimensaoRelacionamento);
       //connector.setCondicao(condicao);
       connector.setLimitRows(20);
       
       connector.createConnection("jdbc:postgresql://localhost:5432/" + banco, usuario, senha);
       
       
       if(connector.statusConection == true){
           System.out.println("Conectado");
           //connector.executaTransformacao();
           connector.createFile();
       }
     
       
    }
    
   
}
