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
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.FileWriter ; 
import java.io.IOException; 
/**
 *
 * @author Usuario
 */
public class Connector {
    
    private Connection connection;
    private Statement statement;
    private Statement stm;
    private Statement statementinterno;
    private ResultSet resultSet;
    private ResultSet rest;
    private ResultSetMetaData resmetadata;
    public boolean statusConection = false;
    ArrayList<String> cols = new ArrayList<String>();
    ArrayList<String> colsWithIds = new ArrayList<String>();
    
    private String tabelaFato;
    private String tabelaDimensao;
    private String colunaDimensao;
    private String idTabelaDimensaoRelacionamento;
    private String idTabelaFato;
    private String idTabelaDimensao;
    private int limitRows = 50;
    private String condicao;
    
    public Connector(){}
    
    public Connector(String tabelaFato, String tabelaDimensao, String colunaDimensao, 
                String idTabelaFato, String idTabelaDimensao){
       this.tabelaFato = tabelaFato;
       this.tabelaDimensao = tabelaDimensao;
       this.colunaDimensao = colunaDimensao;
       this.idTabelaFato = idTabelaFato;
       this.idTabelaDimensao = idTabelaDimensao;
    }
    
    public void createConnection(String url,String username, String password) {
        try {
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();
            stm = connection.createStatement();
            statementinterno = connection.createStatement();
            
        } catch (SQLException ex) {
            System.out.println(ex);
        }     
        
        statusConection = true;
    }
    
    public void closeConnection() {        
        try {
            resultSet.close();
            statement.close();
            connection.close();
        } catch (SQLException ex) {
            System.out.println(ex);
        }
    }
     
    public void executaTransformacao() throws SQLException {
        executarConsultaDeColunas();        
        creatTable("tabeladatamining", "colunareferencia");
        insereColunas();
        inserirValores();
        inserirNulos();
        System.out.println("Transformação concluída.");
    }
    
    public void executarConsultaDeColunas() throws SQLException {
        String where = "";
        
        if(condicao != null){
            where = " where ";
        }
        else{
            condicao = "";
        }
        
        String sql = "select distinct B." + colunaDimensao + ", B.idhierarquia from " + tabelaFato + " A" 
                        + " inner join " + tabelaDimensao + " B on A." + idTabelaDimensaoRelacionamento + " = B." + idTabelaDimensao + where + condicao + " limit " + 1590;
        
        System.out.println(sql);
        resultSet = statement.executeQuery(sql);
        
        while (resultSet.next()) {
            String auxCols = resultSet.getString(1);
            String auxIds = resultSet.getString(2);
            cols.add(auxCols);
            colsWithIds.add(auxIds);
        }
    }
    
    public void creatTable(String nomeDaTabela, String colunaReferencia) {
        if(!statusConection) {
            throw new IllegalStateException("Não conectado ao banco");
        }
        try {
            statement.executeUpdate("drop table " + nomeDaTabela);
            statement.executeUpdate("create table " + nomeDaTabela + "(" + colunaReferencia + " integer)");
        } catch (SQLException ex) {
            Logger.getLogger(Connector.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void insereColunas() throws SQLException {
        for (int i=0; i<cols.size(); i++) {
            String nomeColuna = corrigirNomeColuna(i, cols.get(i));
            //System.out.println(nomeColuna);
            statement.executeUpdate("alter table tabeladatamining add column " + nomeColuna + " varchar");
        }
    }
    
    public String corrigirNomeColuna(int i, String valor){
        valor =  Normalizer.normalize(valor, Normalizer.Form.NFD);
        valor = "I" + (i+1) + "_" + valor
                            .replaceAll(" ", "_")
                            .replaceAll("\\.", "")
                            .replaceAll("[^\\p{ASCII}]", "")
                            .replaceAll("/", "")
                            .replaceAll("-", "")
                            .replaceAll("\\(", "")
                            .replaceAll("\\)", "")
                            .replaceAll("__", "")
                            .replaceAll("\\,", "")                            
                            .replaceAll("\\:", "")                                           
                            .replaceAll("\\|", "")                                       
                            .replaceAll("\\'", "")                                    
                            .replaceAll("&", "");
        return valor.toUpperCase();
    }
    
    public void inserirValores() throws SQLException {        
        System.err.println("INSERINDO VALORES");
        String sql = "select distinct A." + idTabelaFato + " from " + tabelaFato + " A" + " order by A." + idTabelaFato + " limit " + limitRows;
        
        //System.out.println(sql);
        resultSet = statement.executeQuery(sql);
        
        while (resultSet.next()) {            
            String idColunaReferencia = resultSet.getString(1);            
            //System.out.println(colunaReferencia);           
            stm.execute("insert into tabeladatamining (colunaReferencia) values (" + idColunaReferencia + ")");
           
            ResultSet resultSetTemp = stm.executeQuery("select A." + idTabelaDimensao + " from " + tabelaFato + " A where A." + idTabelaFato + " = " + idColunaReferencia);
            
            while (resultSetTemp.next()) {
                String valorReferencia = resultSetTemp.getString(1);
                
                for(int i = 0; i < cols.size(); i++){
                    String nomeColuna = corrigirNomeColuna(i, cols.get(i));
                    if(colsWithIds.get(i).equals(valorReferencia)){
                       String sqlExecute = "update tabeladatamining set " + nomeColuna + " = 'SIM' where colunaReferencia = " + idColunaReferencia;
                       System.out.println(sqlExecute);
                       statementinterno.executeUpdate(sqlExecute);
                    }                      
                }  
            }
        }
    }   
    
    public void inserirNulos() throws SQLException {
        System.err.println("INSERINDO NULOS");
        resultSet = statement.executeQuery("select * from tabeladatamining");
        resmetadata = resultSet.getMetaData();

        statementinterno = connection.createStatement();
        for (int i=2; i<=resmetadata.getColumnCount(); i++){
            System.err.println(i);
            statementinterno.executeUpdate("update tabeladatamining set " + resmetadata.getColumnName(i) + " = 'NAO' where " + resmetadata.getColumnName(i) + " is null");
        }
            
    }

    public void createFile() throws SQLException{        
        try {            
            FileWriter arquivo = new FileWriter ("tabeladatamining.arff");
            arquivo.write("@relation " + tabelaDimensao + "\n");
            executarConsultaDeColunas();
            
            for(int i = 0; i < cols.size(); i++){
                String nomeColuna = corrigirNomeColuna(i, cols.get(i));
                arquivo.write("@attribute " + nomeColuna  + " {" + nomeColuna + ",?}\n");
            }
            arquivo.write("\n@data\n");
            
            resultSet = statement.executeQuery("select * from tabeladatamining");

            while(resultSet.next()){
                String linha = "";
                for(int i = 0; i < cols.size(); i++){
                    String valor = resultSet.getString(2);
                    
                    if(!valor.equals("?")){
                        System.err.println(cols.get(i));
                        System.err.println(valor + ",");
                    }
                    else{
                        
                    }
                    
                }
                
            }               
            
            arquivo.close();
        } catch (IOException e) {
          System.out.println("An error occurred.");
          e.printStackTrace();
        }        
    }
    
    public String getTabelaFato() {
        return tabelaFato;
    }

    public void setTabelaFato(String tabelaFato) {
        this.tabelaFato = tabelaFato;
    }

    public String getTabelaDimensao() {
        return tabelaDimensao;
    }

    public void setTabelaDimensao(String tabelaDimensao) {
        this.tabelaDimensao = tabelaDimensao;
    }

    public String getColunaFato() {
        return colunaDimensao;
    }

    public void setColunaFato(String colunaFato) {
        this.colunaDimensao = colunaFato;
    }

    public String getIdTabelaFato() {
        return idTabelaFato;
    }

    public void setIdTabelaFato(String idTabelaFato) {
        this.idTabelaFato = idTabelaFato;
    }

    public String getIdTabelaDimensao() {
        return idTabelaDimensao;
    }

    public void setIdTabelaDimensao(String idTabelaDimensao) {
        this.idTabelaDimensao = idTabelaDimensao;
    }

    public int getLimitRows() {
        return limitRows;
    }

    public void setLimitRows(int limitRows) {
        this.limitRows = limitRows;
    }    

    public String getCondicao() {
        return condicao;
    }

    public void setCondicao(String condicao) {
        this.condicao = condicao;
    }

    public String getIdTabelaDimensaoRelacionamento() {
        return idTabelaDimensaoRelacionamento;
    }

    public void setIdTabelaDimensaoRelacionamento(String idTabelaDimensaoRelacionamento) {
        this.idTabelaDimensaoRelacionamento = idTabelaDimensaoRelacionamento;
    }
    
    
    
}
