using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjetoColetorApi.Model
{
    public class ProdutoInventario
    {
        public int Codprod { get; set; }
        public string Descricao { get; set; }
        public int Qtunitcx { get; set; }
        public string Embalagem { get; set; }
        public int? Lastro { get; set; }
        public int? Camada { get; set; }
        public string Datavalidade { get; set; }
        public int? Qtcx { get; set; }
        public int? Qtun{ get; set; }
        public int Total { get; set; }

        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }

        //MÉTODOS PARA INVENTARIO COM WMS
        public ProdutoInventario getProduto(string produto, int filial)
        {

            ProdutoInventario produtoInventario = new ProdutoInventario();

            StringBuilder query = new StringBuilder();

            ProdutoInventario pl = new ProdutoInventario();

            try
            {

                OracleConnection con = DataBase.novaConexao();

                OracleCommand cmd = con.CreateCommand();

                query.Append("SELECT P.CODPROD, P.DESCRICAO || ' - ' || P.EMBALAGEM as DESCRICAO, P.QTUNITCX, P.EMBALAGEM, PF.LASTROPAL AS LASTRO, PF.ALTURAPAL AS CAMADA ");
                query.Append("  FROM PCPRODUT P INNER JOIN PCPRODFILIAL PF ON (P.CODPROD = PF.CODPROD)");
                query.Append($" WHERE((P.CODPROD = {produto}) OR (P.CODAUXILIAR = {produto}) OR (P.CODAUXILIAR2 = {produto})) ");
                query.Append($"   AND PF.CODFILIAL = {filial}");

                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    pl.Codprod = reader.GetInt32(0);
                    pl.Descricao = reader.GetString(1);
                    pl.Qtunitcx = reader.GetInt32(2);
                    pl.Embalagem = reader.GetString(3);
                    pl.Lastro = reader.GetInt32(4);
                    pl.Camada = reader.GetInt32(5);
                    pl.Erro = "N";
                    pl.Warning = "N";
                    pl.MensagemErroWarning = null;
                }
                else
                {
                    pl.Erro = "N";
                    pl.Warning = "S";
                    pl.MensagemErroWarning = "Produto não encontrado";

                    produtoInventario = pl;
                }

                con.Close();

                return pl;

            }
            catch (Exception e)
            {

                pl.Erro = "S";
                pl.MensagemErroWarning = e.Message;

                produtoInventario = pl;

                return produtoInventario;
            }

        }
        public Boolean gravaProduto(ProdutoInventario prod)
        {
            Boolean salvou = false;
            StringBuilder query = new StringBuilder();

            try
            {
                OracleConnection con = DataBase.novaConexao();

                OracleCommand cmd = con.CreateCommand();

                string dtvalidade = null;

                dtvalidade = Convert.ToDateTime(prod.Datavalidade).ToString("dd-MM-yyyy");

                //Elimina valores que podem ser nulos
                if (prod.Lastro == null) { prod.Lastro = 0; }
                if (prod.Camada == null) { prod.Camada = 0; }
                if (prod.Qtun == null) { prod.Qtun = 0; }
                if (prod.Qtcx == null) { prod.Qtcx = 0; }

                //INSERT 
                query.Append("INSERT INTO EPCTI.TAB_PROJETO_COLETOR_INVENTARIO (CODPROD, DESCRICAO, QTUNITCX, EMBALAGEM, LASTRO, CAMADA, DTVALIDADE, QTCX, QTUN, TOTAL)");
                query.Append($"VALUES ({prod.Codprod}, SUBSTR('{prod.Descricao}', 1,40),{prod.Qtunitcx}, '{prod.Embalagem}', nvl({prod.Lastro},0), nvl({prod.Camada},0),");
                query.Append($"TO_DATE('{dtvalidade}','DD-MM-YYYY'),{prod.Qtcx},{prod.Qtun},{prod.Total})");


                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                salvou = true;

                con.Close();

                return salvou;
            }
            catch (Exception)
            {
                salvou = false;
                return salvou;
            }

        }
        
    }

    public class EnderecoInventario
    {
        public int Codigo { get; set; }
        public string TipoEndereco { get; set; }
        public int Deposito { get; set; }
        public int Rua { get; set; }
        public int Predio { get; set; }
        public int Nivel { get; set; }
        public int Apto { get; set; }
        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }
        public EnderecoInventario getDadosEndereco(int codEndereco)
        {
            StringBuilder query = new StringBuilder();

            EnderecoInventario end = new EnderecoInventario();

            try
            {
                OracleConnection con = DataBase.novaConexao();

                OracleCommand cmd = con.CreateCommand();

                query.Append($"select to_number(codendereco) as codendereco, tipoender, deposito, rua, predio, nivel, apto from pcendereco where codendereco = {codEndereco}");

                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    end.Codigo = reader.GetInt32(0);
                    end.TipoEndereco = reader.GetString(1);
                    end.Deposito = reader.GetInt32(2);
                    end.Rua = reader.GetInt32(3);
                    end.Predio = reader.GetInt32(4);
                    end.Nivel = reader.GetInt32(5);
                    end.Apto= reader.GetInt32(6);
                    end.Erro = "N";
                    end.Warning = "N";

                }
                else
                {
                    end.Warning = "S";
                    end.MensagemErroWarning = "Nenhum endereço encontrado!";
                }

                con.Close();

                return end;
            }
            catch (Exception e)
            {
                end.MensagemErroWarning = e.Message;

                return end;
            }

        }

        public DataTable getProxOs(string codUsuario, int codEndereco, int contagem)
        {
            DataTable endereco = new DataTable();
            string proxOs;

            StringBuilder query = new StringBuilder();

            try
            {
                OracleConnection con = DataBase.novaConexao();

                OracleCommand cmd = con.CreateCommand();

                // query.Append($"select fnc_busca_prox_os_invent({codUsuario}) as endereco from dual");
                // Para testar, comentar a linha de cima e descomentar a debaixo
                query.Append("select to_char(45947) as inventos from dual");

                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    proxOs = reader.GetString(0);

                    if (proxOs == "-1")
                    {
                        return null;
                    }
                    else
                    {
                        StringBuilder queryOs = new StringBuilder(); 
                        OracleCommand os = con.CreateCommand();

                        queryOs.Append($"select * from (");
                        queryOs.Append($"               select inv.numinvent, inv.codprod, nvl(inv.qt, 0) as qt, inv.contagem, min(contagem) over(partition by inv.codendereco, inv.codprod) as contagem_minima, inv.status, en.codendereco, en.tipoender, en.deposito, en.rua, en.predio, en.nivel, en.apto");
                        queryOs.Append($"                 from pcinventenderecoi inv inner join pcendereco en on (inv.codendereco = en.codendereco)");
                        queryOs.Append($"                where inv.inventos = {proxOs} and inv.status = 'D' and nvl(inv.qt, 0) = 0");
                        queryOs.Append($"                order by en.deposito, en.rua, case when mod(en.rua, 2) = 0 then en.predio end asc, case when mod(en.rua, 2) = 1 then en.predio end desc, en.nivel, en.apto, inv.codprod, inv.codendereco, inv.contagem");
                        queryOs.Append($"               )");
                        queryOs.Append($" where contagem = contagem_minima");

                        os.CommandText = queryOs.ToString();
                        OracleDataAdapter oda = new OracleDataAdapter(os);
                        oda.SelectCommand = os;
                        oda.Fill(endereco);
                        con.Close();

                        return endereco;
                    }
                }

                return null;
            }
            catch (Exception e)
            {
                throw e;
            }

        }
    }

    //METODOS PARA INVENTARIO SEM WMS (MRURAL)
    public class InventarioSemWms
    {
        public DataTable buscaInventario(string filial, string matricula, string BASE)
        {

            DataTable tb = new DataTable("TESTE");
            OracleConnection con = DataBase.novaConexao();
            OracleCommand cmd = con.CreateCommand();

            try
            {
                StringBuilder query = new StringBuilder();



                query.Append($"SELECT * FROM (SELECT NUMINVENT, DATA, COUNT(*) AS QDEPROD, nvl((select fnc_busca_prox_contagem('0',{filial},{matricula}) from dual), -1) as contagem,");
                query.Append("               (SELECT CASE WHEN INVENT_POR_AREA = 0 THEN 'N' ELSE 'S' END AS INVENT_POR_AREA FROM (");
                query.Append($"               SELECT COUNT(*) AS INVENT_POR_AREA FROM TAB_CONTROLE_AREA_INVENT WHERE CODFILIAL = {filial})) AS INVENT_POR_AREA,");
                query.Append($"              nvl((select  codarea from tab_controle_area_invent where CODFUNC = {matricula} and codfilial = {filial}),-1) as codarea ");
                query.Append($"  FROM PCINVENTROT WHERE DTATUALIZACAO IS NULL AND CODFILIAL = {filial} AND DATA > SYSDATE-10 GROUP BY NUMINVENT,DATA ORDER BY NUMINVENT DESC)");
                query.Append("  WHERE ROWNUM = 1");

                cmd.CommandText = query.ToString();


                cmd.Connection = con;
                OracleDataAdapter oDa = new OracleDataAdapter(cmd);
                oDa.SelectCommand = cmd;
                oDa.Fill(tb);

                return tb;

            }
            catch (Exception ex)
            {
                if (con.State == ConnectionState.Open)
                {
                    con.Close();
                }
                cmd.Dispose();
                con.Dispose();
                throw ex;

            }
            finally
            {
                if (con.State == ConnectionState.Open)
                {
                    con.Close();
                }
                cmd.Dispose();
                con.Dispose();

            }
        }
    }

}
