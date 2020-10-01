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
        public int NumInvent { get; set; }
        public int InventOs { get; set; }
        public int CodEndereco { get; set; }
        public int MatDig { get; set; }
        public string Status { get; set; }
        public int Codprod { get; set; }
        public string Descricao { get; set; }
        public int Contagem { get; set; }
        public int Qtunitcx { get; set; }
        public string Embalagem { get; set; }
        public int? Lastro { get; set; }
        public int? Camada { get; set; }
        public string Datavalidade { get; set; }
        public Boolean Alteravalidade { get; set; }
        public int? Qtcx { get; set; }
        public int? Qtun{ get; set; }
        public int Total { get; set; }

        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }

        //MÉTODOS PARA INVENTARIO COM WMS
        public ProdutoInventario GetProduto(string produto, int filial)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            ProdutoInventario produtoInventario;
            StringBuilder query = new StringBuilder();
            ProdutoInventario pl = new ProdutoInventario();

            try
            {
                query.Append("SELECT P.CODPROD, P.DESCRICAO || ' - ' || P.EMBALAGEM as DESCRICAO, P.QTUNITCX");
                query.Append("  FROM PCPRODUT P INNER JOIN PCPRODFILIAL PF ON (P.CODPROD = PF.CODPROD)");
                query.Append($"WHERE ((P.CODPROD = {produto}) OR (P.CODAUXILIAR = {produto}) OR (P.CODAUXILIAR2 = {produto})) ");
                query.Append($"  AND PF.CODFILIAL = {filial}");

                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                if (reader.Read())
                {
                    pl.Codprod = reader.GetInt32(0);
                    pl.Descricao = reader.GetString(1);
                    pl.Qtunitcx = reader.GetInt32(2);
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

                connection.Close();

                return pl;

            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    pl.Erro = "S";
                    pl.MensagemErroWarning = ex.Message;

                    produtoInventario = pl;

                    connection.Close();
                    return produtoInventario;
                }
                exec.Dispose();
                connection.Dispose();
                throw ex;

            }
            finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();

            }

        }
        public Boolean GravaProduto(ProdutoInventario prod)
        {
            Boolean salvou = false;
            OracleConnection connection = DataBase.novaConexao();
            StringBuilder query = new StringBuilder();
            OracleCommand exec = new OracleCommand("STP_GERA_PROX_CONTAGEM", connection);

            try
            {
                string dtvalidade = null;

                dtvalidade = Convert.ToDateTime(prod.Datavalidade).ToString("dd-MM-yyyy");

                //Elimina valores que podem ser nulos
                if (prod.Lastro == null) { prod.Lastro = 0; }
                if (prod.Camada == null) { prod.Camada = 0; }
                if (prod.Qtun == null) { prod.Qtun = 0; }
                if (prod.Qtcx == null) { prod.Qtcx = 0; }

                exec.CommandType = CommandType.StoredProcedure;

                exec.Parameters.Add("P_NUMINVENT", OracleDbType.Int32).Value = prod.NumInvent;
                exec.Parameters.Add("P_INVENTOS", OracleDbType.Int32).Value = prod.InventOs;
                exec.Parameters.Add("P_CODPROD", OracleDbType.Int32).Value = prod.Codprod;
                exec.Parameters.Add("P_QTTOTAL", OracleDbType.Int32).Value = prod.Total;
                exec.Parameters.Add("P_QTUND", OracleDbType.Int32).Value = prod.Qtun;
                exec.Parameters.Add("P_QTCX", OracleDbType.Int32).Value = prod.Qtcx;
                exec.Parameters.Add("P_ENDERECO", OracleDbType.Int32).Value = prod.CodEndereco;
                exec.Parameters.Add("P_CONTAGEM", OracleDbType.Int32).Value = prod.Contagem;
                exec.Parameters.Add("P_DTVALIDADE", OracleDbType.Varchar2).Value = dtvalidade;
                exec.Parameters.Add("P_MATDIG", OracleDbType.Int32).Value = prod.MatDig;
                exec.Parameters.Add("P_STATUS", OracleDbType.Varchar2).Value = prod.Status;
                if (prod.Alteravalidade)
                {
                    exec.Parameters.Add("P_ALTERAVALIDADE", OracleDbType.Boolean).Value = true;
                }
                else
                {
                    exec.Parameters.Add("P_ALTERAVALIDADE", OracleDbType.Boolean).Value = false;
                }

                exec.ExecuteNonQuery();

                connection.Close();

                salvou = true;

                return salvou;
            }
            catch (Exception ex)
            {
                salvou = false;

                
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();

                return salvou;
            }
            finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();
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

        public DataTable GetProxOs(string codUsuario, int codEndereco = -1, int contagem = -1)
        {
            OracleConnection connection = DataBase.novaConexao();

            string proxOs;
            int minimaContagem;
            DataTable endereco = new DataTable();

            StringBuilder query = new StringBuilder();
            OracleCommand exec = connection.CreateCommand();


            try
            {
                OracleTransaction transacao = connection.BeginTransaction();
                exec.Transaction = transacao;

                query.Append($"select fnc_busca_prox_os_invent({codUsuario}, {codEndereco}, {contagem}) as endereco from dual");
                // Para testar, comentar a linha de cima e descomentar a debaixo
                // query.Append("select to_char(45947) as inventos from dual");

                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                if (reader.Read())
                {
                    proxOs = reader.GetString(0);

                    if (proxOs == "-1")
                    {
                        return null;
                    }
                    else
                    {
                        query = new StringBuilder();

                        query.Append($"SELECT NVL(MIN(CONTAGEM), -1) as contagem FROM pcinventenderecoi inv WHERE inv.inventos = {proxOs} AND NVL(inv.status, 'E') IN ('E', 'D') AND nvl(inv.qt, 0) = 0 AND INV.DTCONTFIM IS NULL");

                        exec.CommandText = query.ToString();
                        reader = exec.ExecuteReader();

                        if (reader.Read())
                        {
                            minimaContagem = reader.GetInt32(0);

                            query = new StringBuilder();

                            query.Append($"UPDATE EPCTI.PCINVENTENDERECOI SET MATDIG = {codUsuario} WHERE INVENTOS = {proxOs} AND CONTAGEM = {minimaContagem} AND DTCONTINI IS NULL");

                            exec.CommandText = query.ToString();
                            reader = exec.ExecuteReader();
                        }
                        else
                        {
                            minimaContagem = -1;
                        }

                        query = new StringBuilder();

                        query.Append($"select numinvent, inventos, codprod, descricao, ean, dun, contagem, status, codendereco, tipoender, deposito, rua, predio, nivel, apto, nvl(qtunitcx, 0) as qtunitcx");
                        query.Append($"  from (select inv.numinvent, inv.inventos, inv.codprod, prod.descricao, prod.codauxiliar as ean, prod.codauxiliar2 as dun, nvl(inv.qt, 0) as qt, inv.contagem, nvl(inv.status, 'E') as status,");
                        query.Append($"               rank() over(partition by inv.codendereco order by inv.contagem desc) as contagem_minima, en.codendereco, en.tipoender,");
                        query.Append($"               en.deposito, en.rua, en.predio, en.nivel, en.apto, prod.qtunitcx");
                        query.Append($"          from pcinventenderecoi inv inner join pcendereco en on (inv.codendereco = en.codendereco)");
                        query.Append($"                                     left outer join pcprodut prod on (inv.codprod = prod.codprod)");
                        query.Append($"         where inv.inventos = {proxOs} and nvl(inv.status, 'E') in ('E', 'D') and nvl(inv.qt, 0) = 0 and inv.dtcontfim is null");
                        query.Append($"         order by en.deposito, en.rua, case when mod(en.rua, 2) = 0 then en.predio end asc, case when mod(en.rua, 2) = 1 then en.predio end asc, ");
                        query.Append($"                  en.nivel, en.apto, inv.codprod, inv.codendereco, inv.contagem");
                        query.Append($"        )");
                        query.Append($" where contagem_minima = 1");

                        exec.CommandText = query.ToString();
                        OracleDataAdapter oda = new OracleDataAdapter(exec);
                        oda.SelectCommand = exec;
                        oda.Fill(endereco);


                        transacao.Commit();
                        connection.Close();

                        return endereco;
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();

                return null;
            }
            finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();
            }

        }
    }

    //METODOS PARA INVENTARIO SEM WMS (MRURAL)
    public class InventarioSemWms
    {
        public DataTable BuscaInventario(string filial, string matricula, string BASE)
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
