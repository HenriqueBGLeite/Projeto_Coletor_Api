using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.Text;

namespace ProjetoColetorApi.Model
{
    public class Entrada
    {
        public int NumBonus { get; set; }
        public int Matricula { get; set; }
        public string Nome { get; set; }
        public int Funcao { get; set; }

        public string ConfereCxPlastica(int numOs, int numVol, int codFuncConf)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            string resposta = "Nenhum registro encontrado para a O.S.";
            string osValida;
            string caixaPlastica;

            DataTable validaCx = new DataTable();

            StringBuilder query = new StringBuilder();
            StringBuilder volume = new StringBuilder();

            try
            {
                query.Append("select case when dtfuncretorno is null then 'S' else 'N' end osValida, caixaplastica ");
                query.Append($" from pcvolumeos where numos = {numOs} and numvol = {numVol}");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(validaCx);

                if (validaCx.Rows.Count > 0)
                {
                    osValida = validaCx.Rows[0]["osValida"].ToString();
                    caixaPlastica = validaCx.Rows[0]["caixaplastica"].ToString();

                    if (caixaPlastica == "S")
                    {
                        if (osValida == "S")
                        {
                            volume.Append($"update pcvolumeos set codfuncretorno = {codFuncConf}, dtfuncretorno = sysdate where numos = {numOs} and numvol = {numVol}");

                            exec.CommandText = volume.ToString();
                            OracleDataReader finalizaOs = exec.ExecuteReader();

                            resposta = "S";
                            return resposta;
                        }
                        else
                        {
                            resposta = "O.S./Volume já conferidos. Caixa plástica já retornou.";
                            return resposta;
                        }
                    } 
                    else
                    {
                        resposta = "O.S./Volume não pertence à uma caixa plástica.";
                        return resposta;
                    }                    
                }
                else
                {
                    return resposta;
                }
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return ex.Message;
                }

                exec.Dispose();
                connection.Dispose();

                return ex.Message;
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
        
        public DataTable BuscaCabBonus(string tipoBonus, int codFilial)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable cabBonus = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {

                query.Append("SELECT bo.numbonus, MAX(nf.fornecedor) AS fornecedor, bo.placa");
                query.Append("  FROM pcbonusc bo LEFT OUTER JOIN pcnfent nf ON (bo.numbonus = nf.numbonus AND bo.codfilial = nf.codfilial)");
                query.Append("                   LEFT OUTER JOIN pcwms w ON (bo.numbonus = w.numbonus AND bo.codfilial = w.codfilial)");
                query.Append($"WHERE bo.codfilial = {codFilial}");
                if (tipoBonus == "E")
                {
                query.Append("   AND w.codoper <> 'ED'");
                }
                else
                {
                query.Append("   AND w.codoper = 'ED'");
                }
                query.Append("   AND bo.databonus > sysdate-60");
                query.Append("   AND w.dtcancel IS NULL");
                query.Append("   AND bo.codfuncfecha IS NULL");
                query.Append("   AND bo.codfunccancel IS NULL");
                query.Append(" GROUP BY bo.numbonus, bo.placa");
                query.Append(" ORDER BY bo.numbonus");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(cabBonus);

                return cabBonus;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();

                    throw new Exception(ex.ToString());
                }

                exec.Dispose();
                connection.Dispose();

                throw new Exception(ex.ToString());
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

        public DataTable BuscaEquipeBonus(int numBonus)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable equipeBonus = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {

                query.Append($"SELECT func.matricula, func.nome FROM logistica_bonus_equip eq INNER JOIN pcempr func ON (eq.matricula = func.matricula) WHERE eq.numbonus = {numBonus}");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(equipeBonus);

                return equipeBonus;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();

                    throw new Exception(ex.ToString());
                }

                exec.Dispose();
                connection.Dispose();

                throw new Exception(ex.ToString());
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

        public DataTable BuscaAjudanteBonus(int codFunc)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable ajudanteBonus = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {

                query.Append($"SELECT matricula, nome FROM pcempr WHERE matricula = {codFunc}");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(ajudanteBonus);

                return ajudanteBonus;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();

                    throw new Exception(ex.ToString());
                }

                exec.Dispose();
                connection.Dispose();

                throw new Exception(ex.ToString());
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

        public Boolean InsereEquipeBonus(List<Entrada> lista)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            try
            {
                lista.ForEach(list =>
                {
                    StringBuilder query = new StringBuilder();

                    query.Append($"SELECT count(*) FROM LOGISTICA_BONUS_EQUIP WHERE matricula = {list.Matricula} AND numbonus = {list.NumBonus}");

                    exec.CommandText = query.ToString();
                    OracleDataReader existeNaEquipe = exec.ExecuteReader();

                    if (existeNaEquipe.Read())
                    {
                        int count = existeNaEquipe.GetInt32(0);

                        if (count == 0)
                        {
                            StringBuilder insereEquipe = new StringBuilder();

                            insereEquipe.Append($"INSERT INTO LOGISTICA_BONUS_EQUIP (CODFUNCAO, DATA, MATRICULA, NUMBONUS) VALUES ({list.Funcao}, SYSDATE, {list.Matricula}, {list.NumBonus})");

                            exec.CommandText = insereEquipe.ToString();
                            OracleDataReader reader = exec.ExecuteReader();
                        }
                    }                    
                });

                return true;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return false;

                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

                return false;
                throw new Exception(ex.Message);
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

        public Boolean RemoveEquipeBonus(int codFunc, int numBonus)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder limpaEquipe = new StringBuilder();

            try
            {

                limpaEquipe.Append($"DELETE FROM LOGISTICA_BONUS_EQUIP WHERE matricula = {codFunc} and numbonus = {numBonus}");

                exec.CommandText = limpaEquipe.ToString();
                OracleDataReader deleteEquipe = exec.ExecuteReader();

                return true;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return false;

                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

                return false;
                throw new Exception(ex.Message);
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

        public DataTable BonusConfirmadoEnderecado(int numBonus, string tipoBusca)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable confirmados = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {

                query.Append("SELECT V.NUMBONUS, V.CODPROD, P.DESCRICAO, SUM(V.QT) QTENTRADA, SUM(BI.QTAVARIA) AS QTAVARIA, TO_DATE(DECODE(V.DATAVALIDADE, '30/12/1899', NULL, V.DATAVALIDADE)) DTVALIDADE, ");
                query.Append("       CASE WHEN SUM(V.QT) = 0 THEN 'NÃO INICIADA'");
                query.Append("            WHEN SUM(V.QT) <> BI.QTNF THEN 'PENDENTE'");
                query.Append("            WHEN SUM(V.QT) = BI.QTNF THEN 'CONCLUIDO'");
                query.Append("        END STATUS");
                query.Append("  FROM PCBONUSI BI INNER JOIN PCBONUSICONF V ON(BI.NUMBONUS = V.NUMBONUS AND BI.CODPROD = V.CODPROD)");
                query.Append("                   INNER JOIN PCPRODUT P ON(V.CODPROD = P.CODPROD)");
                query.Append($"WHERE V.NUMBONUS = {numBonus}");
                if (tipoBusca == "C")
                {
                query.Append("   AND NVL(V.ENDERECADO, 'N') = 'N'");
                }
                else
                {
                query.Append("   AND NVL(V.ENDERECADO, 'N') = 'S'");
                }
                query.Append(" GROUP BY V.NUMBONUS, V.CODPROD, P.DESCRICAO, V.DATAVALIDADE, BI.QTNF");
                query.Append(" ORDER BY V.CODPROD");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(confirmados);

                return confirmados;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();

                    throw new Exception(ex.ToString());
                }

                exec.Dispose();
                connection.Dispose();

                throw new Exception(ex.ToString());
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

        public DataTable BuscaProdutoBonus(int numBonus, string codBarra)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable produto = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {

                query.Append("SELECT BI.CODPROD, P.DESCRICAO || ' - ' || P.EMBALAGEM AS DESCRICAO, P.QTUNIT, P.QTUNITCX");
                query.Append("  FROM PCBONUSI BI INNER JOIN PCPRODUT P ON (BI.CODPROD = P.CODPROD)");
                query.Append($"WHERE BI.NUMBONUS = {numBonus}");
                query.Append($"  AND ((CODAUXILIAR = {codBarra}) OR(CODAUXILIAR2 = {codBarra}))");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(produto);

                return produto;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();

                    throw new Exception(ex.ToString());
                }

                exec.Dispose();
                connection.Dispose();

                throw new Exception(ex.ToString());
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

    public class ConferenciaBonus
    {
        public int Numbonus { get; set; }
        public int Codprod{ get; set; }
        public int Qtconf { get; set; }
        public int Qtavaria{ get; set; }
        public string Dtvalidade { get; set; }
        public int Codfuncconf { get; set; }
        public Boolean ConfereProdutoBonus(ConferenciaBonus dados)
        {
            //Verificar se o produto existe na PCBONUSI
            return true;
        }
    }
}
