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

        public DataTable BuscaUma(Int64 codigoUma)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable cabUma = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT MOV.NUMBONUS, MOV.CODIGOUMA, MOV.CODPROD, P.CODAUXILIAR AS EAN, CODAUXILIAR2 AS DUN, MOV.QT, MOV.DTVALIDADE, CONF.DATACONF, CONF.CODFUNCCONF");
                query.Append("  FROM PCMOVENDPEND MOV INNER JOIN PCPRODUT P ON(MOV.CODPROD = P.CODPROD) ");
                query.Append("                        LEFT OUTER JOIN TAB_ENDERECAMENTO_CONF CONF ON(MOV.NUMBONUS = CONF.NUMBONUS AND MOV.CODIGOUMA = CONF.CODIGOUMA)");
                query.Append($"WHERE MOV.CODIGOUMA = {codigoUma}");
                query.Append("   AND MOV.DTESTORNO IS NULL");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(cabUma);

                return cabUma;
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

        public Boolean ConfereUma(int numBonus, int codigoUma, int codBarra, int qtConf, string dataValidade, int codFuncConf)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("INSERT INTO TAB_ENDERECAMENTO_CONF (NUMBONUS, CODPROD, DATACONF, DATAVALIDADE, CODFUNCCONF, CODIGOUMA, QT)");
                query.Append($"                           VALUES ({numBonus}, {codBarra}, SYSDATE, TO_DATE('{dataValidade}', 'DD/MM/YYYY'), {codFuncConf}, {codigoUma}, {qtConf})");

                exec.CommandText = query.ToString();
                OracleDataReader insereConfUma = exec.ExecuteReader();

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
                query.Append("   AND bo.dtfechamento IS NULL");
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

                query.Append("SELECT CONF.NUMBONUS, CONF.CODPROD, P.DESCRICAO, SUM(CONF.QT) QTENTRADA, SUM(BI.QTAVARIA) AS QTAVARIA, TO_DATE(DECODE(CONF.DATAVALIDADE, '30/12/1899', NULL, CONF.DATAVALIDADE)) DTVALIDADE, ");
                query.Append("       CASE WHEN SUM(CONF.QT) = 0 THEN 'NÃO INICIADA'");
                query.Append("            WHEN SUM(CONF.QT) <> BI.QTNF THEN 'PENDENTE'");
                query.Append("            WHEN SUM(CONF.QT) = BI.QTNF THEN 'CONCLUIDO'");
                query.Append("        END STATUS");
                query.Append("  FROM PCBONUSI BI INNER JOIN PCBONUSICONF CONF ON (BI.NUMBONUS = CONF.NUMBONUS AND BI.CODPROD = CONF.CODPROD)");
                query.Append("                   INNER JOIN PCPRODUT P ON (CONF.CODPROD = P.CODPROD)");
                query.Append($"WHERE CONF.NUMBONUS = {numBonus}");
                if (tipoBusca == "C")
                {
                query.Append("   AND NVL(CONF.ENDERECADO, 'N') = 'N'");
                }
                else
                {
                query.Append("   AND NVL(CONF.ENDERECADO, 'N') = 'S'");
                }
                query.Append(" GROUP BY CONF.NUMBONUS, CONF.CODPROD, P.DESCRICAO, CONF.DATAVALIDADE, BI.QTNF");
                query.Append(" ORDER BY CONF.CODPROD");

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
                query.Append("SELECT BI.CODPROD, P.DESCRICAO || ' - ' || P.EMBALAGEM AS DESCRICAO, P.QTUNIT, P.QTUNITCX, PF.LASTROPAL AS LASTRO, PF.ALTURAPAL AS CAMADA, ");
                query.Append("       PF.PRAZOVAL AS DIASVALIDADE, PF.PERCTOLERANCIAVAL AS SHELFLIFE, BI.QTNF, P.CODAUXILIAR, PF.QTTOTPAL AS NORMA, ROUND(MOD(BI.QTNF / PF.QTTOTPAL, 1), 6) AS RESTO");
                query.Append("  FROM PCBONUSI BI INNER JOIN PCPRODUT P ON (BI.CODPROD = P.CODPROD)");
                query.Append("                   INNER JOIN PCBONUSC BC ON (BI.NUMBONUS = BC.NUMBONUS)");
                query.Append("                   INNER JOIN PCPRODFILIAL PF ON (P.CODPROD = PF.CODPROD AND BC.CODFILIAL = PF.CODFILIAL)");
                query.Append($"WHERE BI.NUMBONUS = {numBonus}");
                query.Append($"  AND ((P.CODAUXILIAR = {codBarra}) OR (P.CODAUXILIAR2 = {codBarra}))");

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
        public Int64 Codauxiliar { get; set; }
        public int Qtnf { get; set; }
        public int Qtconf { get; set; }
        public int Qtavaria{ get; set; }
        public string Dtvalidade { get; set; }
        public int Codfuncconf { get; set; }
        public Boolean ConfereProdutoBonus(ConferenciaBonus dados)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleTransaction transacao = connection.BeginTransaction();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder conferenteCab = new StringBuilder();
            StringBuilder confereBonusi = new StringBuilder();
            StringBuilder confereBonusiConf = new StringBuilder();
            StringBuilder confereBonusiVolume = new StringBuilder();

            exec.Transaction = transacao;

            try
            {
                //Atribui conferente no cabeçalho do bônus
                conferenteCab.Append($"UPDATE PCBONUSC SET DATARM = TRUNC(SYSDATE), CODFUNCRM = {dados.Codfuncconf} WHERE NUMBONUS = {dados.Numbonus}");
                exec.CommandText = conferenteCab.ToString();
                OracleDataReader cabBonus = exec.ExecuteReader();

                //Confere item na PCBONUSI
                confereBonusi.Append($"UPDATE PCBONUSI SET QTENTRADA = QTENTRADA + {dados.Qtconf}, QTAVARIA = QTAVARIA + {dados.Qtavaria}, DTVALIDADE = TO_DATE('{dados.Dtvalidade}', 'DD/MM/YYYY') ");
                confereBonusi.Append($" WHERE NUMBONUS = {dados.Numbonus} AND CODPROD = {dados.Codprod}");

                exec.CommandText = confereBonusi.ToString();
                OracleDataReader bonusi = exec.ExecuteReader();


                //Confere item na PCBONUSICONF
                confereBonusiConf.Append("INSERT INTO PCBONUSICONF (NUMBONUS, CODPROD, DATACONF, DATAVALIDADE, CODFUNCCONF, NUMLOTE, QT, QTAVARIA, CODAUXILIAR)");
                confereBonusiConf.Append($"                 VALUES ({dados.Numbonus}, {dados.Codprod}, SYSDATE, TO_DATE('{dados.Dtvalidade}', 'DD/MM/YYYY'), {dados.Codfuncconf}, 1, {dados.Qtconf}, {dados.Qtavaria}, NULL)");

                exec.CommandText = confereBonusiConf.ToString();
                OracleDataReader bonusiconf = exec.ExecuteReader();

                //Confere item na PCBONUSIVOLUME
                confereBonusiVolume.Append("INSERT INTO PCBONUSIVOLUME (NUMBONUS, CODPROD, NUMLOTE, CODAGREGACAO, DTVALIDADE, QTPECAS, QTENTRADA, QTNF, TIPO, ");
                confereBonusiVolume.Append("                            CODIGOUMA, ENDERECADO, CODMOTIVO, DTLANCAMENTO, CODFILIALESTOQUE, CODFILIALGESTAO, CODFUNCCONFERENTE)");
                confereBonusiVolume.Append($"                   VALUES ({dados.Numbonus}, {dados.Codprod}, 1, NULL, TO_DATE('{dados.Dtvalidade}', 'DD/MM/YYYY'), 0, {dados.Qtconf}, {dados.Qtnf}, 'N', 0, 'N', 0, SYSDATE, NULL, NULL, {dados.Codfuncconf})");

                exec.CommandText = confereBonusiVolume.ToString();
                OracleDataReader bonusivolume = exec.ExecuteReader();

                transacao.Commit();
                return true;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    transacao.Rollback();
                    connection.Close();
                    throw new Exception(ex.ToString());
                }

                transacao.Rollback();
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

        public DataTable ExtratoBonus(int numBonus)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable extrato = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT BI.CODPROD, P.DESCRICAO, TO_CHAR(C.DATAVALIDADE, 'DD/MM/YYYY') AS DATAVALIDADE, SUM(C.QT) QT, SUM(C.QTAVARIA) QTAVARIA, C.CODFUNCCONF || ' - ' || FUNC.NOME AS FUNCCONF, ");
                query.Append("       PF.LASTROPAL AS LASTRO, PF.ALTURAPAL AS CAMADA");
                query.Append("  FROM PCBONUSC BC INNER JOIN PCBONUSI BI ON (BC.NUMBONUS = BI.NUMBONUS)");
                query.Append("                   INNER JOIN PCPRODUT P ON (BI.CODPROD = P.CODPROD)");
                query.Append("                   INNER JOIN PCPRODFILIAL PF ON (P.CODPROD = PF.CODPROD AND BC.CODFILIAL = PF.CODFILIAL)");
                query.Append("                   LEFT OUTER JOIN PCBONUSICONF C ON (BI.NUMBONUS = C.NUMBONUS AND BI.CODPROD = C.CODPROD)");
                query.Append("                   LEFT OUTER JOIN PCEMPR FUNC ON (C.CODFUNCCONF = FUNC.MATRICULA)");
                query.Append($"WHERE BC.NUMBONUS = {numBonus}");
                query.Append(" GROUP BY BI.CODPROD, P.DESCRICAO, C.DATAVALIDADE, C.CODFUNCCONF, FUNC.NOME, PF.LASTROPAL, PF.ALTURAPAL");
                query.Append(" ORDER BY BI.CODPROD");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(extrato);

                return extrato;
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

        public Boolean ReabreConfItemBonus(int numBonus, int codProd)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder reabreBonusi = new StringBuilder();
            StringBuilder reabreBonusiConf = new StringBuilder();
            StringBuilder reabreBonusiVolume = new StringBuilder();

            try
            {
                reabreBonusi.Append($"UPDATE PCBONUSI SET QTENTRADA = (SELECT NVL(SUM(QTENTRADA), 0) FROM PCBONUSIVOLUME WHERE NUMBONUS = {numBonus} AND CODPROD = {codProd} AND ENDERECADO = 'S'), ");
                reabreBonusi.Append($"                    QTAVARIA = 0, DTVALIDADE = NULL WHERE NUMBONUS = {numBonus} AND CODPROD = {codProd}");

                exec.CommandText = reabreBonusi.ToString();
                OracleDataReader bonusi = exec.ExecuteReader();


                reabreBonusiConf.Append($"DELETE FROM PCBONUSICONF WHERE NUMBONUS = {numBonus} AND CODPROD = {codProd} AND ENDERECADO = 'N'");

                exec.CommandText = reabreBonusiConf.ToString();
                OracleDataReader bonusiconf = exec.ExecuteReader();

                reabreBonusiVolume.Append($"DELETE FROM PCBONUSIVOLUME WHERE NUMBONUS = {numBonus} AND CODPROD = {codProd} AND ENDERECADO = 'N'");

                exec.CommandText = reabreBonusiVolume.ToString();
                OracleDataReader bonusivolume = exec.ExecuteReader();

                return true;
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

        public Boolean ReabreConfBonus(int numBonus)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder reabreBonusi = new StringBuilder();
            StringBuilder reabreBonusiConf = new StringBuilder();
            StringBuilder reabreBonusiVolume = new StringBuilder();

            try
            {
                reabreBonusi.Append($"UPDATE PCBONUSI SET QTENTRADA = (SELECT NVL(SUM(QTENTRADA), 0) FROM PCBONUSIVOLUME WHERE NUMBONUS = {numBonus} AND CODPROD = PCBONUSI.CODPROD AND ENDERECADO = 'S'), ");
                reabreBonusi.Append($"                    QTAVARIA = 0, DTVALIDADE = NULL WHERE NUMBONUS = {numBonus}");

                exec.CommandText = reabreBonusi.ToString();
                OracleDataReader bonusi = exec.ExecuteReader();


                reabreBonusiConf.Append($"DELETE FROM PCBONUSICONF WHERE NUMBONUS = {numBonus} AND ENDERECADO = 'N'");

                exec.CommandText = reabreBonusiConf.ToString();
                OracleDataReader bonusiconf = exec.ExecuteReader();

                reabreBonusiVolume.Append($"DELETE FROM PCBONUSIVOLUME WHERE NUMBONUS = {numBonus} AND ENDERECADO = 'N'");

                exec.CommandText = reabreBonusiVolume.ToString();
                OracleDataReader bonusivolume = exec.ExecuteReader();

                return true;
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

        public string EnderecaBonus(int numBonus, int codFilial, int codFunc)
        {
            string retorno;
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = new OracleCommand("FNC_ENDERECAMENTO_PROD", connection);

            try
            {
                exec.CommandType = CommandType.StoredProcedure;

                OracleParameter resposta = new OracleParameter("@Resposta", OracleDbType.Varchar2, 100);

                resposta.Direction = ParameterDirection.ReturnValue;

                exec.Parameters.Add(resposta);

                exec.Parameters.Add("pFILIAL", OracleDbType.Int32).Value = codFilial;
                exec.Parameters.Add("pNUMBONUS", OracleDbType.Int32).Value = numBonus;
                exec.Parameters.Add("pCODFUNCCONFERENTE", OracleDbType.Int32).Value = codFunc;

                exec.ExecuteNonQuery();

                if (resposta.Value != DBNull.Value)
                {
                    
                    retorno = resposta.Value.ToString();
                    return retorno;
                } 
                else
                {
                    return "Erro no endereçamento. Tente novamente mais tarde.";
                }
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
}
