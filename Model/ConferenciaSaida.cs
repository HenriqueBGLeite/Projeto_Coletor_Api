using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace ProjetoColetorApi.Model
{
    public class ConferenciaSaida
    {
        public int Numos { get; set; }
        public int Numvol { get; set; }
        public int CodFuncConf { get; set; }
        public int? Codprod { get; set; }
        public int? Numbox { get; set; }
        public int? Qtconf { get; set; }

        public DataTable CabecalhoOs(int numOs, int numVol)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable osCabecalho = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select distinct mov.numos, nvl(mov.numpalete, 0) as numpalete, mov.numped, nvl(mov.numcar, 0) as numcar, ");
                query.Append($"               nvl(mov.numbox, 0) as numbox, nvl(vol.numvol, 0) as numvol, mov.tipoos, (select count(*) from pcvolumeos where numos = {numOs}) as totalOs, ");
                query.Append("                vol.dtconf, vol.codfuncconf, nvl(pend.pendencia, 0) as pendencia");
                query.Append($" from pcmovendpend mov inner join (select numos, numvol, dtconf, codfuncconf from pcvolumeos where numos = {numOs} and numvol = {numVol}) vol on (mov.numos = vol.numos)");
                query.Append("                        left outer join(select count(distinct codprod) pendencia, numos");
                query.Append("                                          from pcmovendpend");
                query.Append($"                                        where numos = {numOs}");
                query.Append("                                           and nvl(qt, 0) <> nvl(qtconferida, 0)");
                query.Append("                                           and dtfimconferencia is null");
                query.Append("                                         group by numos");
                query.Append("                                        ) pend on(mov.numos = pend.numos) ");
                query.Append($"where mov.numos  = {numOs}");
                query.Append($"  and vol.numvol = {numVol}");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(osCabecalho);

                return osCabecalho;         
            } 
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return osCabecalho;
                }

                exec.Dispose();
                connection.Dispose();

                return osCabecalho;
            } finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();
            }
        }

        public DataTable ProdutoOsVolume(string codBarra, int numOs, int numVol, int filial)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable prodOsVolume = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select nvl(os.datavolume, 'N') as conferido, case when os.dtconf2 is null then 'N' else 'S' end reconferido,");
                query.Append("       prod.codprod, osi.numvol, prod.codauxiliar as ean, prod.codauxiliar2 as dun, prod.qtunitcx");
                query.Append("  from pcprodut prod inner join pcprodfilial pf on (prod.codprod = pf.codprod)");
                query.Append("                     inner join pcvolumeosi osi on (prod.codprod = osi.codprod)");
                query.Append("                     inner join pcvolumeos os on (osi.numos = os.numos and osi.numvol = os.numvol) ");
                query.Append($"where pf.codfilial = {filial}");
                query.Append($"  and osi.numos = {numOs}");
                query.Append($"  and osi.numvol = {numVol}");
                query.Append($"  and prod.codauxiliar2 = {codBarra}");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(prodOsVolume);

                return prodOsVolume;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return prodOsVolume;
                }

                exec.Dispose();
                connection.Dispose();

                return prodOsVolume;
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

        public Boolean ConfereVolumeCheckout(ConferenciaSaida dados)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append($"UPDATE PCVOLUMEOS SET DTCONF = SYSDATE, CODFUNCCONF = {dados.CodFuncConf}, DATAVOLUME = 'S' WHERE NUMOS = {dados.Numos} AND NUMVOL = {dados.Numvol}");

                exec.CommandText = query.ToString();
                OracleDataReader updateVolumeConf = exec.ExecuteReader();
                
                connection.Close();
                return true;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return false;
                }

                exec.Dispose();
                connection.Dispose();

                return false;
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

        public Boolean ConfereVolumeCaixaFechada(ConferenciaSaida dados)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleTransaction transacao = connection.BeginTransaction();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder dadosConf = new StringBuilder();
            StringBuilder updateVolumeConf = new StringBuilder();
            StringBuilder updateConf;

            exec.Transaction = transacao;

            try
            {
                dadosConf.Append("select numped, nvl(qtconferida,0) as qtconferida, nvl(qt,0) as qt, codendereco, ");
                dadosConf.Append($"       (select count(*) from pcmovendpend where codprod = {dados.Codprod} and numos = {dados.Numos} and nvl(qtconferida, 0) <> nvl(QT, 0)) as contador, ");
                dadosConf.Append("       (nvl(qt, 0) - nvl(qtconferida, 0)) as qt_sem_conf ");
                dadosConf.Append("  from pcmovendpend ");
                dadosConf.Append($"where codprod = {dados.Codprod}");
                dadosConf.Append($"  and numos   = {dados.Numos}");
                dadosConf.Append($"  and numbox  = {dados.Numbox}");
                dadosConf.Append("   and nvl(qtconferida, 0) <> nvl(QT, 0)");

                exec.CommandText = dadosConf.ToString();
                OracleDataReader confDados = exec.ExecuteReader();

                while (confDados.Read())
                {
                    int cont = 0;
                    Int64 numPed = confDados.GetInt64(0);
                    int qtConferida = confDados.GetInt32(1);
                    int qt = confDados.GetInt32(2);
                    int codEndereco = confDados.GetInt32(3);
                    int contador = confDados.GetInt32(4);
                    int qtUpdate = confDados.GetInt32(5);

                    if (qtUpdate <= dados.Qtconf)
                    {
                        if (contador > 0)
                        {
                            cont++;
                        }

                        if (cont == contador)
                        {
                            qtUpdate = (int) dados.Qtconf;

                            if (contador > 0)
                            {
                                updateConf = new StringBuilder();

                                updateConf.Append($"update pcmovendpend set codfuncconf = {dados.CodFuncConf}, qtconferida = nvl(qtconferida,0) + {qtUpdate}, dtinicioconferencia = sysdate ");
                                updateConf.Append($" where codprod = {dados.Codprod} and numos = {dados.Numos} and numbox = {dados.Numbox} and nvl(qtconferida, 0) <> nvl(qt, 0)");

                                exec.CommandText = updateConf.ToString();
                                OracleDataReader updateExecConf1 = exec.ExecuteReader();
                            } 
                            else
                            {
                                updateConf = new StringBuilder();

                                updateConf.Append($"update pcmovendpend set codfuncconf = {dados.CodFuncConf}, qtconferida = nvl(qtconferida,0) + {qtUpdate}, dtinicioconferencia = sysdate ");
                                updateConf.Append($" where codprod = {dados.Codprod} and numos = {dados.Numos} and numbox = {dados.Numbox}");

                                exec.CommandText = updateConf.ToString();
                                OracleDataReader updateExecConf2 = exec.ExecuteReader();
                            }
                        }
                        else
                        {
                            qtConferida = qtConferida - (int) dados.Qtconf;

                            updateConf = new StringBuilder();

                            updateConf.Append($"update pcmovendpend set codfuncconf = {dados.CodFuncConf}, qtconferida = nvl(qtconferida,0) + {qtUpdate}, dtinicioconferencia = sysdate ");
                            updateConf.Append($" where codprod = {dados.Codprod} and numos = {dados.Numos} and numbox = {dados.Numbox} and nvl(qtconferida, 0) <> nvl(qt, 0)");

                            exec.CommandText = updateConf.ToString();
                            OracleDataReader updateExecConf3 = exec.ExecuteReader();
                        }
                    }
                    else
                    {
                        qtUpdate = (int) dados.Qtconf;

                        updateConf = new StringBuilder();

                        updateConf.Append($"update pcmovendpend set codfuncconf = {dados.CodFuncConf}, qtconferida = nvl(qtconferida,0) + {qtUpdate}, dtinicioconferencia = sysdate ");
                        updateConf.Append($" where codprod = {dados.Codprod} and numos = {dados.Numos} and numbox = {dados.Numbox} and nvl(qtconferida, 0) <> nvl(qt, 0)");

                        exec.CommandText = updateConf.ToString();
                        OracleDataReader updateExecConf4 = exec.ExecuteReader();
                    }
                }

                updateVolumeConf.Append($"update pcvolumeos set datavolume = 'S', codfuncconf = {dados.CodFuncConf}, dtconf = sysdate where numvol = {dados.Numvol} and numos = {dados.Numos}");
                exec.CommandText = updateVolumeConf.ToString();
                OracleDataReader execUpdateVolumeConf = exec.ExecuteReader();

                transacao.Commit();
                connection.Close();
                return true;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    transacao.Rollback();
                    connection.Close();
                    return false;
                }

                exec.Dispose();
                connection.Dispose();

                return false;
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

        public DataTable buscaQtOsPendente(int numOs, int numVol)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable qtOsPendente = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select count(*) as pendencia "); 
                query.Append("  from (select codprod");
                query.Append("          from (select mov.codprod, nvl(mov.qt, 0) as qtped, nvl(mov.qtconferida, 0) as qtconf");
                query.Append("                  from pcmovendpend mov");
                query.Append($"                where mov.numos  = {numOs}");
                query.Append($"                  and mov.numbox = {numVol}");
                query.Append("                   and nvl(mov.qtconferida, 0) <> nvl(mov.qt, 0))");
                query.Append(" where qtconf <> qtped");
                query.Append(" group by codprod)");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(qtOsPendente);

                return qtOsPendente;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return qtOsPendente;
                }

                exec.Dispose();
                connection.Dispose();

                return qtOsPendente;
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

        public Boolean FinalizaConferenciaOs(int numOs)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append($"update pcmovendpend set dtfimconferencia = sysdate where numos = {numOs} and dtfimconferencia is null");

                exec.CommandText = query.ToString();
                OracleDataReader finalizaOs = exec.ExecuteReader();

                connection.Close();
                return true;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return false;
                }

                exec.Dispose();
                connection.Dispose();

                return false;
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

        public DataTable DivergenciaOs(int numOs)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable divergenciaOs = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select mov.numos, tab.codprod, tab.descricao, en.rua, en.predio, en.nivel, en.apto, tipo.descricao as tipoOs, func.nome as separador");
                query.Append("  from pcmovendpend mov inner join (select sum(nvl(mov.qtconferida, 0)) as qtconferida, prod.codprod, prod.descricao");
                query.Append("                                      from pcprodut prod left outer join pcmovendpend mov on(prod.codprod = mov.codprod)");
                query.Append($"                                    where mov.numos = {numOs}");
                query.Append("                                     group by prod.codprod, prod.descricao) tab on (mov.codprod = tab.codprod)");
                query.Append("                        inner join pcendereco en on (mov.codendereco = en.codendereco and mov.codfilial = en.codfilial)");
                query.Append("                        inner join pctipoos tipo on (mov.tipoos = tipo.codigo)");
                query.Append("                        left outer join pcempr func on (func.matricula = mov.codfuncos)");
                query.Append($"where mov.numos = {numOs}");
                query.Append("   and nvl(mov.qtconferida, 0) <> nvl(mov.qt, 0)");
                query.Append(" order by nvl(mov.numpalete, 0), mov.numos, en.rua, case when mod(en.rua, 2) = 1 then en.predio end asc, case when mod(en.rua, 2) = 0 then en.predio end desc, en.nivel, en.apto");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(divergenciaOs);

                return divergenciaOs;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return divergenciaOs;
                }

                exec.Dispose();
                connection.Dispose();

                return divergenciaOs;
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

        public DataTable PendenciaOsCarregamento(int numcar)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable divergenciaOs = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT * FROM ( ");
                query.Append("SELECT MOV.NUMOS, PROD.CODPROD, PROD.DESCRICAO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, T.DESCRICAO AS TIPOOS, P.NOME AS SEPARADOR, NVL(MOV.NUMPALETE, 0) AS NUMPALETE");
                query.Append("  FROM PCMOVENDPEND MOV INNER JOIN PCTIPOOS T ON (T.CODIGO = MOV.TIPOOS)");
                query.Append("                        INNER JOIN PCENDERECO EN ON (MOV.CODENDERECO = EN.CODENDERECO AND MOV.CODFILIAL = EN.CODFILIAL)");
                query.Append("                        LEFT OUTER JOIN PCPRODUT PROD ON (PROD.CODPROD = MOV.CODPROD)");
                query.Append("                        LEFT OUTER JOIN PCEMPR P ON (P.MATRICULA = MOV.CODFUNCOS)");
                query.Append($"WHERE NUMCAR = {numcar}");
                query.Append("   AND DTFIMCONFERENCIA IS NULL");
                query.Append("   AND TIPOOS <> '13'");
                query.Append("   AND DTESTORNO IS NULL");


                query.Append(" UNION ");


                query.Append("SELECT MOV.NUMOS, PROD.CODPROD, PROD.DESCRICAO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, T.DESCRICAO AS TIPOOS, P.NOME AS SEPARADOR, NVL(MOV.NUMPALETE, 0) AS NUMPALETE");
                query.Append("  FROM PCMOVENDPEND MOV INNER JOIN PCTIPOOS T ON (T.CODIGO = MOV.TIPOOS)");
                query.Append("                        INNER JOIN PCVOLUMEOS OS ON (OS.NUMOS = MOV.NUMOS)");
                query.Append("                        INNER JOIN PCENDERECO EN ON (MOV.CODENDERECO = EN.CODENDERECO AND MOV.CODFILIAL = EN.CODFILIAL)");
                query.Append("                        LEFT OUTER JOIN PCPRODUT PROD ON (PROD.CODPROD = MOV.CODPROD)");
                query.Append("                        LEFT OUTER JOIN PCEMPR P ON (P.MATRICULA = MOV.CODFUNCOS)");
                query.Append($"WHERE NUMCAR = {numcar}");
                query.Append("   AND TIPOOS = '13'");
                query.Append("   AND ((OS.DTCONF IS NULL AND OS.CODFUNCCONF IS NULL AND MOV.DTESTORNO IS NULL) OR(NVL(MOV.QTCONFERIDA, 0) <> NVL(MOV.QT, 0)))");
                query.Append(") ORDER BY NUMPALETE, NUMOS, RUA, CASE WHEN MOD(RUA, 2) = 1 THEN PREDIO END ASC, CASE WHEN MOD(RUA, 2) = 0 THEN PREDIO END DESC, NIVEL, APTO");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(divergenciaOs);

                return divergenciaOs;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return divergenciaOs;
                }

                exec.Dispose();
                connection.Dispose();

                return divergenciaOs;
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

        public Boolean ReabreConferenciaProduto(int numOs, int codProd)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleTransaction transacao = connection.BeginTransaction();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder updateOs = new StringBuilder();
            StringBuilder updateVolume = new StringBuilder();

            exec.Transaction = transacao;

            try
            {
                updateOs.Append($"update pcmovendpend set qtconferida = null, dtinicioconferencia = null, dtfimconferencia = null, codfuncconf = null where numos = {numOs} and codprod = {codProd}");

                exec.CommandText = updateOs.ToString();
                OracleDataReader execUpdateOs = exec.ExecuteReader();

                updateVolume.Append($"update pcvolumeos set datavolume = 'N', codfuncconf = null, dtconf = null where numos = {numOs} and numvol in (select numvol from pcvolumeosi where numos = {numOs} and codprod = {codProd})");
                exec.CommandText = updateVolume.ToString();
                OracleDataReader execUpdateVolume = exec.ExecuteReader();

                transacao.Commit();

                connection.Close();
                return true;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return false;
                }

                exec.Dispose();
                connection.Dispose();

                return false;
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

        public Boolean ReabreConferenciaOs(int numOs)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleTransaction transacao = connection.BeginTransaction();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder updateOs = new StringBuilder();
            StringBuilder updateVolume = new StringBuilder();

            exec.Transaction = transacao;

            try
            {
                updateOs.Append($"update pcmovendpend set qtconferida = null, dtinicioconferencia = null, dtfimconferencia = null, codfuncconf = null where numos = {numOs}");

                exec.CommandText = updateOs.ToString();
                OracleDataReader execUpdateOs = exec.ExecuteReader();

                updateVolume.Append($"update pcvolumeos set datavolume = 'N', codfuncconf = null, dtconf = null where numos = {numOs}");
                exec.CommandText = updateVolume.ToString();
                OracleDataReader execUpdateVolume = exec.ExecuteReader();

                transacao.Commit();

                connection.Close();
                return true;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return false;
                }

                exec.Dispose();
                connection.Dispose();

                return false;
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


