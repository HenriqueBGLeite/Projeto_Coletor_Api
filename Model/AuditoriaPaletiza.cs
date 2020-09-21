using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Net.Mail;
using System.Net;

namespace ProjetoColetorApi.Model
{
    public class AuditoriaPaletiza
    {
        public int NumCar { get; set; }
        public int NumPalete { get; set; }
        public int NumOs { get; set; }
        public int NumVol { get; set; }
        public int? CodProd { get; set; }
        public int? CodFilial { get; set; }
        public int CodFunc { get; set; }
        public string TipoConferencia { get; set; }

        public string ValidaCarregamento(int numCar)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            string existeCarga;
            DataTable carregamento = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append($"select case when count(*) = 1 then 'S' else 'N' end existeCarga from pccarreg where numcar = {numCar} and dt_cancel is null");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(carregamento);

                if (carregamento.Rows.Count > 0 && numCar != 0)
                {
                    existeCarga = carregamento.Rows[0]["existeCarga"].ToString();

                    return existeCarga;
                }
                else
                {
                    return "N";
                }
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

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

        public int PendenciasCarga(int numCar, string tipoConferencia)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            int qtdPendencia;
            DataTable divergenciaOs = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {   
                if (tipoConferencia == "P")
                {
                    query.Append("select count(os.numvol) as divergencia");
                    query.Append($" from (select distinct numos from pcmovendpend where numcar = {numCar} and dtestorno is null) mov inner join pcvolumeos os on (mov.numos = os.numos)");
                    query.Append("where os.datapalete is null");
                } else
                {
                    query.Append("select count(os.numvol) as divergencia");
                    query.Append($" from (select distinct numos from pcmovendpend where numcar = {numCar} and dtestorno is null) mov inner join pcvolumeos os on (mov.numos = os.numos)");
                    query.Append("where os.dtconf2 is null");
                }

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(divergenciaOs);

                if (divergenciaOs.Rows.Count > 0)
                {
                    qtdPendencia = Convert.ToInt32(divergenciaOs.Rows[0]["divergencia"]);
                    
                    return qtdPendencia;
                } else
                {
                    return -1;
                }
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

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

        public DataTable ProximoCliente(int numCar, int codFunc)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable proximoCli = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT letra, palete FROM (");
                query.Append("SELECT DISTINCT vol.letra, os.numpalete as palete, ped.numordemcarga");
                query.Append("  FROM pcvolumeos vol INNER JOIN (SELECT numos, numpalete, numcar, codoper, dtestorno, codfilial, numped");
                query.Append("                                  FROM  pcmovendpend");
                query.Append($"                                 WHERE numcar = {numCar}) os ON (vol.numos = os.numos)");
                query.Append("                      INNER JOIN pcpedc ped ON (os.numcar = ped.numcar AND os.numped = ped.numped) ");
                query.Append($"WHERE ped.numcar = {numCar}");
                query.Append($"   AND vol.datapalete IS NULL AND (vol.codfuncmontapalete IS NULL OR vol.codfuncmontapalete = {codFunc})");
                query.Append("   AND os.codoper = 'S'");
                query.Append("   AND os.dtestorno IS NULL");
                query.Append(" ORDER BY os.numpalete, ped.numordemcarga)");
                query.Append(" WHERE rownum = 1");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(proximoCli);

                return proximoCli;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

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

        public DataTable CabecalhoOs(AuditoriaPaletiza dados)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            string pertencecarga;
            int osaberta;
            string reconferido;
            DataTable osCabecalho = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select os.numos, prod.codauxiliar2 as dun, os.numpalete, os.numped, os.numcar, os.numbox, os.numvol, os.tipoos, os.dtconf, os.codfuncconf, os.pendencia as divergencia, pertenceCarga, reconferido, ");
                query.Append("       (SELECT COUNT(*) AS os FROM pcmovendpend");
                query.Append($"        WHERE numos = {dados.NumOs} AND dtfimconferencia IS NULL) AS osaberta,");
                query.Append("       (select count(os.numvol) as divergencia");
                query.Append("          from (select distinct numos from pcmovendpend where numcar = os.numcar and dtestorno is null) mov inner join pcvolumeos os on (mov.numos = os.numos)");
                query.Append("         where os.dtconf2 is null) as qtospendente");
                query.Append("  from (select mov.numos, mov.codprod, nvl(mov.numpalete, 0) as numpalete, mov.numped, nvl(mov.numcar, 0) as numcar, ");
                query.Append("               nvl(mov.numbox, 0) as numbox, nvl(vol.numvol, 1) as numvol, mov.tipoos, ");
                query.Append($"              case when mov.numcar = {dados.NumCar} then 'S' else 'N'end as pertenceCarga, ");
                query.Append("               case when vol.dtconf2 is not null then 'S' else 'N' end as reconferido, ");
                query.Append("               vol.dtconf2 as dtconf, ");
                query.Append("               vol.codfuncconf2 as codfuncconf, ");
                query.Append("               nvl(pend.pendencia, 0) as pendencia, mov.codprod as prod_os, vol.codprod as prod_vol");
                query.Append("          from pcmovendpend mov left outer join (select vos.numos, vos.numvol, vos.dtconf2, vos.codfuncconf2, vosi.codprod");
                query.Append("                                                   from pcvolumeos vos, pcvolumeosi vosi");
                query.Append("                                                  where vos.numos = vosi.numos(+)");
                query.Append("                                                    and vos.numvol = vosi.numvol(+)");
                query.Append($"                                                   and vos.numos = {dados.NumOs}");
                query.Append($"                                                   and vos.numvol = {dados.NumVol}) vol on (mov.numos = vol.numos)");
                query.Append("                                left outer join (select count(distinct numvol) pendencia, numos");
                query.Append("                                                   from pcvolumeos");
                query.Append($"                                                 where numos = {dados.NumOs}");
                query.Append("                                                    and dtconf2 is null");
                query.Append("                                                  group by numos");
                query.Append("                                                ) pend on (mov.numos = pend.numos)");
                query.Append($"        where mov.numos = {dados.NumOs}");
                query.Append("           and mov.dtestorno is null");
                query.Append("         order by case when mov.codprod = vol.codprod then 0 else 1 end");
                query.Append("       ) os inner join pcprodut prod on (os.codprod = prod.codprod)");
                query.Append(" where os.prod_os = nvl(os.prod_vol, os.prod_os)");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(osCabecalho);

                /*
                if (osCabecalho.Rows.Count > 0)
                {
                    pertencecarga = osCabecalho.Rows[0]["pertenceCarga"].ToString();
                    osaberta = Convert.ToInt32(osCabecalho.Rows[0]["osaberta"]);
                    reconferido = osCabecalho.Rows[0]["reconferido"].ToString();

                    if (pertencecarga == "S")
                    {
                        if (osaberta == 0)
                        {
                            if (dados.TipoConferencia == "A" && reconferido == "N")
                            {
                                return osCabecalho;
                            }
                            else
                            {
                                string resposta = "O.S. já foi reconferida.";
                                throw new Exception(resposta);
                            }
                        }
                        else
                        {
                            string resposta = "É necessário realizar a conferência para auditar/paletizar.";
                            throw new Exception(resposta);
                        }
                    }
                    else
                    {
                        string resposta = "O.S. não pertence ao carregamento.";
                        throw new Exception(resposta);
                    }
                } 
                */

                return osCabecalho;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw ex;
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

        public string PaletizaVolume(AuditoriaPaletiza dados)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            string resposta = "Nenhum registro encontrado para a O.S.";
            string pertencecarga; 
            int osaberta;
            int palete;
            string paletizado;
            string atribuidoFuncionario;

            DataTable osCabecalho = new DataTable();

            StringBuilder query = new StringBuilder();
            StringBuilder volume = new StringBuilder();
            StringBuilder registraConf = new StringBuilder();

            try
            {

                query.Append("select os.numos, os.numpalete, os.numped, os.numcar, os.numbox, os.numvol, os.tipoos, os.pendencia, pertenceCarga, paletizado, atribuidoFunc, ");
                query.Append("       (SELECT COUNT(*) AS os FROM pcmovendpend");
                query.Append($"        WHERE numos = {dados.NumOs} AND dtfimconferencia IS NULL) AS osaberta,");
                query.Append("       (select count(os.numvol) as divergencia");
                query.Append("          from (select distinct numos from pcmovendpend where numcar = os.numcar and dtestorno is null) mov inner join pcvolumeos os on (mov.numos = os.numos)");
                query.Append("         where os.datapalete is null) as qtospendente");
                query.Append("  from (select mov.numos, nvl(mov.numpalete, 0) as numpalete, mov.numped, nvl(mov.numcar, 0) as numcar, ");
                query.Append("               nvl(mov.numbox, 0) as numbox, nvl(vol.numvol, 1) as numvol, mov.tipoos, ");
                query.Append($"              case when mov.numcar = {dados.NumCar} then 'S' else 'N'end pertenceCarga, ");
                query.Append("               case when vol.datapalete is null then 'N' else 'S' end paletizado, ");
                query.Append("               case when vol.codfuncmontapalete is null then 'N' else 'S' end atribuidoFunc, ");
                query.Append("               nvl(pend.pendencia, 0) as pendencia, mov.codprod as prod_os, vol.codprod as prod_vol");
                query.Append("          from pcmovendpend mov left outer join (select vos.numos, vos.numvol, vos.datapalete, vos.codfuncmontapalete, vosi.codprod");
                query.Append("                                                   from pcvolumeos vos, pcvolumeosi vosi");
                query.Append("                                                  where vos.numos = vosi.numos(+)");
                query.Append("                                                    and vos.numvol = vosi.numvol(+)");
                query.Append($"                                                   and vos.numos = {dados.NumOs}");
                query.Append($"                                                   and vos.numvol = {dados.NumVol}");
                query.Append($"                                                   and (vos.codfuncmontapalete is null or vos.codfuncmontapalete = {dados.CodFunc})) vol on (mov.numos = vol.numos)");
                query.Append("                                left outer join (select count(distinct numvol) pendencia, numos");
                query.Append("                                                   from pcvolumeos");
                query.Append($"                                                 where numos = {dados.NumOs}");
                query.Append("                                                    and datapalete is null");
                query.Append("                                                  group by numos");
                query.Append("                                                ) pend on (mov.numos = pend.numos)");
                query.Append($"        where mov.numos = {dados.NumOs}");
                query.Append("           and mov.dtestorno is null");
                query.Append("         order by case when mov.codprod = vol.codprod then 0 else 1 end");
                query.Append("       ) os");
                query.Append(" where os.prod_os = nvl(os.prod_vol, os.prod_os)");

                /*
                query.Append("SELECT nvl(pend.numcar, 0) AS numcar, nvl(pend.numpalete, 0) AS numpalete, pend.numos, nvl(pend.numbox, 0) AS numbox, pend.tipoos, v.numvol,");
                query.Append($"      CASE WHEN pend.numcar = {dados.NumCar} THEN 'S' ELSE 'N' END AS pertenceCarga, ");
                query.Append("       CASE WHEN datapalete IS NULL THEN 'N' ELSE 'S' END AS paletizado, ");
                query.Append("       CASE WHEN codfuncmontapalete IS NULL THEN 'N' ELSE 'S' END AS atribuidoFunc, ");
                query.Append("       (SELECT COUNT(*) AS os");
                query.Append("          FROM pcmovendpend");
                query.Append($"        WHERE numos = {dados.NumOs}");
                query.Append("           AND dtfimconferencia IS NULL) AS osaberta");
                query.Append("  FROM pcvolumeos V INNER JOIN pcmovendpend pend ON (pend.numos = v.numos) ");
                query.Append($"WHERE v.numos = {dados.NumOs}");
                query.Append($"  AND v.numvol = {dados.NumVol}");
                query.Append($"  AND (v.codfuncmontapalete is null or v.codfuncmontapalete = {dados.CodFunc})");
                query.Append($"  AND pend.codfilial = {dados.CodFilial}");
                query.Append("   AND ROWNUM = 1");*/

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(osCabecalho);
                
                if (osCabecalho.Rows.Count > 0)
                {
                    pertencecarga = osCabecalho.Rows[0]["pertenceCarga"].ToString();
                    osaberta = Convert.ToInt32(osCabecalho.Rows[0]["osaberta"]);
                    paletizado = osCabecalho.Rows[0]["paletizado"].ToString();
                    atribuidoFuncionario = osCabecalho.Rows[0]["atribuidoFunc"].ToString();
                    palete = Convert.ToInt32(osCabecalho.Rows[0]["numpalete"]);

                    if (pertencecarga == "S")
                    {
                        if (osaberta == 0)
                        {
                             if (dados.TipoConferencia == "P" && paletizado == "N")
                            {
                                if (palete == dados.NumPalete)
                                {
                                    volume.Append($"update pcvolumeos set codfuncmontapalete = {dados.CodFunc}, datapalete = sysdate, numpalete = {dados.NumPalete} where numos = {dados.NumOs} and numvol = {dados.NumVol}");

                                    exec.CommandText = volume.ToString();
                                    OracleDataReader finalizaOs = exec.ExecuteReader();

                                    if (atribuidoFuncionario == "N")
                                    {
                                        registraConf.Append($"update pcvolumeos set codfuncmontapalete = {dados.CodFunc} ");
                                        registraConf.Append($" where exists (select 1 from pcmovendpend where numcar = {osCabecalho.Rows[0]["numcar"]} and numpalete = {osCabecalho.Rows[0]["numpalete"]}");
                                        registraConf.Append("                                             and numos = pcvolumeos.numos and dtestorno is null)");

                                        exec.CommandText = registraConf.ToString();
                                        OracleDataReader reader = exec.ExecuteReader();
                                    }

                                    resposta = "S";
                                    return resposta;
                                }
                                else
                                {
                                    resposta = "O.S. não pertence ao palete.";
                                    return resposta;
                                }
                            }
                             else
                            {
                                resposta = "O.S. já foi paletizada.";
                                return resposta;
                            }
                        }
                        else
                        {
                            resposta = "É necessário realizar a conferência para auditar/paletizar.";
                            return resposta;
                        }
                    } 
                    else
                    {
                        resposta = "O.S. não pertence ao carregamento.";
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
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

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

        public Boolean AuditaVolumeOs(int numOs, int numVol, int codFunc)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder registraConf = new StringBuilder();

            try
            {
                registraConf.Append($"update pcvolumeos set dtconf2 = sysdate, codfuncconf2 = {codFunc} ");
                registraConf.Append($" where numos = {numOs} and numvol = {numVol}");

                exec.CommandText = registraConf.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                return true;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

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

        public DataTable AtualizaDivergPend(int numCar, int numOs, string tipoConferencia)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            int qtdPendencia;
            DataTable divergenciaOs = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                if (tipoConferencia == "P")
                {
                    query.Append("select max(pendencia) as pendencia, max(divergencia) as divergencia");
                    query.Append("  from (select count(os.numvol) as pendencia, null as divergencia");
                    query.Append("          from (select distinct numos");
                    query.Append("                  from pcmovendpend");
                    query.Append($"                where numcar = {numCar}");
                    query.Append("                   and dtestorno is null) mov inner join pcvolumeos os on(mov.numos = os.numos)");
                    query.Append("         where os.datapalete is null");

                    query.Append("        union all ");

                    query.Append("        select null as pendencia, count(os.numvol) as divergencia");
                    query.Append("          from (select distinct numos");
                    query.Append("                  from pcmovendpend");
                    query.Append($"                where numos = {numOs}");
                    query.Append("                   and dtestorno is null) mov inner join pcvolumeos os on(mov.numos = os.numos)");
                    query.Append("                 where os.datapalete is null)");
                }
                else
                {
                    query.Append("select max(pendencia) as pendencia, max(divergencia) as divergencia");
                    query.Append("  from (select count(os.numvol) as pendencia, null as divergencia");
                    query.Append("          from (select distinct numos");
                    query.Append("                  from pcmovendpend");
                    query.Append($"                where numcar = {numCar}");
                    query.Append("                   and dtestorno is null) mov inner join pcvolumeos os on(mov.numos = os.numos)");
                    query.Append("         where os.dtconf2 is null");

                    query.Append("        union all ");

                    query.Append("        select null as pendencia, count(os.numvol) as divergencia");
                    query.Append("          from (select distinct numos");
                    query.Append("                  from pcmovendpend");
                    query.Append($"                where numos = {numOs}");
                    query.Append("                   and dtestorno is null) mov inner join pcvolumeos os on(mov.numos = os.numos)");
                    query.Append("                 where os.dtconf2 is null)");
                }

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(divergenciaOs);

                if (divergenciaOs.Rows.Count > 0)
                {
                    return divergenciaOs;
                }
                else
                {
                    throw new Exception("Nenhum registro foi localizado.");
                }
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

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

        public DataTable DivergenciaOs(int numOs, string tipoConferencia)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable divergencia = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select tab_os.letra, tab_os.numpalete, tab_os.numos, tab_os.numvol, tab.codprod, tab.descricao, en.rua, en.predio, en.nivel, en.apto, tipo.descricao as tipoOs, func.nome as separador");
                query.Append("  from (select mov.numped, os.letra, mov.codfilial, mov.numos, os.numvol, mov.codprod, mov.numpalete, mov.codendereco, mov.tipoos, mov.codfuncos");
                query.Append("          from pcmovendpend mov inner join pcvolumeos os on (mov.numos = os.numos)");
                query.Append("                                left outer join pcvolumeosi osi on (os.numos = osi.numos and os.numvol = osi.numvol and mov.codprod = osi.codprod)");
                query.Append($"        where mov.numos = {numOs}");
                if (tipoConferencia == "P")
                {
                        query.Append("   and os.datapalete IS NULL");
                }
                else
                {
                        query.Append("   and os.dtconf2 IS NULL");
                }

                query.Append("        union ");

                query.Append("        select mov.numped, null as letra, mov.codfilial, mov.numos, 1 as numvol, mov.codprod, mov.numpalete, mov.codendereco, mov.tipoos, mov.codfuncos");
                query.Append("          from pcmovendpend mov");
                query.Append($"         where mov.numos = {numOs}");
                query.Append("            and mov.tipoos = 17");
                query.Append("            and nvl(mov.qtconferida, 0) <> nvl(mov.qt, 0)");
                query.Append("       ) tab_os inner join (select sum(nvl(mov.qtconferida, 0)) as qtconferida, prod.codprod, prod.descricao");
                query.Append("                              from pcprodut prod left outer join pcmovendpend mov on(prod.codprod = mov.codprod)");
                query.Append($"                            where mov.numos = {numOs}");
                query.Append("                             group by prod.codprod, prod.descricao) tab on (tab_os.codprod = tab.codprod)");
                query.Append("                INNER JOIN PCPEDC PED ON (tab_os.NUMPED = PED.NUMPED AND tab_os.CODFILIAL = PED.CODFILIAL)");
                query.Append("                inner join pcendereco en on (tab_os.codendereco = en.codendereco and tab_os.codfilial = en.codfilial)");
                query.Append("                inner join pctipoos tipo on (tab_os.tipoos = tipo.codigo)");
                query.Append("                left outer join pcempr func on (func.matricula = tab_os.codfuncos)");
                query.Append(" order by tab_os.numpalete, ped.numordemcarga, tab_os.numos, tab_os.numvol, en.rua, case when mod(en.rua, 2) = 1 then en.predio end asc, case when mod(en.rua, 2) = 0 then en.predio end desc, en.nivel, en.apto");


                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(divergencia);

                return divergencia;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

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

        public DataTable PendenciaCarregamento(int numCar, string tipoConferencia)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable pendencia = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT TAB.LETRA, TAB.NUMOS, TAB.NUMVOL, PROD.CODPROD, PROD.DESCRICAO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, T.DESCRICAO AS TIPOOS, P.NOME AS SEPARADOR, NVL(TAB.NUMPALETE, 0) AS NUMPALETE ");
                query.Append("  FROM (SELECT MOV.NUMPED, OS.LETRA, MOV.CODFILIAL, MOV.NUMOS, MOV.CODPROD, OS.NUMVOL, NVL(MOV.NUMPALETE, 0) AS NUMPALETE, MOV.TIPOOS, MOV.CODENDERECO, MOV.CODFUNCOS");
                query.Append("          FROM PCMOVENDPEND MOV INNER JOIN PCVOLUMEOS OS ON (MOV.NUMOS = OS.NUMOS)");
                query.Append("                                INNER JOIN PCVOLUMEOSI OSI ON (OS.NUMOS = OSI.NUMOS AND OS.NUMVOL = OSI.NUMVOL AND MOV.CODPROD = OSI.CODPROD)");
                query.Append($"        WHERE MOV.NUMCAR = {numCar}");
                if (tipoConferencia == "P")
                {
                    query.Append("       AND os.datapalete IS NULL");
                }
                else
                {
                    query.Append("       AND os.dtconf2 IS NULL");
                }
                query.Append("           AND MOV.TIPOOS <> '13'");
                query.Append("           AND MOV.DTESTORNO IS NULL");
                query.Append("           AND MOV.DATA > SYSDATE - 30");

                query.Append("        UNION ");

                query.Append("        SELECT MOV.NUMPED, OS.LETRA, MOV.CODFILIAL, MOV.NUMOS, MOV.CODPROD, OS.NUMVOL, NVL(MOV.NUMPALETE, 0) AS NUMPALETE, MOV.TIPOOS, MOV.CODENDERECO, MOV.CODFUNCOS");
                query.Append("          FROM PCMOVENDPEND MOV INNER JOIN PCVOLUMEOS OS ON (OS.NUMOS = MOV.NUMOS)");
                query.Append($"        WHERE MOV.NUMCAR = {numCar}");
                query.Append("           AND MOV.TIPOOS = '13'");
                query.Append("           AND MOV.DTESTORNO IS NULL");
                if (tipoConferencia == "P")
                {
                    query.Append("       AND os.datapalete IS NULL");
                }
                else
                {
                    query.Append("       AND os.dtconf2 IS NULL");
                }
                query.Append("           AND MOV.DATA > SYSDATE - 30");

                query.Append("        UNION ");

                query.Append("        SELECT MOV.NUMPED, NULL AS LETRA, MOV.CODFILIAL, MOV.NUMOS, MOV.CODPROD, 1 AS NUMVOL, NVL(MOV.NUMPALETE, 0) AS NUMPALETE, MOV.TIPOOS, MOV.CODENDERECO, MOV.CODFUNCOS");
                query.Append("          FROM PCMOVENDPEND MOV");
                query.Append($"        WHERE MOV.NUMCAR = {numCar}");
                query.Append("           AND MOV.DTFIMCONFERENCIA IS NULL");
                query.Append("           AND MOV.TIPOOS = 17");
                query.Append("           AND MOV.DTESTORNO IS NULL");
                query.Append("           AND MOV.DATA > SYSDATE - 30) TAB INNER JOIN PCTIPOOS T ON (T.CODIGO = TAB.TIPOOS)");
                // query.Append("                                            INNER JOIN PCPEDC PED ON (TAB.NUMTRANSWMS = PED.NUMTRANSWMS AND TAB.CODFILIAL = PED.CODFILIAL)"); NOVO PROCESSO SEM PEDIDO NO WMS
                query.Append("                                            INNER JOIN PCPEDC PED ON (TAB.NUMPED = PED.NUMPED AND TAB.CODFILIAL = PED.CODFILIAL)"); 
                query.Append("                                            INNER JOIN PCENDERECO EN ON (TAB.CODENDERECO = EN.CODENDERECO AND TAB.CODFILIAL = EN.CODFILIAL)");
                query.Append("                                            INNER JOIN PCPRODUT PROD ON (PROD.CODPROD = TAB.CODPROD)");
                query.Append("                                            LEFT OUTER JOIN PCEMPR P ON (P.MATRICULA = TAB.CODFUNCOS)");
                query.Append(" ORDER BY TAB.NUMPALETE, PED.NUMORDEMCARGA, TAB.LETRA, TAB.NUMOS, TAB.NUMVOL, EN.RUA, CASE WHEN MOD(EN.RUA, 2) = 1 THEN EN.PREDIO END ASC, CASE WHEN MOD(EN.RUA, 2) = 0 THEN EN.PREDIO END DESC, EN.NIVEL, EN.APTO");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(pendencia);

                return pendencia;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

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
    }
}


