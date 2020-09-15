using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.Text;

namespace ProjetoColetorApi.Model
{
    public class Armazenagem
    {
        public DataTable BuscaProxBox(int codfunc)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable proxBox = new DataTable();

            StringBuilder query = new StringBuilder();
            StringBuilder registraConf = new StringBuilder();

            try
            {
                query.Append("select codbox, numbonus from (select nvl(mov.codbox, 0) as codbox, mov.numbonus");
                query.Append("                                from pcmovendpend mov inner join pcendereco en on (mov.codendereco = en.codendereco)");
                query.Append($"                                                     inner join (select codfilial from pcempr where matricula = {codfunc}) fil on (mov.codfilial = fil.codfilial)");
                query.Append("                                                      inner join tab_enderecamento_conf conf on (mov.numbonus = conf.numbonus and mov.codigouma = conf.codigouma and mov.codprod = conf.codprod)");
                query.Append("                               where mov.dtestorno is null");
                query.Append("                                 and mov.tipoos = 97");
                query.Append("                                 and mov.codrotina = 9844");
                query.Append("                                 and en.tipoender = 'AE'");
                query.Append("                                 and mov.posicao = 'P'");
                query.Append("                                 and mov.codfunccoferente is null");
                query.Append("                                 and nvl(mov.codenderecoorig, 0) = 0");
                query.Append("                                 and mov.codoper like 'E%'");
                query.Append("                               order by en.rua, CASE WHEN MOD(en.rua, 2) = 1 THEN en.predio END ASC, CASE WHEN MOD(en.rua, 2) = 0 THEN en.predio END DESC, en.nivel, en.apto)");
                query.Append(" where rownum = 1");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(proxBox);

                if (proxBox.Rows.Count > 0)
                {
                    registraConf.Append($"update pcmovendpend set codfunccoferente = {codfunc} where numbonus = {proxBox.Rows[0]["numbonus"]} and dtestorno is null and nvl(codenderecoorig, 0) = 0");
                    
                    exec.CommandText = registraConf.ToString();
                    OracleDataReader reader = exec.ExecuteReader();
                }

                return proxBox;
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

        public DataTable ProxOsTranspalete(int codigoUma, int numBonus)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable transpalete = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select numbonus, codendereco, codbox, rua, predio, nivel, apto, mesmoBonus ");
                query.Append("  from (select mov.codendereco, nvl(mov.codbox, 0) as codbox, en.rua, en.predio, en.nivel, en.apto, mov.numbonus,");
                query.Append($"              case when mov.numbonus = {numBonus} then 'S' else 'N' end mesmoBonus");
                query.Append("          from pcmovendpend mov inner join pcendereco en on (mov.codendereco = en.codendereco)");
                query.Append("                                inner join tab_enderecamento_conf conf on(mov.numbonus = conf.numbonus and mov.codigouma = conf.codigouma and mov.codprod = conf.codprod)");
                query.Append("         where mov.dtestorno is null");
                query.Append("           and mov.tipoos = 97");
                query.Append("           and mov.codrotina = 9844");
                query.Append("           and en.tipoender = 'AE'");
                query.Append("           and mov.posicao = 'P'");
                query.Append("           and nvl(mov.codenderecoorig, 0) = 0");
                query.Append("           and mov.codoper like 'E%'");
                query.Append($"          and nvl(mov.codigouma, 0) = {codigoUma}");
                query.Append("         order by en.rua, CASE WHEN MOD(en.rua, 2) = 1 THEN en.predio END ASC, CASE WHEN MOD(en.rua, 2) = 0 THEN en.predio END DESC, en.nivel, en.apto)");
                query.Append(" where rownum = 1");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(transpalete);

                return transpalete;
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

        public Boolean RegistraEndOrig(int codigoUma, int codEndereco, int codFuncConf)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleTransaction transacao = connection.BeginTransaction();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder query = new StringBuilder();
            StringBuilder enderecoOrig = new StringBuilder();
            StringBuilder produtividade = new StringBuilder();

            exec.Transaction = transacao;

            try
            {
                query.Append("select mov.numos");
                query.Append("  from pcmovendpend mov inner join pcendereco en on (mov.codendereco = en.codendereco)");
                query.Append($"where mov.codigouma = {codigoUma}");
                query.Append($"  and exists (select 1 from pcendereco where codendereco = {codEndereco} and rua = en.rua and tipoender = 'AP')");
                query.Append("   and mov.dtestorno is null");
                
                exec.CommandText = query.ToString();
                OracleDataReader achouEndereco = exec.ExecuteReader();

                if (achouEndereco.Read())
                {
                    enderecoOrig.Append($"update pcmovendpend set codenderecoorig = {codEndereco} where codigouma = {codigoUma}");

                    exec.CommandText = enderecoOrig.ToString();
                    OracleDataReader confereUma = exec.ExecuteReader();

                    produtividade.Append("insert into LOGISTICA_OS_EQUIPE (numos, codfuncao, matricula, data) ");
                    produtividade.Append($"values ({achouEndereco.GetInt32(0)}, '12', {codFuncConf}, sysdate)");

                    exec.CommandText = produtividade.ToString();
                    OracleDataReader execProdutividade = exec.ExecuteReader();

                    transacao.Commit();
                    connection.Close();

                    return true;
                }
                else
                {
                    return false;
                }

            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    transacao.Rollback();
                    connection.Close();
                    return false;
                }

                transacao.Rollback();
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

        public DataTable BuscaPendenciaBox(int numBonus)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable qtOsPendente = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select count(*) as pendencia ");
                query.Append("  from pcmovendpend mov inner join tab_enderecamento_conf conf on (mov.numbonus = conf.numbonus and mov.codigouma = conf.codigouma and mov.codprod = conf.codprod) ");
                query.Append($"where mov.numbonus = {numBonus}");
                query.Append("   and mov.dtestorno is null and nvl(mov.codenderecoorig, 0) = 0");

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

        public Boolean CancelarOpTranspalete(int numBonus)
        {
            Boolean cancelado = false;

            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append($"update pcmovendpend set codfunccoferente = null where numbonus = {numBonus} and dtestorno is null and nvl(codenderecoorig, 0) = 0");

                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                cancelado = true;
                return cancelado;
            }
            catch (Exception ex)
            {
                cancelado = false;

                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return cancelado;
                }

                exec.Dispose();
                connection.Dispose();

                return cancelado;
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
