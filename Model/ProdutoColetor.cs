using Microsoft.AspNetCore.Mvc.Razor;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjetoColetorApi.Model
{
    public class ProdutoColetor
    {
        public int Codfilial { get; set; }
        public int Codprod { get; set; }
        public string Descricao { get; set; }
        public string Embalagemmaster { get; set; }
        public string Unidademaster { get; set; }
        public int Qtcx { get; set; }
        public Int64? Dun { get; set; }
        public string Embalagem { get; set; }
        public string Unidade { get; set; }
        public int Qtunit { get; set; }
        public Int64? Ean { get; set; }
        public double? Alt { get; set; }
        public double? Larg { get; set; }
        public double? Comp { get; set; }
        public double? AltUn { get; set; }
        public double? LargUn { get; set; }
        public double? CompUn { get; set; }
        public int? Lastro { get; set; }
        public int? Camada { get; set; }
        public int? Total { get; set; }
        public double? Peso { get; set; }
        public double? PesoUn { get; set; }
        public int? Capacidade { get; set; }
        public int? Reposicao { get; set; }
        public int? PrazoValidade { get; set; }
        public int? ShelfLife { get; set; }

        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }

        public Boolean editaDados(ProdutoColetor prod)
        {
            Boolean salvou = false;

            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder pcprodut = new StringBuilder();
            StringBuilder pcprodfilial = new StringBuilder();
            StringBuilder pcprodutpicking = new StringBuilder();

            try
            {
                OracleTransaction transacao = connection.BeginTransaction();
                exec.Transaction = transacao;

                //Elimina valores que podem ser nulos
                //PCPRODUT - Unidade
                if (prod.Ean == null) { prod.Ean = 0; }
                if (prod.AltUn == null) { prod.AltUn = 0; }
                if (prod.LargUn == null) { prod.LargUn = 0; }
                if (prod.CompUn == null) { prod.CompUn = 0; }
                if (prod.PesoUn == null) { prod.PesoUn = 0; }
                //PCPRODUT - Master
                if (prod.Embalagemmaster == null) { prod.Embalagemmaster = ""; }
                if (prod.Dun == null) { prod.Dun = 0; }
                if (prod.Alt == null) { prod.Alt = 0; }
                if (prod.Larg == null) { prod.Larg = 0; }
                if (prod.Comp == null) { prod.Comp = 0; }
                if (prod.Peso == null) { prod.Peso = 0; }
                //PCPRODFILIAL - Norma Palete
                if (prod.Lastro == null) { prod.Lastro = 0; }
                if (prod.Camada == null) { prod.Camada = 0; }
                if (prod.Total == null) { prod.Total = 0; }
                //PCPRODFILIAL - Validade
                if (prod.PrazoValidade == null) { prod.PrazoValidade = 0; }
                if (prod.ShelfLife == null) { prod.ShelfLife = 0; }
                //PCPRODUTOPICKING
                if (prod.Capacidade == null) { prod.Capacidade = 0; }
                if (prod.Reposicao == null) { prod.Reposicao = 0; }

                //CALCULA VOL. MASTER
                decimal volumeMaster = ((Convert.ToDecimal(prod.Alt) * Convert.ToDecimal(prod.Larg) * Convert.ToDecimal(prod.Comp)) / 1000000);
                decimal volumeUnit = ((Convert.ToDecimal(prod.AltUn) * Convert.ToDecimal(prod.LargUn) * Convert.ToDecimal(prod.CompUn)) / 1000000); 

                //UPDATE PCPRODUT 
                pcprodut.Append($"UPDATE PCPRODUT SET CODAUXILIAR = {prod.Ean}, CODAUXILIAR2 = {prod.Dun}, ALTURAARM = {prod.Alt.ToString().Replace(",", ".")}, LARGURAARM = {prod.Larg.ToString().Replace(",", ".")}, ");
                pcprodut.Append($"COMPRIMENTOARM = {prod.Comp.ToString().Replace(",", ".")}, VOLUMEARM = {volumeMaster.ToString().Replace(",", ".")}, PESOLIQMASTER = {prod.Peso.ToString().Replace(",", ".")}, PESOBRUTOMASTER = {prod.Peso.ToString().Replace(",", ".")}, ");
                pcprodut.Append($"VOLUME = {volumeUnit.ToString().Replace(",", ".")}, PESOLIQ = ROUND({prod.PesoUn.ToString().Replace(",", ".")},4), PESOBRUTO = ROUND({prod.PesoUn.ToString().Replace(",", ".")},4), ");
                pcprodut.Append($"ALTURAM3 = {prod.AltUn.ToString().Replace(",", ".")}, LARGURAM3 = {prod.LargUn.ToString().Replace(",", ".")}, COMPRIMENTOM3 = {prod.CompUn.ToString().Replace(",", ".")}");
                pcprodut.Append($" WHERE CODPROD = {prod.Codprod}");

                exec.CommandText = pcprodut.ToString();
                OracleDataReader produt = exec.ExecuteReader();

                //UPDATE PCPRODFILIAL
                pcprodfilial.Append($"UPDATE PCPRODFILIAL SET LASTROPAL = {prod.Lastro}, ALTURAPAL = {prod.Camada}, QTTOTPAL = ({prod.Total}), PRAZOVAL = {prod.PrazoValidade}, PERCTOLERANCIAVAL = {prod.ShelfLife}");
                pcprodfilial.Append($" WHERE CODPROD = {prod.Codprod} AND CODFILIAL = {prod.Codfilial}");

                exec.CommandText = pcprodfilial.ToString();
                OracleDataReader prodfilial = exec.ExecuteReader();

                //UPDATE PCPRODUTOPICKING
                pcprodutpicking.Append($"UPDATE PCPRODUTPICKING SET CAPACIDADE = {prod.Capacidade}, PONTOREPOSICAO = {prod.Reposicao}");
                pcprodutpicking.Append($" WHERE CODPROD = {prod.Codprod} AND CODFILIAL = {prod.Codfilial}");

                exec.CommandText = pcprodutpicking.ToString();
                OracleDataReader produtpicking = exec.ExecuteReader();

                salvou = true;

                transacao.Commit();
                connection.Close();

                return salvou;
            }
            catch (Exception ex)
            {
                salvou = false;

                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();                    
                    return salvou;
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

        public DataTable getProduto(string produto, int filial)
        {

            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            DataTable produtoColetor = new DataTable();

            ProdutoColetor p = new ProdutoColetor();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT TO_NUMBER(PF.CODFILIAL) AS CODFILIAL, P.CODPROD, P.DESCRICAO || ' - ' || P.EMBALAGEM as DESCRICAO, ");
                query.Append("       P.EMBALAGEM, P.UNIDADE, P.QTUNIT AS QTUNIT, CODAUXILIAR AS EAN, P.ALTURAM3 AS ALTUN, P.LARGURAM3 AS LARGUN, P.COMPRIMENTOM3 AS COMPUN, P.PESOBRUTO AS PESOUN, ");
                query.Append("       P.EMBALAGEMMASTER, P.UNIDADEMASTER, P.QTUNITCX AS QTCX, CODAUXILIAR2 AS DUN, P.ALTURAARM AS ALT, P.LARGURAARM AS LARG, P.COMPRIMENTOARM AS COMP, P.PESOBRUTOMASTER AS PESO, ");
                query.Append("       NVL(PF.LASTROPAL, 0) AS LASTRO, NVL(PF.ALTURAPAL, 0) AS CAMADA, PF.QTTOTPAL AS TOTAL, PK.CAPACIDADE AS CAPACIDADE, PK.PONTOREPOSICAO AS REPOSICAO, PF.PRAZOVAL AS PRAZOVALIDADE, PF.PERCTOLERANCIAVAL AS SHELFLIFE");
                query.Append("  FROM PCPRODUT P INNER JOIN PCPRODFILIAL PF ON(P.CODPROD = PF.CODPROD) LEFT OUTER JOIN PCPRODUTPICKING PK ON (P.CODPROD = PK.CODPROD AND PF.CODFILIAL = PK.CODFILIAL)");
                query.Append($" WHERE ((P.CODPROD = {produto}) OR (P.CODAUXILIAR = {produto}) OR (P.CODAUXILIAR2 = {produto})) ");
                query.Append($"   AND PF.CODFILIAL = {filial}");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(produtoColetor);
 
                return produtoColetor;
            }
            catch (Exception ex)
            {

                if (connection.State == ConnectionState.Open)
                {
                    p.Erro                = "S";
                    p.MensagemErroWarning = ex.Message;

                    connection.Close();

                    return null;
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

    public class ProdutoEndereco
    {
        public int CodEndereco { get; set; }
        public string TipoEndereco { get; set; }
        public int Deposito { get; set; }
        public int Rua { get; set; }
        public int Predio { get; set; }
        public int Nivel { get; set; }
        public int Apto { get; set; }
        public int Qt { get; set; }

        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }

        public List<ProdutoEndereco> getEnderecoProduto(string produto, int filial)
        {
            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            List<ProdutoEndereco> produtoEndereco = new List<ProdutoEndereco>();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select en.codendereco, en.tipoender, en.deposito, en.rua, en.predio, en.nivel, en.apto, est.qt");
                query.Append("  from pcendereco en inner join pcestendereco est on (en.codendereco = est.codendereco)");
                query.Append("                     inner join pcprodut prod on (est.codprod = prod.codprod)");
                query.Append($" where ((prod.codprod = {produto}) or (prod.codauxiliar = {produto}) or (prod.codauxiliar2 = {produto}))");
                query.Append($"   and en.codfilial = {filial}");
                query.Append(" order by decode(en.tipoender, 'AP', 'AAP'), en.deposito, en.rua, case when mod(en.rua, 2) = 1 then en.predio end asc, case when mod(en.rua, 2) = 0 then en.predio end desc, en.nivel, en.apto");


                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                while (reader.Read())
                {
                    ProdutoEndereco pend = new ProdutoEndereco();

                    pend.CodEndereco = reader.GetInt32(0);
                    pend.TipoEndereco = reader.GetString(1);
                    pend.Deposito     = reader.GetInt32(2);
                    pend.Rua          = reader.GetInt32(3);
                    pend.Predio       = reader.GetInt32(4);
                    pend.Nivel        = reader.GetInt32(5);
                    pend.Apto         = reader.GetInt32(6);
                    pend.Qt           = reader.GetInt32(7);

                    produtoEndereco.Add(pend);
                }

                connection.Close();

                return produtoEndereco;

            }
            catch (Exception ex)
            {

                if (connection.State == ConnectionState.Open)
                {
                    ProdutoEndereco pend = new ProdutoEndereco();

                    pend.Erro = "S";
                    pend.MensagemErroWarning = ex.Message;

                    produtoEndereco.Add(pend);


                    connection.Close();
                    return produtoEndereco;
                }

                exec.Dispose();
                connection.Dispose();

                return produtoEndereco;
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

    public class ProdutoEstoque
    {
        public string Codfilial { get; set; }
        public int Qtestger { get; set; }
        public int Qtreserv { get; set; }
        public int Qtbloq { get; set; }
        public int Qtavaria { get; set; }
        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }

        public List<ProdutoEstoque> getEstoqueProduto(string produto, int codFunc)
        {
            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            List<ProdutoEstoque> produtoEstoque = new List<ProdutoEstoque>();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select est.codfilial, nvl(est.qtestger,0) as qtestger, nvl(est.qtreserv,0) as qtreserv, nvl(est.qtbloqueada,0) as qtbloqueada, nvl(est.qtindeniz,0) as qtindeniz");
                query.Append("  from pcest est inner join pcfilial f on (est.codfilial = f.codigo)");
                query.Append("                 inner join pcprodut prod on (est.codprod = prod.codprod)");
                query.Append($" where ((prod.codprod = {produto}) or (prod.codauxiliar = {produto}) or (prod.codauxiliar2 = {produto}))");
                query.Append("   and f.dtexclusao is null");
                query.Append("   and f.codigo <> 99");
                query.Append($"  and f.codigo in (select codigoa from pclib where codfunc = {codFunc} and codtabela = 1 )");
                query.Append(" order by case when est.codfilial = 7 then '07' else est.codfilial end");


                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                while (reader.Read())
                {
                    ProdutoEstoque pe = new ProdutoEstoque();

                    pe.Codfilial = reader.GetString(0);
                    pe.Qtestger = reader.GetInt32(1);
                    pe.Qtreserv = reader.GetInt32(2);
                    pe.Qtbloq = reader.GetInt32(3);
                    pe.Qtavaria = reader.GetInt32(4);

                    produtoEstoque.Add(pe);
                }

                connection.Close();

                return produtoEstoque;

            }
            catch (Exception ex)
            {

                if (connection.State == ConnectionState.Open)
                {
                    ProdutoEstoque pe = new ProdutoEstoque();

                    pe.Erro = "S";
                    pe.MensagemErroWarning = ex.Message;

                    produtoEstoque.Add(pe);

                    connection.Close();
                    
                    return produtoEstoque;
                }

                exec.Dispose();
                connection.Dispose();

                return produtoEstoque;
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

    public class Filiais
    {
        public string Codfilial { get; set; }
        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }

        public List<Filiais> getFiliais(int codigo)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            List<Filiais> filiais = new List<Filiais>();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select f.codigo");
                query.Append("  from pcfilial f ");
                query.Append($"where f.dtexclusao is null");
                query.Append("   and f.codigo <> 99");
                query.Append($"  and f.codigo in (select codigoa from pclib where codfunc = {codigo} and codtabela = 1 )");
                query.Append(" order by case when f.codigo = 7 then '07' else f.codigo end");


                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                while (reader.Read())
                {
                    Filiais fil = new Filiais();

                    fil.Codfilial = reader.GetString(0);

                    filiais.Add(fil);
                }

                connection.Close();

                return filiais;

            }
            catch (Exception ex)
            {

                if (connection.State == ConnectionState.Open)
                {
                    Filiais fil = new Filiais();

                    fil.Erro = "S";
                    fil.MensagemErroWarning = ex.Message;

                    filiais.Add(fil);

                    connection.Close();

                    return filiais;
                }

                exec.Dispose();
                connection.Dispose();

                return filiais;
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

    public class ProdutoEnderecoPicking
    {
        public int Numreposicao { get; set; }
        public int Codfilial { get; set; }
        public int Codendereco { get; set; }
        public int Codfunc { get; set; }
        public int Codprod { get; set; }
        public string Descricao { get; set; }
        public int Qt { get; set; }
        public int Deposito { get; set; }
        public int Rua { get; set; }
        public int Predio { get; set; }
        public int Nivel { get; set; }
        public int Apto { get; set; }
        public Int64 Ean { get; set; }
        public Int64 Dun { get; set; }
        public int Qtunitcx { get; set; }


        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }

        public List<ProdutoEnderecoPicking> getListaReposicaoAberta(int codUsuario)
        {
            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            List<ProdutoEnderecoPicking> listaReposicao = new List<ProdutoEnderecoPicking>();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select rep.codfilial, rep.numreposicao, rep.codprod, prod.descricao, prod.codauxiliar as ean, prod.codauxiliar2 as dun, prod.qtunitcx, ");
                query.Append("       pk.codendereco, en.deposito, en.rua, en.predio, en.nivel, en.apto, rep.qt");
                query.Append("  from pcprodut prod inner join tab_logistica_reposicao rep on (prod.codprod = rep.codprod)");
                query.Append("                     inner join pcprodutpicking pk on (rep.codprod = pk.codprod and rep.codfilial = pk.codfilial)");
                query.Append("                     inner join pcendereco en on(pk.codendereco = en.codendereco and rep.codfilial = en.codfilial) ");
                query.Append($"where rep.codfunclista = {codUsuario}");
                query.Append("  and rep.dtconflista is null and rep.dtcancel is null and pk.tipo = 'V' ");
                query.Append("order by decode(en.tipoender, 'AP', 'AAP'), en.deposito, en.rua, case when mod(en.rua, 2) = 1 then en.predio end asc, case when mod(en.rua, 2) = 0 then en.predio end desc, en.nivel, en.apto");


                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                while (reader.Read())
                {
                    ProdutoEnderecoPicking lista = new ProdutoEnderecoPicking();

                    lista.Codfilial = reader.GetInt32(0);
                    lista.Numreposicao = reader.GetInt32(1);
                    lista.Codprod      = reader.GetInt32(2);
                    lista.Descricao    = reader.GetString(3);
                    lista.Ean          = reader.GetInt64(4);
                    lista.Dun          = reader.GetInt64(5);
                    lista.Qtunitcx     = reader.GetInt32(6);
                    lista.Codendereco  = reader.GetInt32(7);
                    lista.Deposito     = reader.GetInt32(8);
                    lista.Rua          = reader.GetInt32(9);
                    lista.Predio       = reader.GetInt32(10);
                    lista.Nivel        = reader.GetInt32(11);
                    lista.Apto         = reader.GetInt32(12);
                    lista.Qt           = reader.GetInt32(13);
                    lista.Warning      = "N";
                    lista.Erro         = "N";

                    listaReposicao.Add(lista);
                }

                connection.Close();

                return listaReposicao;

            }
            catch (Exception ex)
            {

                if (connection.State == ConnectionState.Open)
                {
                    ProdutoEnderecoPicking lista = new ProdutoEnderecoPicking();

                    lista.Erro = "S";
                    lista.MensagemErroWarning = ex.Message;

                    listaReposicao.Add(lista);


                    connection.Close();
                    return listaReposicao;
                }

                exec.Dispose();
                connection.Dispose();

                return listaReposicao;
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

        public ProdutoEnderecoPicking getEnderecoProdutoPicking(string produto, int filial)
        {
            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            ProdutoEnderecoPicking produtoEnderecoPicking = new ProdutoEnderecoPicking();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append($"select en.codendereco, prod.codprod, prod.descricao, case when {produto} = prod.codauxiliar2 then prod.qtunitcx else prod.qtunit end qt, ");
                query.Append("        en.deposito, en.rua, en.predio, en.nivel, en.apto, prod.codauxiliar as ean, prod.codauxiliar2 as dun, prod.qtunitcx");
                query.Append("  from pcendereco en inner join pcprodutpicking pk on (en.codendereco = pk.codendereco and en.codfilial = pk.codfilial)");
                query.Append("                     inner join pcestendereco est on (en.codendereco = est.codendereco)");
                query.Append("                     inner join pcprodut prod on (est.codprod = prod.codprod) ");
                query.Append($"where ((prod.codprod = {produto}) or (prod.codauxiliar = {produto}) or (prod.codauxiliar2 = {produto}))");
                query.Append($"  and en.codfilial = {filial} and pk.tipo = 'V'");
                query.Append(" order by en.deposito, en.rua, case when mod(en.rua, 2) = 1 then en.predio end asc, case when mod(en.rua, 2) = 0 then en.predio end desc, en.nivel, en.apto");

                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                ProdutoEnderecoPicking pend = new ProdutoEnderecoPicking();

                if (reader.Read())
                {
                    pend.Codendereco = reader.GetInt32(0);
                    pend.Codprod = reader.GetInt32(1);
                    pend.Descricao = reader.GetString(2);
                    pend.Qt = reader.GetInt32(3);
                    pend.Deposito = reader.GetInt32(4);
                    pend.Rua = reader.GetInt32(5);
                    pend.Predio = reader.GetInt32(6);
                    pend.Nivel = reader.GetInt32(7);
                    pend.Apto = reader.GetInt32(8);
                    pend.Ean = reader.GetInt64(9);
                    pend.Dun = reader.GetInt64(10);
                    pend.Qtunitcx = reader.GetInt32(11);
                    pend.Codfilial = filial;
                    pend.Warning = "N";
                    pend.Erro = "N";

                    produtoEnderecoPicking = pend;
                }
                else
                {
                    pend.Erro = "N";
                    pend.Warning = "S";
                    pend.MensagemErroWarning = "Produto não encontrado.";

                    produtoEnderecoPicking = pend;
                }

                connection.Close();

                return produtoEnderecoPicking;

            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    ProdutoEnderecoPicking pend = new ProdutoEnderecoPicking();

                    pend.Erro = "S";
                    pend.MensagemErroWarning = ex.Message;

                    produtoEnderecoPicking = pend;

                    connection.Close();

                    return produtoEnderecoPicking;                  
                }

                exec.Dispose();
                connection.Dispose();

                return produtoEnderecoPicking;
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

        public int proximaReposicao()
        {
            int proxReposicao;
            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();
            
            StringBuilder query = new StringBuilder();
            try
            {
                query.Append("SELECT NVL(MAX(NUMREPOSICAO),0) + 1 AS PROXREPOSICAO FROM TAB_LOGISTICA_REPOSICAO");
                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                if (reader.Read())
                {
                    proxReposicao = reader.GetInt32(0);
                    return proxReposicao;
                }
                else
                {
                    proxReposicao = 0;
                    return proxReposicao;
                }
            }
            catch (Exception ex)
            {
                proxReposicao = 0;

                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return proxReposicao;
                }

                exec.Dispose();
                connection.Dispose();

                return proxReposicao;
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

        public List<ProdutoEnderecoPicking> gravaListaEndereco(List<ProdutoEnderecoPicking> lista)
        {
            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            List<ProdutoEnderecoPicking> listaOrdenada = new List<ProdutoEnderecoPicking>();

            StringBuilder numReposicao = new StringBuilder();

            try
            {
                lista.ForEach(list =>
                {
                    StringBuilder query = new StringBuilder();
                    ProdutoEnderecoPicking data = new ProdutoEnderecoPicking();

                    data.Numreposicao = list.Numreposicao;
                    data.Codfunc = list.Codfunc;
                    data.Codprod = list.Codprod;
                    data.Codfilial = list.Codfilial;
                    data.Qt = list.Qt;

                    query.Append($"INSERT INTO TAB_LOGISTICA_REPOSICAO (NUMREPOSICAO, CODPROD, CODFILIAL, QT, DTINICIOCONF, CODFUNCLISTA) VALUES ({data.Numreposicao}, {data.Codprod}, {data.Codfilial}, {data.Qt}, SYSDATE, {data.Codfunc})");
                    exec.CommandText = query.ToString();
                    OracleDataReader reader = exec.ExecuteReader();
                });

                listaOrdenada = this.getListaReposicaoAberta(lista[0].Codfunc);

                return listaOrdenada;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    ProdutoEnderecoPicking listaErro = new ProdutoEnderecoPicking();

                    listaErro.Erro = "S";
                    listaErro.MensagemErroWarning = ex.Message;

                    listaOrdenada.Add(listaErro);

                    connection.Close();
                    return listaOrdenada;
                }

                exec.Dispose();
                connection.Dispose();
                return listaOrdenada;
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

        public Boolean finalizaConfListagem(ProdutoEnderecoPicking lista)
        {
            Boolean salvou = false;

            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();
            
            StringBuilder query = new StringBuilder();

            try
            {
                query.Append($"UPDATE TAB_LOGISTICA_REPOSICAO SET QTCONFERIDA = {lista.Qt}, DTFIMCONF = SYSDATE, DTCONFLISTA = SYSDATE WHERE NUMREPOSICAO = {lista.Numreposicao} AND CODPROD = {lista.Codprod} AND CODFILIAL = {lista.Codfilial}");
                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                salvou = true;
                return salvou;
            }
            catch (Exception ex)
            {
                salvou = false;

                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    return salvou;
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

        public Boolean cancelarListagem(int numListagem)
        {
            Boolean cancelado = false;

            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append($"UPDATE TAB_LOGISTICA_REPOSICAO SET DTCANCEL = SYSDATE WHERE NUMREPOSICAO = {numListagem}");
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
