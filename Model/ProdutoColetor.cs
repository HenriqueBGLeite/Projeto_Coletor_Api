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
        public Int64 Dun { get; set; }
        public string Embalagem { get; set; }
        public string Unidade { get; set; }
        public int Qtunit { get; set; }
        public Int64 Ean { get; set; }
        public int Alt { get; set; }
        public int Larg { get; set; }
        public int Comp { get; set; }
        public int AltUn { get; set; }
        public int LargUn { get; set; }
        public int CompUn { get; set; }
        public int Lastro { get; set; }
        public int Camada { get; set; }
        public int Total { get; set; }
        public double Peso { get; set; }
        public double PesoUn { get; set; }
        public int Capacidade { get; set; }
        public int Reposicao { get; set; }
        public int PrazoValidade { get; set; }
        public int ShelfLife { get; set; }

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

                //CALCULA VOL. MASTER
                decimal volumeMaster = ((Convert.ToDecimal(prod.Alt) * Convert.ToDecimal(prod.Larg) * Convert.ToDecimal(prod.Comp)) / 1000000);
                decimal volumeUnit = ((Convert.ToDecimal(prod.AltUn) * Convert.ToDecimal(prod.LargUn) * Convert.ToDecimal(prod.CompUn)) / 1000000); 

                //UPDATE PCPRODUT 
                pcprodut.Append($"UPDATE PCPRODUT SET CODAUXILIAR = {prod.Ean}, CODAUXILIAR2 = {prod.Dun}, ALTURAARM = {prod.Alt}, LARGURAARM = {prod.Larg}, ");
                pcprodut.Append($"COMPRIMENTOARM = {prod.Comp}, VOLUMEARM = {volumeMaster.ToString().Replace(",", ".")}, PESOLIQMASTER = {prod.Peso.ToString().Replace(",", ".")}, PESOBRUTOMASTER = {prod.Peso.ToString().Replace(",", ".")}, ");
                pcprodut.Append($"VOLUME = {volumeUnit.ToString().Replace(",", ".")}, PESOLIQ = ROUND({prod.PesoUn.ToString().Replace(",", ".")},4), PESOBRUTO = ROUND({prod.PesoUn.ToString().Replace(",", ".")},4), ");
                pcprodut.Append($"ALTURAM3 = {prod.AltUn}, LARGURAM3 = {prod.LargUn}, COMPRIMENTOM3 = {prod.CompUn}");
                pcprodut.Append($" where codprod = {prod.Codprod}");

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
                if (connection.State == ConnectionState.Open)
                {
                    salvou = false;
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

        public ProdutoColetor getProduto(string produto, int filial)
        {

            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            ProdutoColetor produtoColetor = new ProdutoColetor();
            ProdutoColetor p = new ProdutoColetor();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT TO_NUMBER(PF.CODFILIAL) AS CODFILIAL, P.CODPROD, P.DESCRICAO || ' - ' || P.EMBALAGEM as DESCRICAO, ");
                query.Append("       P.EMBALAGEM, P.UNIDADE, P.QTUNIT AS QTUNIT, CODAUXILIAR AS EAN, P.ALTURAM3 AS ALT_UN, P.LARGURAM3 AS LARG_UN, P.COMPRIMENTOM3 AS COMP_UN, P.PESOBRUTO AS PESO_UN, ");
                query.Append("       P.EMBALAGEMMASTER, P.UNIDADEMASTER, P.QTUNITCX AS QTCX, CODAUXILIAR2 AS DUN, P.ALTURAARM AS ALT, P.LARGURAARM AS LARG, P.COMPRIMENTOARM AS COMP, P.PESOBRUTOMASTER AS PESO, ");
                query.Append("       NVL(PF.LASTROPAL, 0) AS LASTRO, NVL(PF.ALTURAPAL, 0) AS CAMADA, PF.QTTOTPAL AS QTTOTAL, PK.CAPACIDADE AS CAPACIDADE, PK.PONTOREPOSICAO AS REPOSICAO, PF.PRAZOVAL, PF.PERCTOLERANCIAVAL");
                query.Append("  FROM PCPRODUT P INNER JOIN PCPRODFILIAL PF ON(P.CODPROD = PF.CODPROD) LEFT OUTER JOIN PCPRODUTPICKING PK ON (P.CODPROD = PK.CODPROD AND PF.CODFILIAL = PK.CODFILIAL)");
                query.Append($" WHERE ((P.CODPROD = {produto}) OR (P.CODAUXILIAR = {produto}) OR (P.CODAUXILIAR2 = {produto})) ");
                query.Append($"   AND PF.CODFILIAL = {filial}");

                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                if(reader.Read())
                {
                    p.Codfilial = reader.GetInt32(0);
                    p.Codprod = reader.GetInt32(1);
                    p.Descricao = reader.GetString(2);
                    p.Embalagem = reader.GetString(3);
                    p.Unidade = reader.GetString(4);
                    p.Qtunit = reader.GetInt32(5);
                    p.Ean = reader.GetInt64(6);
                    p.AltUn = reader.GetInt32(7);
                    p.LargUn = reader.GetInt32(8);
                    p.CompUn = reader.GetInt32(9);
                    p.PesoUn = reader.GetDouble(10);
                    p.Embalagemmaster = reader.GetString(11);
                    p.Unidademaster = reader.GetString(12);
                    p.Qtcx = reader.GetInt32(13);
                    p.Dun = reader.GetInt64(14);
                    p.Alt = reader.GetInt32(15);
                    p.Larg = reader.GetInt32(16);
                    p.Comp = reader.GetInt32(17);
                    p.Peso = reader.GetDouble(18);
                    p.Lastro = reader.GetInt32(19);
                    p.Camada = reader.GetInt32(20);
                    p.Total = reader.GetInt32(21);
                    p.Capacidade = reader.GetInt32(22);
                    p.Reposicao = reader.GetInt32(23);
                    p.PrazoValidade = reader.GetInt32(24);
                    p.ShelfLife = reader.GetInt32(25);
                    p.Erro = "N";
                    p.Warning = "N";
                    p.MensagemErroWarning = null;

                    produtoColetor = p;
                }
                else
                {

                    p.Erro = "N";
                    p.Warning = "S";
                    p.MensagemErroWarning = "Produto não encontrado";

                    produtoColetor = p;
                }

                return produtoColetor;

            }
            catch (Exception ex)
            {

                if (connection.State == ConnectionState.Open)
                {
                    p.Erro = "S";
                    p.MensagemErroWarning = ex.Message;

                    produtoColetor = p;

                    connection.Close();
                }

                exec.Dispose();
                connection.Dispose();

                return produtoColetor;
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
}
