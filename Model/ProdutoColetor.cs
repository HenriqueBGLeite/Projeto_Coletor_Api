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
            StringBuilder pcprodut = new StringBuilder();
            StringBuilder pcprodfilial = new StringBuilder();
            OracleConnection con = DataBase.novaConexao();

            try
            {

                OracleCommand cmd = con.CreateCommand();

                //CALCULA VOL. MASTER
                decimal volumeMaster = ((Convert.ToDecimal(prod.Alt) * Convert.ToDecimal(prod.Larg) * Convert.ToDecimal(prod.Comp)) / 1000000);
                decimal volumeUnit = (volumeMaster / prod.Qtcx);
                //CALCULA PESO UNITARIO
                double pesoUnit = (prod.Peso / prod.Qtcx);

                //UPDATE PCPRODUT 
                pcprodut.Append($"UPDATE PCPRODUT SET CODAUXILIAR = {prod.Ean}, CODAUXILIAR2 = {prod.Dun}, ALTURAARM = {prod.Alt}, LARGURAARM = {prod.Larg}, ");
                pcprodut.Append($"COMPRIMENTOARM = {prod.Comp}, VOLUMEARM = {volumeMaster.ToString().Replace(",", ".")}, PESOLIQMASTER = {prod.Peso.ToString().Replace(",", ".")}, PESOBRUTOMASTER = {prod.Peso.ToString().Replace(",", ".")}, ");
                pcprodut.Append($"VOLUME = {volumeUnit.ToString().Replace(",", ".")}, PESOLIQ = ROUND({pesoUnit.ToString().Replace(",", ".")},4), PESOBRUTO = ROUND({pesoUnit.ToString().Replace(",", ".")},4) ");
                pcprodut.Append($" where codprod = {prod.Codprod}");

                cmd.CommandText = pcprodut.ToString();
                OracleDataReader produt = cmd.ExecuteReader();

                //UPDATE PCPRODFILIAL
                pcprodfilial.Append($"UPDATE PCPRODFILIAL SET LASTROPAL = {prod.Lastro}, ALTURAPAL = {prod.Camada}, QTTOTPAL = ({prod.Lastro}*{prod.Camada})");
                pcprodfilial.Append($" WHERE CODPROD = {prod.Codprod} AND CODFILIAL = {prod.Codfilial}");

                cmd.CommandText = pcprodfilial.ToString();
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

        public ProdutoColetor getProduto(string produto, int filial)
        {

            OracleConnection connection = DataBase.novaConexao();

            OracleCommand exec = connection.CreateCommand();

            ProdutoColetor produtoColetor = new ProdutoColetor();
            ProdutoColetor p = new ProdutoColetor();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT TO_NUMBER(PF.CODFILIAL) AS CODFILIAL, P.CODPROD, P.DESCRICAO || ' - ' || P.EMBALAGEM as DESCRICAO, P.EMBALAGEMMASTER, P.UNIDADEMASTER, ");
                query.Append("       P.QTUNITCX AS QTCX, CODAUXILIAR2 AS DUN, P.EMBALAGEM, P.UNIDADE, P.QTUNIT AS QTUNIT, CODAUXILIAR AS EAN, P.ALTURAARM AS ALT, ");
                query.Append("       P.LARGURAARM AS LARG, P.COMPRIMENTOARM AS COMP, P.PESOBRUTOMASTER AS PESO, NVL(PF.LASTROPAL, 0) AS LASTRO, NVL(PF.ALTURAPAL, 0) AS CAMADA, ");
                query.Append("       PF.QTTOTPAL AS QTTOTAL, PK.CAPACIDADE * P.QTUNITCX AS CAPACIDADE, PK.PONTOREPOSICAO * P.QTUNITCX AS REPOSICAO, PF.PRAZOVAL, PF.PERCTOLERANCIAVAL, ");
                query.Append("       P.ALTURAM3 AS ALT_UN, P.LARGURAM3 AS LARG_UN, P.COMPRIMENTOM3 AS COMP_UN, P.PESOBRUTO AS PESO_UN");
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
                    p.Embalagemmaster = reader.GetString(3);
                    p.Unidademaster = reader.GetString(4);
                    p.Qtcx = reader.GetInt32(5);
                    p.Dun = reader.GetInt64(6);
                    p.Embalagem = reader.GetString(7);
                    p.Unidade = reader.GetString(8);
                    p.Qtunit = reader.GetInt32(9);
                    p.Ean = reader.GetInt64(10);
                    p.Alt = reader.GetInt32(11);
                    p.Larg = reader.GetInt32(12);
                    p.Comp = reader.GetInt32(13);
                    p.Peso = reader.GetDouble(14);
                    p.Lastro = reader.GetInt32(15);
                    p.Camada = reader.GetInt32(16);
                    p.Total = reader.GetInt32(17);
                    p.Capacidade = reader.GetInt32(18);
                    p.Reposicao = reader.GetInt32(19);
                    p.PrazoValidade = reader.GetInt32(20);
                    p.ShelfLife = reader.GetInt32(21);
                    p.AltUn = reader.GetInt32(22);
                    p.LargUn = reader.GetInt32(23);
                    p.CompUn = reader.GetInt32(24);
                    p.PesoUn = reader.GetDouble(25);
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
        private string tipoendereco;
        private int deposito;
        private int rua;
        private int predio;
        private int nivel;
        private int apto;

        private string erro;
        private string warning;
        private string mensagemErroWarning;

        public string TipoEndereco { get => tipoendereco; set => tipoendereco = value; }
        public int Deposito { get => deposito; set => deposito = value; }
        public int Rua { get => rua; set => rua = value; }
        public int Predio { get => predio; set => predio = value; }
        public int Nivel { get => nivel; set => nivel = value; }
        public int Apto { get => apto; set => apto = value; }

        public string Erro { get => erro; set => erro = value; }
        public string Warning { get => warning; set => warning = value; }
        public string MensagemErroWarning { get => mensagemErroWarning; set => mensagemErroWarning = value; }

        public List<ProdutoEndereco> getEnderecoProduto(string produto, int filial)
        {

            List<ProdutoEndereco> produtoEndereco = new List<ProdutoEndereco>();

            StringBuilder query = new StringBuilder();

            try
            {

                OracleConnection con = DataBase.novaConexao();

                OracleCommand cmd = con.CreateCommand();

                query.Append("select en.tipoender, en.deposito, en.rua, en.predio, en.nivel, en.apto");
                query.Append("  from pcendereco en inner join pcestendereco est on (en.codendereco = est.codendereco)");
                query.Append("                     inner join pcprodut prod on (est.codprod = prod.codprod)");
                query.Append($" where ((prod.codprod = {produto}) or (prod.codauxiliar = {produto}) or (prod.codauxiliar2 = {produto}))");
                query.Append($"   and en.codfilial = {filial}");
                query.Append(" order by en.deposito, en.rua, case when mod(en.rua, 2) = 1 then en.predio end asc, case when mod(en.rua, 2) = 0 then en.predio end desc, en.nivel, en.apto");


                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    ProdutoEndereco pend = new ProdutoEndereco();

                    pend.tipoendereco = reader.GetString(0);
                    pend.deposito     = reader.GetInt32(1);
                    pend.rua          = reader.GetInt32(2);
                    pend.predio       = reader.GetInt32(3);
                    pend.nivel        = reader.GetInt32(4);
                    pend.apto         = reader.GetInt32(5);

                    produtoEndereco.Add(pend);
                }

                con.Close();

                return produtoEndereco;

            }
            catch (Exception e)
            {
                ProdutoEndereco pend = new ProdutoEndereco();

                pend.erro                = "S";
                pend.mensagemErroWarning = e.Message;

                produtoEndereco.Add(pend);

                return produtoEndereco;
            }

        }
    }

    public class ProdutoEnderecoPicking
    {
        private int codprod;
        private string descricao;
        private int qt;
        private int deposito;
        private int rua;
        private int predio;
        private int nivel;
        private int apto;

        private string erro;
        private string warning;
        private string mensagemErroWarning;

        public int Codprod { get => codprod; set => codprod = value; }
        public string Descricao { get => descricao; set => descricao = value; }
        public int Qt { get => qt; set => qt = value; }
        public int Deposito { get => deposito; set => deposito = value; }
        public int Rua { get => rua; set => rua = value; }
        public int Predio { get => predio; set => predio = value; }
        public int Nivel { get => nivel; set => nivel = value; }
        public int Apto { get => apto; set => apto = value; }

        public string Erro { get => erro; set => erro = value; }
        public string Warning { get => warning; set => warning = value; }
        public string MensagemErroWarning { get => mensagemErroWarning; set => mensagemErroWarning = value; }

        public List<ProdutoEnderecoPicking> getEnderecoProdutoPicking(string produto, int filial)
        {

            List<ProdutoEnderecoPicking> produtoEnderecoPicking = new List<ProdutoEnderecoPicking>();

            StringBuilder query = new StringBuilder();

            try
            {

                OracleConnection con = DataBase.novaConexao();

                OracleCommand cmd = con.CreateCommand();

                query.Append("select prod.codprod, prod.descricao, est.qt, en.deposito, en.rua, en.predio, en.nivel, en.apto");
                query.Append("  from pcendereco en inner join pcprodutpicking pk on (en.codendereco = pk.codendereco and en.codfilial = pk.codfilial)");
                query.Append("                     inner join pcestendereco est on(en.codendereco = est.codendereco)");
                query.Append("                     inner join pcprodut prod on(est.codprod = prod.codprod)");
                query.Append($"where ((prod.codprod = {produto}) or (prod.codauxiliar = {produto}) or (prod.codauxiliar2 = {produto}))");
                query.Append($"  and en.codfilial = { filial}");
                query.Append("order by en.deposito, en.rua, case when mod(en.rua, 2) = 1 then en.predio end asc, case when mod(en.rua, 2) = 0 then en.predio end desc, en.nivel, en.apto");


                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                ProdutoEnderecoPicking pend = new ProdutoEnderecoPicking();

                if (reader.Read())
                {
                        
                    pend.codprod = reader.GetInt32(0);
                    pend.descricao = reader.GetString(1);
                    pend.qt = reader.GetInt32(2);
                    pend.deposito = reader.GetInt32(3);
                    pend.rua = reader.GetInt32(4);
                    pend.predio = reader.GetInt32(5);
                    pend.nivel = reader.GetInt32(6);
                    pend.apto = reader.GetInt32(7);

                    produtoEnderecoPicking.Add(pend);
                } 
                else
                {
                    pend.erro = "N";
                    pend.warning = "S";
                    pend.mensagemErroWarning = "Produto não encontrado";

                    produtoEnderecoPicking.Add(pend);
                }

                con.Close();

                return produtoEnderecoPicking;

            }
            catch (Exception e)
            {
                ProdutoEnderecoPicking pend = new ProdutoEnderecoPicking();

                pend.erro = "S";
                pend.mensagemErroWarning = e.Message;

                produtoEnderecoPicking.Add(pend);

                return produtoEnderecoPicking;
            }

        }
    }

    public class ProdutoEstoque
    {

        private string codfilial;
        private int qtestger;
        private int qtreserv;
        private int qtbloq;
        private int qtavaria;

        private string erro;
        private string warning;
        private string mensagemErroWarning;

        public string Codfilial { get => codfilial; set => codfilial = value; }
        public int Qtestger { get => qtestger; set => qtestger = value; }
        public int Qtreserv { get => qtreserv; set => qtreserv = value; }
        public int Qtbloq { get => qtbloq; set => qtbloq = value; }
        public int Qtavaria { get => qtavaria; set => qtavaria = value; }

        public string Erro { get => erro; set => erro = value; }
        public string Warning { get => warning; set => warning = value; }
        public string MensagemErroWarning { get => mensagemErroWarning; set => mensagemErroWarning = value; }

        public List<ProdutoEstoque> getEstoqueProduto(string produto, int codigo)
        {

            List<ProdutoEstoque> produtoEstoque = new List<ProdutoEstoque>();

            StringBuilder query = new StringBuilder();

            try
            {

                OracleConnection con = DataBase.novaConexao();

                OracleCommand cmd = con.CreateCommand();

                query.Append("select est.codfilial, nvl(est.qtestger,0) as qtestger, nvl(est.qtreserv,0) as qtreserv, nvl(est.qtbloqueada,0) as qtbloqueada, nvl(est.qtindeniz,0) as qtindeniz");
                query.Append("  from pcest est inner join pcfilial f on (est.codfilial = f.codigo)");
                query.Append("                 inner join pcprodut prod on (est.codprod = prod.codprod)");
                query.Append($" where ((prod.codprod = {produto}) or (prod.codauxiliar = {produto}) or (prod.codauxiliar2 = {produto}))");
                query.Append("   and f.dtexclusao is null");
                query.Append("   and f.codigo <> 99");
                query.Append($"  and f.codigo in (select codigoa from pclib where codfunc = {codigo} and codtabela = 1 )");
                query.Append(" order by case when est.codfilial = 7 then '07' else est.codfilial end");


                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    ProdutoEstoque pe = new ProdutoEstoque();

                    pe.codfilial = reader.GetString(0);
                    pe.qtestger = reader.GetInt32(1);
                    pe.qtreserv = reader.GetInt32(2);
                    pe.qtbloq = reader.GetInt32(3);
                    pe.qtavaria = reader.GetInt32(4);

                    produtoEstoque.Add(pe);
                }

                con.Close();

                return produtoEstoque;

            }
            catch (Exception e)
            {
                ProdutoEstoque pe = new ProdutoEstoque();

                pe.erro = "S";
                pe.mensagemErroWarning = e.Message;

                produtoEstoque.Add(pe);

                return produtoEstoque;
            }

        }
    }

    public class Filiais
    {

        private string codfilial;

        private string erro;
        private string warning;
        private string mensagemErroWarning;

        public string Codfilial { get => codfilial; set => codfilial = value; }

        public string Erro { get => erro; set => erro = value; }
        public string Warning { get => warning; set => warning = value; }
        public string MensagemErroWarning { get => mensagemErroWarning; set => mensagemErroWarning = value; }

        public List<Filiais> getFiliais(int codigo)
        {

            List<Filiais> filiais = new List<Filiais>();

            StringBuilder query = new StringBuilder();

            try
            {

                OracleConnection con = DataBase.novaConexao();

                OracleCommand cmd = con.CreateCommand();

                query.Append("select f.codigo");
                query.Append("  from pcfilial f ");
                query.Append($"where f.dtexclusao is null");
                query.Append("   and f.codigo <> 99");
                query.Append($"  and f.codigo in (select codigoa from pclib where codfunc = {codigo} and codtabela = 1 )");
                query.Append(" order by case when f.codigo = 7 then '07' else f.codigo end");


                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    Filiais fil = new Filiais();

                    fil.codfilial = reader.GetString(0);

                    filiais.Add(fil);
                }

                con.Close();

                return filiais;

            }
            catch (Exception e)
            {
                Filiais fil = new Filiais();

                fil.erro = "S";
                fil.mensagemErroWarning = e.Message;

                filiais.Add(fil);

                return filiais;
            }

        }
    }
}
