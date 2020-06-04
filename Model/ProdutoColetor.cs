using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjetoColetorApi.Model
{
    public class ProdutoColetor
    {
        private int codfilial;
        private int codprod;
        private string descricao;
        private string embalagemmaster;
        private string unidademaster;
        private int qtcx;
        private Int64 dun;
        private string embalagem;
        private string unidade;
        private int qtunit;
        private Int64 ean;
        private int alt;
        private int larg;
        private int comp;
        private double peso;
        private int lastro;
        private int camada;
        private int total;

        private string erro;
        private string warning;
        private string mensagemErroWarning;


        public int Codfilial { get => codfilial; set => codfilial = value; }
        public int Codprod { get => codprod; set => codprod = value; }
        public string Descricao { get => descricao; set => descricao = value; }
        public string Embalagemmaster { get => embalagemmaster; set => embalagemmaster = value; }
        public string Unidademaster { get => unidademaster; set => unidademaster = value; }
        public int Qtcx { get => qtcx; set => qtcx = value; }
        public Int64 Dun { get => dun; set => dun = value; }
        public string Embalagem { get => embalagem; set => embalagem = value; }
        public string Unidade { get => unidade; set => unidade = value; }
        public int Qtunit { get => qtunit; set => qtunit= value; }
        public Int64 Ean { get => ean; set => ean = value; }
        public int Alt { get => alt; set => alt = value; }
        public int Larg { get => larg; set => larg = value; }
        public int Comp { get => comp; set => comp = value; }
        public int Lastro { get => lastro; set => lastro = value; }
        public int Camada { get => camada; set => camada = value; }
        public int Total { get => total; set => total = value; }
        public double Peso { get => peso; set => peso = value; }

        public string Erro { get => erro; set => erro = value; }
        public string Warning { get => warning; set => warning = value; }
        public string MensagemErroWarning { get => mensagemErroWarning; set => mensagemErroWarning = value; }

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
                decimal volumeMaster = ((Convert.ToDecimal(prod.alt) * Convert.ToDecimal(prod.larg) * Convert.ToDecimal(prod.comp)) / 1000000);
                decimal volumeUnit = (volumeMaster / prod.qtcx);
                //CALCULA PESO UNITARIO
                double pesoUnit = (prod.peso / prod.qtcx);

                //UPDATE PCPRODUT 
                pcprodut.Append($"UPDATE PCPRODUT SET CODAUXILIAR = {prod.ean}, CODAUXILIAR2 = {prod.dun}, ALTURAARM = {prod.alt}, LARGURAARM = {prod.larg}, ");
                pcprodut.Append($"COMPRIMENTOARM = {prod.comp}, VOLUMEARM = {volumeMaster.ToString().Replace(",", ".")}, PESOLIQMASTER = {prod.peso.ToString().Replace(",", ".")}, PESOBRUTOMASTER = {prod.peso.ToString().Replace(",", ".")}, ");
                pcprodut.Append($"VOLUME = {volumeUnit.ToString().Replace(",", ".")}, PESOLIQ = ROUND({pesoUnit.ToString().Replace(",", ".")},4), PESOBRUTO = ROUND({pesoUnit.ToString().Replace(",", ".")},4) ");
                pcprodut.Append($" where codprod = {prod.codprod}");

                cmd.CommandText = pcprodut.ToString();
                OracleDataReader produt = cmd.ExecuteReader();

                //UPDATE PCPRODFILIAL
                pcprodfilial.Append($"UPDATE PCPRODFILIAL SET LASTROPAL = {prod.lastro}, ALTURAPAL = {prod.camada}, QTTOTPAL = ({prod.lastro}*{prod.camada})");
                pcprodfilial.Append($" WHERE CODPROD = {prod.codprod} AND CODFILIAL = {prod.codfilial}");

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

            ProdutoColetor produtoColetor = new ProdutoColetor();

            StringBuilder query = new StringBuilder();

            ProdutoColetor p = new ProdutoColetor();

            try
            {

                OracleConnection con = DataBase.novaConexao();

                OracleCommand cmd = con.CreateCommand();

                query.Append("SELECT TO_NUMBER(PF.CODFILIAL) AS CODFILIAL, P.CODPROD, P.DESCRICAO || ' - ' || P.EMBALAGEM as DESCRICAO, P.EMBALAGEMMASTER, P.UNIDADEMASTER, ");
                query.Append("       P.QTUNITCX AS QTCX, CODAUXILIAR2 AS DUN, P.EMBALAGEM, P.UNIDADE, P.QTUNIT AS QTUNIT, CODAUXILIAR AS EAN, P.ALTURAARM AS ALT, ");
                query.Append("       P.LARGURAARM AS LARG, P.COMPRIMENTOARM AS COMP, P.PESOBRUTOMASTER AS PESO, NVL(PF.LASTROPAL, 0) AS LASTRO, NVL(PF.ALTURAPAL, 0) AS CAMADA, ");
                query.Append("       PF.QTTOTPAL AS QTTOTAL ");
                query.Append("  FROM PCPRODUT P INNER JOIN PCPRODFILIAL PF ON(P.CODPROD = PF.CODPROD) ");
                query.Append($" WHERE((P.CODPROD = {produto}) OR (P.CODAUXILIAR = {produto}) OR (P.CODAUXILIAR2 = {produto})) ");
                query.Append($"   AND PF.CODFILIAL = {filial}");

                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                if(reader.Read())
                {

                    p.codfilial = reader.GetInt32(0);
                    p.codprod = reader.GetInt32(1);
                    p.descricao = reader.GetString(2);
                    p.embalagemmaster = reader.GetString(3);
                    p.unidademaster = reader.GetString(4);
                    p.qtcx = reader.GetInt32(5);
                    p.dun = reader.GetInt64(6);
                    p.embalagem = reader.GetString(7);
                    p.unidade = reader.GetString(8);
                    p.qtunit = reader.GetInt32(9);
                    p.ean = reader.GetInt64(10);
                    p.alt = reader.GetInt32(11);
                    p.larg = reader.GetInt32(12);
                    p.comp = reader.GetInt32(13);
                    p.peso = reader.GetDouble(14);
                    p.lastro = reader.GetInt32(15);
                    p.camada = reader.GetInt32(16);
                    p.total = reader.GetInt32(17);
                    p.erro = "N";
                    p.warning = "N";
                    p.mensagemErroWarning = null;

                    produtoColetor = p;
                }
                else
                {

                    p.erro = "N";
                    p.warning = "S";
                    p.mensagemErroWarning = "Produto não encontrado";

                    produtoColetor = p;
                }
                
                con.Close();
                return produtoColetor;

            }
            catch (Exception e)
            {

                p.erro = "S";
                p.mensagemErroWarning = e.Message;

                produtoColetor = p;

                return produtoColetor;
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
