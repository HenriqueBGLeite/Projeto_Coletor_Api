using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjetoColetorApi.Model
{
    public class Usuario
    {
        public int Filial { get; set; }
        public int Code { get; set; }
        public string Password { get; set; }
        public string Nome { get; set; }
        public string TipoConferencia { get; set; }
        public string PermiteFecharBonus { get; set; }
        public int DiasMinValidade { get; set; }
        public string DigitaQt { get; set; }
        public string UsaWms { get; set; }
        public string AcessoSistema { get; set; }
        public string AcessoDigQtInvent { get; set; }
        public string AcessoConfBonus { get; set; }
        public string AcessoConfPedido { get; set; }
        public string AcessoAlterarProduto { get; set; }
        public string AcessoListaEndereco { get; set; }
        public string AcessoVerEndereco { get; set; }
        public string AcessoAltValInvent { get; set; }
        public string AcessoAltEmbProdBonus { get; set; }
        public string Base { get; set; }
        public string Token { get; set; }
        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }

        public Usuario validaUsuario(Usuario usuario)
        {
            StringBuilder query = new StringBuilder();

            try
            {
                OracleConnection con = DataBase.novaConexao(usuario.Base);

                OracleCommand cmd = con.CreateCommand();

                query.Append("SELECT TO_NUMBER(PCEMPR.CODFILIAL) AS FILIAL, PCEMPR.MATRICULA AS CODIGO, CASE WHEN INSTR(PCEMPR.NOME, ' ') = 0 THEN TRIM(PCEMPR.NOME) ELSE TRIM(SUBSTR(PCEMPR.NOME,1,(INSTR(PCEMPR.NOME, ' ')))) END NOME,");
                query.Append("       FILIAIS.TIPOCONFERENCIA, FILIAIS.PERMITIR_FECHAR_BONUS, FILIAIS.DIASMINVALIDADE, FILIAIS.DIGITAQT, nvl(USAWMS, 'N') AS USAWMS,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTRO WHERE CODUSUARIO = PCEMPR.MATRICULA AND CODROTINA = 9844), 'N') AS ACESSSOSISTEMA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 1 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_DIG_QT_INVENT,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 2 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_CONF_BONUS,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 3 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_CONF_PEDIDO,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 4 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_ALTERAR_PRODUTO,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 5 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_VER_LISTAENDERECO,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 8 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_VER_ENDERECO,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 9 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_ALT_VAL_INVENT,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 11 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ATUALIZA_EMB_PROD_BONUS");
                query.Append("  FROM PCEMPR INNER JOIN FILIAIS ON(PCEMPR.CODFILIAL = FILIAIS.CODFIL)");
                query.Append("              INNER JOIN PCFILIAL ON(PCFILIAL.CODIGO = PCEMPR.CODFILIAL)");
                query.Append("              LEFT OUTER JOIN(SELECT IV.NUMINVENT AS NUMINVENT, USU.MATRICULA FROM PCINVENTENDERECO IV INNER JOIN PCEMPR USU ON(IV.CODFUNC = USU.MATRICULA)");
                query.Append($"                              WHERE IV.DTATUALIZACAO IS NULL AND USU.MATRICULA = { usuario.Code } ");
                query.Append("                                 AND ROWNUM = 1");
                query.Append("                               ORDER BY DECODE(IV.CODFILIAL, USU.CODFILIAL, 'S', 'N')) INVENT ON(PCEMPR.MATRICULA = INVENT.MATRICULA)");
                query.Append($"WHERE NVL(PCEMPR.CODBARRA, PCEMPR.MATRICULA) = { usuario.Code }");
                query.Append($"  AND DECRYPT(PCEMPR.SENHABD, PCEMPR.USUARIOBD) = UPPER('{ usuario.Password }')");

                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    usuario.Filial = reader.GetInt32(0);
                    usuario.Code = reader.GetInt32(1);
                    usuario.Password = "";
                    usuario.Nome = reader.GetString(2);
                    usuario.TipoConferencia = reader.GetString(3);
                    usuario.PermiteFecharBonus = reader.GetString(4);
                    usuario.DiasMinValidade = reader.GetInt32(5);
                    usuario.DigitaQt = reader.GetString(6);
                    usuario.UsaWms = reader.GetString(7);
                    usuario.AcessoSistema = reader.GetString(8);
                    usuario.AcessoDigQtInvent = reader.GetString(9);
                    usuario.AcessoConfBonus = reader.GetString(10);
                    usuario.AcessoConfPedido = reader.GetString(11);
                    usuario.AcessoAlterarProduto = reader.GetString(12);
                    usuario.AcessoListaEndereco = reader.GetString(13);
                    usuario.AcessoVerEndereco = reader.GetString(14);
                    usuario.AcessoAltValInvent = reader.GetString(15);
                    usuario.AcessoAltEmbProdBonus = reader.GetString(16);
                    usuario.Erro = "N";
                    usuario.Warning = "N";

                    con.Close();

                    return usuario;
                }
                else
                {
                    usuario.Erro = "N";
                    usuario.Warning = "S";
                    usuario.MensagemErroWarning = "Usuário/senha inválido.";

                    con.Close();

                    return usuario;
                }
            }
            catch (Exception e)
            {
                usuario.Erro = "S";
                usuario.MensagemErroWarning = e.Message;

                return usuario;
            }

        }
    }
}
