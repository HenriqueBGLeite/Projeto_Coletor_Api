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
        public string UsaWms { get; set; }
        public string AcessoSistema { get; set; }
        public string AcessoEntrada { get; set; }
        public string AcessoConferirBonusEntrada { get; set; }
        public string AcessoConferirBonusDevolucao { get; set; }
        public string AcessoConferirUma { get; set; }
        public string AcessoConferirCaixaPlastica { get; set; }
        public string AcessoSaida { get; set; }
        public string AcessoConferirOs { get; set; }
        public string AcessoPaletizarCarga { get; set; }
        public string AcessoAuditarCarregamento { get; set; }
        public string AcessoArmazenagem{ get; set; }
        public string AcessoOperadorTranspalete { get; set; }
        public string AcessoOperadorEmpilhadeira { get; set; }
        public string AcessoRepositorMercadoria { get; set; }
        public string AcessoDadosProduto { get; set; }
        public string AcessoListarEnderecos { get; set; }
        public string AcessoInventario { get; set; }
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

                query.Append("SELECT TO_NUMBER(PCEMPR.CODFILIAL) AS FILIAL, PCEMPR.MATRICULA AS CODIGO, ");
                query.Append("       CASE WHEN INSTR(PCEMPR.NOME, ' ') = 0 THEN TRIM(PCEMPR.NOME) ELSE TRIM(SUBSTR(PCEMPR.NOME,1,(INSTR(PCEMPR.NOME, ' ')))) END NOME, NVL(USAWMS, 'N') AS USAWMS,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTRO WHERE CODUSUARIO = PCEMPR.MATRICULA AND CODROTINA = 9844), 'N') AS ACESSO_SISTEMA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 14 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_ENTRADA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 15 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_BONUS_ENTRADA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 16 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_BONUS_DEVOLUCAO,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 17 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_CONFERIR_UMA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 18 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_CONFERIR_CX_PLASTICA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 19 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_SAIDA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 20 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_CONFERIR_OS,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 21 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_PALETIZAR_CARGA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 22 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_AUDITAR_CARREGAMENTO,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 23 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_ARMAZENAGEM,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 24 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_OPERADOR_TRANSPALETE,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 25 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_OPERADOR_EMPILHADEIRA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 26 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_REPOSITOR_MERCADORIA,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 27 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_DADOS_PRODUTO,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 28 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_LISTAR_ENDERECOS,");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 29 AND PCCONTROI.CODROTINA = 9844 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS ACESSO_INVENTARIO");
                query.Append("  FROM PCEMPR INNER JOIN PCFILIAL ON (PCFILIAL.CODIGO = PCEMPR.CODFILIAL)");
                query.Append("              LEFT OUTER JOIN (SELECT IV.NUMINVENT AS NUMINVENT, USU.MATRICULA FROM PCINVENTENDERECO IV INNER JOIN PCEMPR USU ON (IV.CODFUNC = USU.MATRICULA)");
                query.Append($"                               WHERE IV.DTATUALIZACAO IS NULL AND USU.MATRICULA = { usuario.Code } ");
                query.Append("                                  AND ROWNUM = 1");
                query.Append("                                ORDER BY DECODE(IV.CODFILIAL, USU.CODFILIAL, 'S', 'N')) INVENT ON (PCEMPR.MATRICULA = INVENT.MATRICULA)");
                query.Append($"WHERE NVL(PCEMPR.CODBARRA, PCEMPR.MATRICULA) = { usuario.Code }");
                query.Append($"  AND DECRYPT(PCEMPR.SENHABD, PCEMPR.USUARIOBD) = UPPER('{ usuario.Password }')");

                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    if (reader.GetString(4) == "S")
                    {
                        usuario.Filial = reader.GetInt32(0);
                        usuario.Code = reader.GetInt32(1);
                        usuario.Password = "";
                        usuario.Nome = reader.GetString(2);
                        usuario.UsaWms = reader.GetString(3);
                        usuario.AcessoSistema = reader.GetString(4);
                        usuario.AcessoEntrada = reader.GetString(5);
                        usuario.AcessoConferirBonusEntrada = reader.GetString(6);
                        usuario.AcessoConferirBonusDevolucao= reader.GetString(7);
                        usuario.AcessoConferirUma = reader.GetString(8);
                        usuario.AcessoConferirCaixaPlastica = reader.GetString(9);
                        usuario.AcessoSaida = reader.GetString(10);
                        usuario.AcessoConferirOs = reader.GetString(11);
                        usuario.AcessoPaletizarCarga = reader.GetString(12);
                        usuario.AcessoAuditarCarregamento = reader.GetString(13);
                        usuario.AcessoArmazenagem = reader.GetString(14);
                        usuario.AcessoOperadorTranspalete= reader.GetString(15);
                        usuario.AcessoOperadorEmpilhadeira = reader.GetString(16);
                        usuario.AcessoRepositorMercadoria = reader.GetString(17);
                        usuario.AcessoDadosProduto = reader.GetString(18);
                        usuario.AcessoListarEnderecos = reader.GetString(19);
                        usuario.AcessoInventario = reader.GetString(20);
                        usuario.Erro = "N";
                        usuario.Warning = "N";

                        con.Close();

                        return usuario;
                    }
                    else {
                        usuario.Erro = "N";
                        usuario.Warning = "S";
                        usuario.MensagemErroWarning = "Usuário sem acesso ao sistema.";

                        con.Close();

                        return usuario;
                    }
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
