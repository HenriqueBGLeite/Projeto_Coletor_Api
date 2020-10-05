using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using ProjetoColetorApi.Model;
using System.Web.Http.Cors;
using Microsoft.AspNetCore.Authorization;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using Microsoft.IdentityModel.Tokens;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace ProjetoColetorApi.Controllers
{
    [AllowAnonymous]
    [Route("api/[Controller]/[Action]/")]
    public class LoginController : Controller
    {
        private readonly IConfiguration _configuration;

        public LoginController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpPost]
        public Usuario getUsuario([FromBody] Usuario usuario)
        {
            var usuarioAutenticado = new Usuario();

            usuarioAutenticado = usuarioAutenticado.validaUsuario(usuario);

            if ( usuarioAutenticado.Erro == "N" && usuarioAutenticado.Warning == "N" )
            {
                //GERAÇÃO DO TOKEN
                var claims = new[]
                {
                    //Declarar dados que precisa no token
                    new Claim ("filial", usuarioAutenticado.Filial.ToString()), //Verificar como faz quando o valor for INT
                    new Claim ("codigo", usuarioAutenticado.Code.ToString()), //Verificar como faz quando o valor for INT
                    new Claim ("nome", usuarioAutenticado.Nome),
                    new Claim ("usaWms", usuarioAutenticado.UsaWms),
                    new Claim ("acessoSistema", usuarioAutenticado.AcessoSistema),
                    new Claim ("acessoEntrada", usuarioAutenticado.AcessoEntrada),
                    new Claim ("acessoConferirBonusEntrada", usuarioAutenticado.AcessoConferirBonusEntrada),
                    new Claim ("acessoConferirBonusDevolucao", usuarioAutenticado.AcessoConferirBonusDevolucao),
                    new Claim ("acessoConferirUma", usuarioAutenticado.AcessoConferirUma),
                    new Claim ("acessoConferirCxPlastica", usuarioAutenticado.AcessoConferirCaixaPlastica),
                    new Claim ("acessoSaida", usuarioAutenticado.AcessoSaida),
                    new Claim ("acessoConferirOs", usuarioAutenticado.AcessoConferirOs),
                    new Claim ("acessoPaletizarCarga", usuarioAutenticado.AcessoPaletizarCarga),
                    new Claim ("acessoAuditarCarregamento", usuarioAutenticado.AcessoAuditarCarregamento),
                    new Claim ("acessoArmazenagem", usuarioAutenticado.AcessoArmazenagem),
                    new Claim ("acessoOperadorTranspalete", usuarioAutenticado.AcessoOperadorTranspalete),
                    new Claim ("acessoOperadorEmpilhadeira", usuarioAutenticado.AcessoOperadorEmpilhadeira),
                    new Claim ("acessoRepositorMercadoria", usuarioAutenticado.AcessoRepositorMercadoria),
                    new Claim ("acessoDadosProduto", usuarioAutenticado.AcessoDadosProduto),
                    new Claim ("acessoListarEnderecos", usuarioAutenticado.AcessoListarEnderecos),
                    new Claim ("acessoInventario", usuarioAutenticado.AcessoInventario),
                    new Claim ("base", usuarioAutenticado.Base)
                };

                //Recebe uma instancia da classe SymmetricSecurityKey
                //Armazenando a chave de criptografia usada na criação do token
                var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_configuration["SecurityKey"]));

                //Recebe um objeto de tipo SigninCredentials contendo a chave de criptografia e o algoritimo de segurança empregados na geração de assinaturas digitais para tokens
                var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

                //Gera o Token com os dados previamente declarados
                var token = new JwtSecurityToken(
                    issuer: "EPOCA",
                    audience: "EPOCA",
                    claims: claims,
                    expires: DateTime.Now.AddHours(12),
                    signingCredentials: creds
                    );

                usuarioAutenticado.Token = new JwtSecurityTokenHandler().WriteToken(token);

                return usuarioAutenticado;
            }
            else
            {
                return usuarioAutenticado;
            }
        }
    }
}