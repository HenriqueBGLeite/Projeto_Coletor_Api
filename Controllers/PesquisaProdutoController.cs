using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using ProjetoColetorApi.Model;

namespace ProjetoColetorApi.Controllers
{
    [Route("api/[Controller]/[Action]/")]
    [Authorize()]
    public class PesquisaProdutoController : Controller
    {
        // Tela de consultar produto
        [HttpPut]
        public Boolean editaDadosProd([FromBody] ProdutoColetor prod)
        {
            Boolean salvou;

            salvou = prod.editaDados(prod);

            return salvou;
        }

        [Route("{produto}/{filial}")]
        public JsonResult getProduto(string produto, int filial)
        {
            return Json(new ProdutoColetor().getProduto(produto, filial));
        }

        [Route("{produto}/{filial}")]
        public JsonResult getEnderecoProduto(string produto, int filial)
        {
            return Json(new ProdutoEndereco().getEnderecoProduto(produto, filial));
        }

        [Route("{produto}/{codFunc}")]
        public JsonResult getEstoqueProduto(string produto, int codFunc)
        {
            return Json(new ProdutoEstoque().getEstoqueProduto(produto, codFunc));
        }

        [Route("{codUsuario}")]
        public JsonResult getFiliais(int codUsuario)
        {
            return Json(new Filiais().getFiliais(codUsuario));
        }

        // Tela de listar endereços
        [Route("{codUsuario}")]
        public JsonResult getListaReposicaoAberta(int codUsuario)
        {
            return Json(new ProdutoEnderecoPicking().getListaReposicaoAberta(codUsuario));
        }

        [Route("{produto}/{filial}")]
        public JsonResult getEnderecoProdutoPicking(string produto, int filial)
        {
            return Json(new ProdutoEnderecoPicking().getEnderecoProdutoPicking(produto, filial));
        }

        public JsonResult proximaRequisicao()
        {
            return Json(new ProdutoEnderecoPicking().proximaReposicao());
        }

        [HttpPost]
        public JsonResult gravaListaEndereco([FromBody] List<ProdutoEnderecoPicking> lista)
        {
            return Json(new ProdutoEnderecoPicking().gravaListaEndereco(lista));
        }

        [HttpPut]
        public Boolean finalizaConfListagem([FromBody] ProdutoEnderecoPicking lista)
        {
            Boolean salvou;

            salvou = new ProdutoEnderecoPicking().finalizaConfListagem(lista);

            return salvou;
        }

        [Route("{numLista}")]
        public Boolean VerificaListagem(int numLista)
        {
            Boolean aberto;

            aberto = new ProdutoEnderecoPicking().VerificaListagem(numLista);

            return aberto;
        }

        [HttpPut]
        [Route("{numLista}")]
        public Boolean cancelarListagem(int numLista)
        {
            Boolean cancelado;

            cancelado = new ProdutoEnderecoPicking().cancelarListagem(numLista);

            return cancelado;
        }
    }
}