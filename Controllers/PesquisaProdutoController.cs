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

        [Route("{produto}/{codigo}")]
        public JsonResult getEstoqueProduto(string produto, int codigo)
        {
            return Json(new ProdutoEstoque().getEstoqueProduto(produto, codigo));
        }

        [Route("{codUsuario}")]
        public JsonResult getFiliais(int codUsuario)
        {
            return Json(new Filiais().getFiliais(codUsuario));
        }

        [Route("{produto}/{filial}")]
        public JsonResult getEnderecoProdutoPicking(string produto, int filial)
        {
            return Json(new ProdutoEnderecoPicking().getEnderecoProdutoPicking(produto, filial));
        }

        [HttpPut]
        public Boolean editaDadosProd([FromBody] ProdutoColetor prod)
        {
            Boolean salvou = false;

            salvou = prod.editaDados(prod);

            return salvou;
        }
    }
}