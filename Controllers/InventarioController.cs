using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ProjetoColetorApi.Model;

namespace ProjetoColetorApi.Controllers
{
    [Route("api/[Controller]/[Action]/")]
    [Authorize()]
    public class InventarioController : Controller
    {
        //MÉTODOS PARA INVENTARIO COM WMS
        [HttpPost]
        public Boolean gravaProdutoInventario([FromBody] ProdutoInventario prod)
        {
            Boolean salvou = prod.gravaProduto(prod);
            return salvou;
        }

        [Route("{produto}/{filial}")]
        public JsonResult getProdutoInventario(string produto, int filial)
        {
            return Json(new ProdutoInventario().getProduto(produto, filial));
        }

        [Route("{codUsuario}/{codEndereco?}/{contagem?}")]
        public JsonResult getProxOs(string codUsuario, int codEndereco = -1, int contagem = 1)
        {
            return Json(new EnderecoInventario().getProxOs(codUsuario, codEndereco, contagem));
        }

        //MÉTODOS PARA INVENTARIO SEM WMS (MRURAL)
        [Route("{codFilial}/{codUsuario}")]
        public int getInventario(int codFilial, int codUsuario)
        {
            return 123456;
        }

    }
}