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
    public class ArmazenagemController : Controller
    {
        [Route("{codfunc}")]
        public JsonResult BuscarProxBox(int codfunc)
        {
            return Json(new Armazenagem().BuscaProxBox(codfunc));
        }

        [Route("{codigoUma}/{numBonus}")]
        public JsonResult BuscarProxOsTranspalete(int codigoUma, int numBonus)
        {
            return Json(new Armazenagem().ProxOsTranspalete(codigoUma, numBonus));
        }

        [HttpPut]
        [Route("{codigoUma}/{codEndereco}/{codFuncConf}")]
        public Boolean RegistraEndOrig(int codigoUma, int codEndereco, int codFuncConf)
        {
            return new Armazenagem().RegistraEndOrig(codigoUma, codEndereco, codFuncConf);
        }

        [Route("{numBonus}")]
        public JsonResult BuscaPendenciaBox(int numBonus)
        {
            return Json(new Armazenagem().BuscaPendenciaBox(numBonus));
        }

        [HttpPut]
        [Route("{numBonus}")]
        public Boolean CancelarOpTranspalete(int numBonus)
        {
            return new Armazenagem().CancelarOpTranspalete(numBonus);
        }

        
    }
}
