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
    public class EntradaController : Controller
    {
        [Route("{numOs}/{numVol}/{codFuncConf}")]
        [HttpPut]
        public string ConfereCxPlastica(int numOs, int numVol, int codFuncConf)
        {
            return new Entrada().ConfereCxPlastica(numOs, numVol, codFuncConf);
        }

        [Route("{codigoUma}")]
        public JsonResult BuscaUma(Int64 codigoUma)
        {
            return Json(new ConferenciaUma().BuscaUma(codigoUma));
        }

        [HttpPost]
        public Boolean ConfereUma([FromBody] ConferenciaUma dados)
        {
            return new ConferenciaUma().ConfereUma(dados);
        }

        [Route("{tipoBonus}/{codFilial}")]
        public JsonResult BuscaCabBonus(string tipoBonus, int codFilial)
        {
            return Json(new Entrada().BuscaCabBonus(tipoBonus, codFilial));
        }

        [Route("{numBonus}")]
        public JsonResult BuscaEquipeBonus(int numBonus)
        {
            return Json(new Entrada().BuscaEquipeBonus(numBonus));
        }

        [Route("{codFunc}")]
        public JsonResult BuscaAjudanteBonus(int codFunc)
        {
            return Json(new Entrada().BuscaAjudanteBonus(codFunc));
        }

        [HttpPost]
        public Boolean InsereEquipeBonus([FromBody] List<Entrada> lista)
        {
          return new Entrada().InsereEquipeBonus(lista);
        }

        [HttpDelete]
        [Route("{codFunc}/{numBonus}")]
        public Boolean RemoveEquipeBonus(int codFunc, int numBonus)
        {
            return new Entrada().RemoveEquipeBonus(codFunc, numBonus);
        }

        [Route("{numBonus}/{tipoBusca}")]
        public JsonResult BonusConfirmadoEnderecado(int numBonus, string tipoBusca)
        {
            return Json(new Entrada().BonusConfirmadoEnderecado(numBonus, tipoBusca));
        }

        [Route("{numBonus}/{codBarra}")]
        public JsonResult BuscaProdutoBonus(int numBonus, string codBarra)
        {
            return Json(new Entrada().BuscaProdutoBonus(numBonus, codBarra));
        }

        [HttpPost]
        public Boolean ConfereProdutoBonus([FromBody] ConferenciaBonus dados)
        {
            return new ConferenciaBonus().ConfereProdutoBonus(dados);
        }

        [Route("{numBonus}")]
        public JsonResult ExtratoBonus(int numBonus)
        {
            return Json(new ConferenciaBonus().ExtratoBonus(numBonus));
        }

        [HttpPut]
        [Route("{numBonus}/{codProd}")]
        public Boolean ReabreConfItemBonus(int numBonus, int codProd)
        {
            return new ConferenciaBonus().ReabreConfItemBonus(numBonus, codProd);
        }

        [HttpPut]
        [Route("{numBonus}")]
        public Boolean ReabreConfBonus(int numBonus)
        {
            return new ConferenciaBonus().ReabreConfBonus(numBonus);
        }

        [HttpPut]
        [Route("{numBonus}/{codFilial}/{codFunc}")]
        public string EnderecaBonus(int numBonus, int codFilial, int codFunc)
        {
            return new ConferenciaBonus().EnderecaBonus(numBonus, codFilial, codFunc);
        }

        [Route("{numBonus}")]
        public List<int> BuscaDadosImpressao(int numBonus)
        {
            return new ParametrosEndereca().BuscaDadosImpressao(numBonus);
        }

        [HttpPut]
        public string ChamaImpressao([FromBody] ParametrosEndereca parametros)
        {
            return new ParametrosEndereca().ChamaImpressao(parametros);
        }
    }
}
