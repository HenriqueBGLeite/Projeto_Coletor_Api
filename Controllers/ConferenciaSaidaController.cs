using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http;
using ProjetoColetorApi.Model;


namespace ProjetoColetorApi.Controllers
{
    [Route("api/[Controller]/[Action]/")]
    [Authorize()]
    public class ConferenciaSaidaController : Controller
    {
        [Route("{numOs}/{numVol}")]
        public JsonResult CabecalhoOs(int numOs, int numVol)
        {
            return Json(new ConferenciaSaida().CabecalhoOs(numOs, numVol));
        }

        [Route("{codBarra}/{numOs}/{numVol}/{filial}")]
        public JsonResult ProdutoOsVolume(string codBarra, int numOs, int numVol, int filial)
        {
            return Json(new ConferenciaSaida().ProdutoOsVolume(codBarra, numOs, numVol, filial));
        }

        [HttpPut]
        public Boolean ConfereVolumeCheckout([FromBody] ConferenciaSaida dados)
        {
            return dados.ConfereVolumeCheckout(dados);
        }

        [HttpPut]
        public Boolean ConfereVolumeCaixaFechada([FromBody] ConferenciaSaida dados)
        {
            return dados.ConfereVolumeCaixaFechada(dados);
        }

        [Route("{numOs}/{numVol}")]
        public JsonResult buscaQtOsPendente(int numOs, int numVol)
        {
            return Json(new ConferenciaSaida().buscaQtOsPendente(numOs, numVol));
        }

        [HttpPut]
        [Route("{numOs}")]
        public Boolean FinalizaConferenciaOs(int numOs)
        {
            return new ConferenciaSaida().FinalizaConferenciaOs(numOs);
        }

        [Route("{numOs}")]
        public JsonResult DivergenciaOs(int numOs)
        {
            return Json(new ConferenciaSaida().DivergenciaOs(numOs));
        }

        [Route("{numCar}")]
        public JsonResult PendenciaOsCarregamento(int numcar)
        {
            return Json(new ConferenciaSaida().PendenciaOsCarregamento(numcar));
        }
    }
}
