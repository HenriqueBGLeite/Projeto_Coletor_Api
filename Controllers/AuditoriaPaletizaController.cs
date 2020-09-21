using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Authorization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using ProjetoColetorApi.Model;
using System.Net;

namespace ProjetoColetorApi.Controllers
{
    [Route("api/[Controller]/[Action]/")]
    [Authorize()]
    public class AuditoriaPaletizaController : Controller
    {
        [Route("{numCar}")]
        public string ValidaCarregamento(int numCar)
        {
            return new AuditoriaPaletiza().ValidaCarregamento(numCar);
        }

        [Route("{numCar}/{tipoConferencia}")]
        public int PendenciasCarga(int numCar, string tipoConferencia)
        {    
            return new AuditoriaPaletiza().PendenciasCarga(numCar, tipoConferencia);
        }

        [Route("{numCar}/{codFunc}")]
        public JsonResult ProximoCliente(int numCar, int codFunc)
        {
            return Json(new AuditoriaPaletiza().ProximoCliente(numCar, codFunc));
        }

        [HttpPut]
        public JsonResult PaletizaVolume([FromBody] AuditoriaPaletiza dados)
        {
            return Json(new AuditoriaPaletiza().PaletizaVolume(dados));
        }

        public JsonResult CabecalhoOs([FromBody] AuditoriaPaletiza dados)
        {
            return Json(new AuditoriaPaletiza().CabecalhoOs(dados));       
        }

        [HttpPut]
        [Route("{numOs}/{numVol}/{codFunc}")]
        public JsonResult AuditaVolumeOs(int numos, int numVol, int codFunc)
        {
            return Json(new AuditoriaPaletiza().AuditaVolumeOs(numos, numVol, codFunc));
        }

        [Route("{numOs}/{tipoConferencia}")]
        public JsonResult DivergenciaCarregamento(int numOs, string tipoConferencia)
        {
            return Json(new AuditoriaPaletiza().DivergenciaOs(numOs, tipoConferencia));
        }

        [Route("{numCar}/{tipoConferencia}")]
        public JsonResult PendenciaCarregamento(int numCar, string tipoConferencia)
        {
            return Json(new AuditoriaPaletiza().PendenciaCarregamento(numCar, tipoConferencia));
        }

        [Route("{numCar}/{numOs}/{tipoConferencia}")]
        public JsonResult AtualizaDivergPend(int numCar, int numOs, string tipoConferencia)
        {
            return Json(new AuditoriaPaletiza().AtualizaDivergPend(numCar, numOs, tipoConferencia));
        }
    }
}
