using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
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
            DateTime start = DateTime.Now;

            JsonResult resposta = Json(new ConferenciaSaida().CabecalhoOs(numOs, numVol));

            DateTime endTime = DateTime.Now;

            TimeSpan tempoTotal = endTime.Subtract(start);

            if (tempoTotal.Seconds >= 2)
            {
                new ConferenciaSaida().EnviaEmail("O.S/Volume: " + numOs + "/" + numVol + " - Segundos: " + tempoTotal.Seconds + " / Milissegundos: " + tempoTotal.Milliseconds, "CabecalhoOs");
            }
                        
            if (tempoTotal.Seconds >= 1)
            {
                new ConferenciaSaida().GravaLog(numOs, numVol, "CabecalhoOs", tempoTotal.Seconds, tempoTotal.Milliseconds, 0);
            }

            return resposta;
        }

        [Route("{codBarra}/{numOs}/{numVol}/{filial}")]
        public JsonResult ProdutoOsVolume(string codBarra, int numOs, int numVol, int filial)
        {
            return Json(new ConferenciaSaida().ProdutoOsVolume(codBarra, numOs, numVol, filial));
        }

        [Route("{codBarra}/{numOs}/{filial}")]
        public JsonResult ProdutoOs(string codBarra, int numOs, int filial)
        {
            return Json(new ConferenciaSaida().ProdutoOs(codBarra, numOs, filial));
        }

        [HttpPut]
        public Boolean ConfereVolumeCheckout([FromBody] ConferenciaSaida dados)
        {
            DateTime start = DateTime.Now;

            Boolean resposta = dados.ConfereVolumeCheckout(dados);

            DateTime endTime = DateTime.Now;

            TimeSpan tempoTotal = endTime.Subtract(start);

            if (tempoTotal.Seconds >= 2)
            {
                new ConferenciaSaida().EnviaEmail("O.S/Volume: " + dados.Numos + "/" + dados.Numvol + " - Produto: " + dados.Codprod + " Segundos: " + tempoTotal.Seconds + " / Milissegundos: " + tempoTotal.Milliseconds, "ConfereVolumeCheckout");
            }

            if (tempoTotal.Seconds >= 1)
            {
                new ConferenciaSaida().GravaLog(dados.Numos, dados.Numvol, "ConfereVolumeCheckout", tempoTotal.Seconds, tempoTotal.Milliseconds, dados.Codprod);
            }

            return resposta;
        }

        [HttpPut]
        public Boolean ConfereVolumeCaixaFechada([FromBody] ConferenciaSaida dados)
        {
            DateTime start = DateTime.Now;

            Boolean resposta = dados.ConfereVolumeCaixaFechada(dados);

            DateTime endTime = DateTime.Now;

            TimeSpan tempoTotal = endTime.Subtract(start);

            if (tempoTotal.Seconds >= 2)
            {
                new ConferenciaSaida().EnviaEmail("O.S/Volume: " + dados.Numos + "/" + dados.Numvol + " - Produto: " + dados.Codprod + " Segundos: " + tempoTotal.Seconds + " / Milissegundos: " + tempoTotal.Milliseconds, "ConfereVolumeCaixaFechada");
            }

                if (tempoTotal.Seconds >= 1)
            {
                new ConferenciaSaida().GravaLog(dados.Numos, dados.Numvol, "ConfereVolumeCaixaFechada", tempoTotal.Seconds, tempoTotal.Milliseconds, dados.Codprod);
            }

            return resposta;
        }

        [Route("{numOs}/{numBox}")]
        public JsonResult buscaQtVolumePendente(int numOs, int numBox)
        {
            return Json(new ConferenciaSaida().buscaQtVolumePendente(numOs, numBox));
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

        [HttpPut]
        [Route("{numOs}/{codProd}")]
        public Boolean ReabreConferenciaProduto(int numos, int codprod)
        {
            return new ConferenciaSaida().ReabreConferenciaProduto(numos, codprod);
        }

        [HttpPut]
        [Route("{numOs}")]
        public Boolean ReabreConferenciaOs(int numos)
        {
            return new ConferenciaSaida().ReabreConferenciaOs(numos);
        }
    }
}
