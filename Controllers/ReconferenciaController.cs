using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Authorization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using ProjetoColetorApi.Model;

namespace ProjetoColetorApi.Controllers
{
    [Route("api/[Controller]/[Action]/")]
    [Authorize()]
    public class ReconferenciaController : Controller
    {
        [Route("{numCar}")]
        public string ValidaCarregamento(int numCar)
        {
            return new Reconferencia().ValidaCarregamento(numCar);
        }

        [Route("{numCar}")]
        public int PendenciasCarga(int numCar)
        {    
            return new Reconferencia().PendenciasCarga(numCar);
        }
    }
}
