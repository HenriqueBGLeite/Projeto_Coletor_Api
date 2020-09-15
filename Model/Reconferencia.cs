using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Net.Mail;
using System.Net;

namespace ProjetoColetorApi.Model
{
    public class Reconferencia
    {
        public string ValidaCarregamento(int numCar)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            string existeCarga;
            DataTable carregamento = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append($"select case when count(*) = 1 then 'S' else 'N' end existeCarga from pccarreg where numcar = {numCar} and dt_cancel is null");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(carregamento);

                if (carregamento.Rows.Count > 0 && numCar != 0)
                {
                    existeCarga = carregamento.Rows[0]["existeCarga"].ToString();

                    return existeCarga;
                }
                else
                {
                    return "N";
                }
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

                throw new Exception(ex.Message);
            }
            finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();
            }
        }

        public int PendenciasCarga(int numCar)
        {
            OracleConnection connection = DataBase.novaConexao();
            OracleCommand exec = connection.CreateCommand();

            int qtdPendencia;
            DataTable divergenciaOs = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select count(os.numvol) as divergencia");
                query.Append($" from (select distinct numos from pcmovendpend where numcar = {numCar} and dtestorno is null) mov inner join pcvolumeos os on (mov.numos = os.numos)");
                query.Append("where os.dtconf2 is null");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(divergenciaOs);

                if (divergenciaOs.Rows.Count > 0)
                {
                    qtdPendencia = Convert.ToInt32(divergenciaOs.Rows[0]["divergencia"]);
                    
                    return qtdPendencia;
                } else
                {
                    return -1;
                }
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    throw new Exception(ex.Message);
                }

                exec.Dispose();
                connection.Dispose();

                throw new Exception(ex.Message);
            }
            finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();
            }
        }

    }
}


