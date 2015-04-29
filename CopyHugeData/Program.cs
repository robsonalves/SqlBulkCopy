using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using Newtonsoft.Json;

namespace CopyHugeData
{
    internal class Program
    {
        public static Dictionary<string, string> Connection { get; set; }
        private static string _fileConnection = "ConnectionStrings.json";
        private static SqlDataReader _dr = null;
        private static SqlBulkCopy _bulkCopy = null;
        private static SqlConnection conn = null;
        private static Stopwatch _watch = null;

        static void Main(string[] args)
        {
            StartWatch();
            LoadJsonConnection();
            LoadData();
            BulkCopy();
            Destroy();
            StopWatch();
        }

        private static void StartWatch()
        {
            _watch = new Stopwatch();
            _watch.Start();
            Console.WriteLine("Started At: " + DateTime.Now.TimeOfDay);
        }

        private static void StopWatch()
        {
            _watch.Stop();
            Console.Write("Tempo em minutos: " + _watch.Elapsed.Minutes);
            Console.ReadKey();
        }

        private static void Destroy()
        {
            _bulkCopy.Close();
            _dr.Close();
        }

        private static void BulkCopy()
        {
            _bulkCopy = new SqlBulkCopy(Connection["Destiny"]);
            _bulkCopy.SqlRowsCopied += BulkCopyOnSqlRowsCopied;

            _bulkCopy.DestinationTableName = Connection["TableDestiny"];
            Console.WriteLine("Iniciando BulkCopy");
            _bulkCopy.BulkCopyTimeout = int.MaxValue;
            Console.Write("Timeout setted : " + int.MaxValue);
            _bulkCopy.WriteToServer(_dr);
            Console.WriteLine("Fim do BulkCopy");

        }

        private static void BulkCopyOnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs sqlRowsCopiedEventArgs)
        {
            Console.Write(sqlRowsCopiedEventArgs.RowsCopied);
        }



        private static void LoadData()
        {
            string sql = string.Format("select * from {0}",Connection["TableDestiny"]);
            Console.WriteLine(sql);
            conn = new SqlConnection(Connection["Source"]);
            SqlCommand cmd = new SqlCommand(sql, conn);
            conn.Open();
            Console.WriteLine(conn.State.ToString());
            _dr = cmd.ExecuteReader();
            Console.WriteLine("Data reader carregado com suceso");
        }

        private static void LoadJsonConnection()
        {
            var fileinfo = new FileInfo(_fileConnection);
            if (fileinfo.Exists)
            {
                using (StreamReader sr = new StreamReader(_fileConnection))
                {
                    string json = sr.ReadToEnd();
                    dynamic array = JsonConvert.DeserializeObject(json);

                    Connection = new Dictionary<string, string>();
                    Connection.Add("Source", array.ConnectionSource.ToString());
                    Connection.Add("Destiny", array.ConnectionDestiny.ToString());
                    Connection.Add("TableDestiny", array.TableDestiny.ToString());
                }
            }
        }
    }
}
