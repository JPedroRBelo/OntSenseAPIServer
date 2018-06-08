using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Net.Sockets;
using System.IO;
using VDS.RDF.Query;
using VDS.RDF.Update;
using System.Runtime.CompilerServices;
using System.Globalization;


namespace OntSenseAPIServer
{
    class Program
    {
        private static SparqlRemoteUpdateEndpoint endpoint;
        //private static readonly string ONT_SENSE_URL = "http://localhost:3030/ontsense";
        //private static readonly string ONT_SENSE_URL = "http://172.26.164.88:3030/ontsense";    // URL address of the triple store
        private static readonly string ONT_SENSE_URL = "http://192.168.0.102:3030/ontsense";
        private static readonly int PORT = 11000;
        private static int count;
        private static Queue<string> queue;


        static void Main(string[] args)
        {
            queue = new Queue<string>();
            count = 0;
            //Then create our Endpoint instance
            endpoint = new SparqlRemoteUpdateEndpoint(ONT_SENSE_URL);

            TcpListener ServerSocket = new TcpListener(PORT);
            ServerSocket.Start();
            Console.WriteLine("OntSense API started on Port " + PORT + ", with " + ONT_SENSE_URL + " Server...");
            TcpClient clientSocket = ServerSocket.AcceptTcpClient();
            handleClient client = new handleClient();
            client.startClient(clientSocket);
            while (true)
            {
                string auxString = "";

                lock (queue)
                {
                    if (queue.Count > 0)
                    {
                        auxString = queue.Dequeue();

                    }
                    else
                    {
                        Thread.Yield();
                    }
                }
                if (!auxString.Equals(""))
                    endpoint.Update(auxString);
            }
        }
        public class handleClient
        {
            TcpClient clientSocket;
            public void startClient(TcpClient inClientSocket)
            {
                this.clientSocket = inClientSocket;
                Thread ctThread = new Thread(Listener);
                ctThread.Start();
            }
            private void Listener()
            {
                byte[] buffer = new byte[10];
                while (true)
                {
                    BinaryReader reader = new BinaryReader(clientSocket.GetStream());
                    try
                    {
                        string r = reader.ReadString();
                        count++;
                        if (count % 1000 == 0)
                        {
                            Console.WriteLine("Messages received: " + count);
                        }
                        lock (queue)
                        {
                            queue.Enqueue(r);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
            }
        }
    }
}