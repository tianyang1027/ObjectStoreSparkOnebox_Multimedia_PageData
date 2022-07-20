using System;
using System.Collections.Generic;
using System.IO;
using SparkCsharp;
using System.Diagnostics;
using System.Text;
using SparkCsharp.SparkComponent;
using SparkCsharp.SparkProxy;

namespace ObjectStoreSparkOnebox
{
    class Program
    {
        static void Main(string[] args)
        {
            SparkClient.Instance = new ObjectStoreSparkClient(Console.WriteLine, Path.Combine(Environment.CurrentDirectory, @"..\..\spark-binaries\confCluster\env.cmd"));
            Spark.Program = new SparkCsharpSparkProgram("ObjectStoreSparkOnebox_Multimedia_PageData");
            Spark.Program.Register();
            Spark.Program.AddDependency("ObjectStoreSparkOnebox.exe");
            Spark.Program.SynchronizeDependencies();
            var ret = Spark.Run(env =>
            {
                return ObjectStoreSparkOnebox.SparkJob.ExecuteDriver(env);
            });

            ObjectStoreUtils.Log("The result is {0}.", ret);
        }
    }
}
