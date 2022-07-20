using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Linq;
using System.Text;
using SparkCsharp;
using System.Diagnostics;
using SparkCsharp.SparkComponent;
using SparkCsharp.SparkProxy;
using RapidSerializer.IO;
using Microsoft.Bond;
using Microsoft.ObjectStore;
using Microsoft.Search.ObjectStore;
using Microsoft.Bing.MultimediaRepository;
using System;

namespace ObjectStoreSparkOnebox
{
    class SparkJob
    {

        public static object ExecuteDriver(IEnvironmentProxy env)
        {
            var table = new ObjectStoreUtils.TableID("Multimedia", "PageData", ObjectStoreUtils.DC.MultiBN);
            var conf = ObjectStoreUtils.NewConfProxy(env, new List<ObjectStoreUtils.TableID> { table });

            conf.Set("spark.executor.instances", "120");

            var inputStream = "https://cosmos11.osdinfra.net/cosmos/ObjectStoreSpark/shares/MMRepository.prod/PageCaption/v-yangtian/InputPageKey.tsv";
            using (var sc = env.NewSparkContext(conf))
            {
                long recordsUploaded = CosmosUtils.CreateRDDFromCosmos(sc, inputStream, "\r\n",
                        false)
                    .Map(r => string.Format("{0}\t{1}", r, ReadTitle(r)))
                    .UploadToCosmos("https://cosmos11.osdinfra.net/cosmos/ObjectStoreSpark/shares/MMRepository.prod/PageCaption/v-yangtian/page_title_20220721.tsv");

                return recordsUploaded;
            }
        }
        public const string EnvironmentEndpoint = "objectstoremulti.prod.co.binginternal.com:83/sds";
        public const string NamespaceName = "Multimedia";
        public const string TableName = "PageData";

        public const string columnName = "BasicFeatures";
        public const string featureName1 = "PageTitle"; // subkey
        public const string featureName2 = "DomainKey"; // subkey
        public const string featureName3 = "Host"; // subkey
        public const string featureName4 = "VideoData"; // subkey

        public static DateTime s_baseTime = new DateTime(1970, 1, 1);

        public static String ReadTitle(string pageKey)
        {
            using (var client = Client.Builder<Multimedia.PageDataKey, Multimedia.PageDataValue>(environment: EnvironmentEndpoint,
                                                  osNamespace: NamespaceName,
                                                  osTable: TableName,
                                                  timeout: new TimeSpan(0, 0, 0, 500),
                                                  maxRetries: 1).Create())
            {
                var key = new Multimedia.PageDataKey
                {
                    PageKey = pageKey
                };

                var keys = new List<Multimedia.PageDataKey> { key };

                List<ColumnLocation> locations = new List<ColumnLocation>();

                locations.Add(new ColumnLocation(columnName, featureName1));
                locations.Add(new ColumnLocation(columnName, featureName2));
                locations.Add(new ColumnLocation(columnName, featureName3));
                locations.Add(new ColumnLocation(columnName, featureName4));

                var task = client.ColumnReadFromPrimary(keys, locations).WithDebugInfoEnabled().SendAsyncWithDebugFullResponse();

                try
                {
                    task.Wait();
                }
                catch (Exception ex)
                {
                    //Console.WriteLine("Read() - Exception: {0}, {1}", ex.Message, ex.StackTrace);
                }

                var results = task.Result;

                //Console.WriteLine("results.SubResponses.Count = " + results.SubResponses.Count()); // the count should equal to number of keys

                if (results.SubResponses.Count() > 0)
                {

                    Multimedia.StringFeatureValue pageTitleFeature;
                    Multimedia.StringFeatureValue domainFeature;
                    Multimedia.StringFeatureValue hostFeature;
                    Multimedia.StringFeatureValue videoDataFeature;

                    ObjectStoreWireProtocol.OSColumnOperationResultType res;

                    res = results.SubResponses.First().ColumnRecord.GetColumnValue(locations[0].ColumnName, locations[0].SubKey, out pageTitleFeature);
                    res = results.SubResponses.First().ColumnRecord.GetColumnValue(locations[1].ColumnName, locations[1].SubKey, out domainFeature);
                    res = results.SubResponses.First().ColumnRecord.GetColumnValue(locations[2].ColumnName, locations[2].SubKey, out hostFeature);
                    res = results.SubResponses.First().ColumnRecord.GetColumnValue(locations[2].ColumnName, locations[3].SubKey, out videoDataFeature);

                    if (pageTitleFeature != null)
                    {
                        //Console.WriteLine($"pageTitle : {pageTitleFeature.Value}");
                        return pageTitleFeature.Value;
                    }

                    if (domainFeature != null)
                        //Console.WriteLine($"domainKey : {domainFeature.Value}");

                        if (hostFeature != null)
                            //Console.WriteLine($"host : {hostFeature.Value}");
                            if (videoDataFeature != null)
                                //Console.WriteLine($"videoData : {videoDataFeature.Value}");

                                ////if (pageTitleFeature != null && domainFeature != null)
                                ////{
                                ////	return $"pageTitle : {pageTitleFeature.Value} \t domainKey : {domainFeature.Value}";
                                ////}
                                ////else
                                ////{
                                ////	return "pageTitleFeature or domainFeature is null";
                                ////}
                                return string.Empty;
                }
                else
                {
                    return string.Empty;
                }

                return string.Empty;
            }
        }
    }
}
