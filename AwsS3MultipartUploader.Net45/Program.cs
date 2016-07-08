using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

using Mono.Options;
using Framework.Cloud.Aws;
using Framework.Log;

namespace AwsS3MultipartUploader.Net45
{
    class Program
    {
        static int Main(string[] args)
        {

            //! systemName
            //! us-east-1 The US East (Virginia) endpoint.
            //! us-west-1 The US West (N. California) endpoint.
            //! us-west-2 The US West (Oregon) endpoint.
            //! eu-west-1 The EU West (Ireland) endpoint.
            //! ap-northeast-1 The Asia Pacific (Tokyo) endpoint.
            //! ap-southeast-1 The Asia Pacific (Singapore) endpoint.
            //! ap-southeast-2 The Asia Pacific (Sydney) endpoint.
            //! sa-east-1 The South America (Sao Paulo) endpoint.
            //! us-gov-west-1 The US GovCloud West (Oregon) endpoint.
            //! cn-north-1 The China (Beijing) endpoint.

            string awsAccessKeyId = null;
            string awsSecretAccessKey = null;
            string systemName = "ap-northeast-1";
            string bucketName = null;
            string keyName = null;
            string filePath = null;
            int part = 5;
            int parallel = 10;
            int timeout = 300;
            bool serverEncryption = false;
            string logPath = null;
            bool showHelp = false;

            OptionSet optionSet = new OptionSet()
            {
                {"a|accesskey=", "AWS access key.", v => awsAccessKeyId = v},
                {"s|secretkey=", "AWS secret access key.", v => awsSecretAccessKey = v},
                {"r|region=", "AWS region.", v => systemName = v},
                {"b|bucket=", "AWS bucket name.", v => bucketName = v},
                {"k|key=", "AWS key name.", v => keyName = v},
                {"p|path=", "upload file path.", v => filePath = v},
                {"part=", "part size(MB).", (int v) => part = v},
                {"parallel=", "parallel upload count 1 to 64.", (int v) => parallel = v},
                {"e|encryption", "server side encryption (AES-256).", v => serverEncryption = v != null},
                {"t|timeout=", "timeout(sec).", (int v) => timeout = v},
                {"l|log=", "log file.", v => logPath = v},
                {"?|help", "show help.", v => showHelp = v != null}
            };

            try
            {
                List<string> extra = optionSet.Parse(args);
                //extra.ForEach(t => Console.WriteLine(t));
            }
            catch (OptionException ex)
            {
                ShowExceptionMessage(ex);
                return 1;
            }

            if (showHelp || awsAccessKeyId == null || awsSecretAccessKey == null || bucketName == null || keyName == null || filePath == null)
            {
                ShowUsage(optionSet);
                return 1;
            }

            //! part size: 5MB - 5GB
            int partSize = part;
            partSize = Math.Min(Math.Max(partSize, 5), 5000);
            long partBytes = partSize * 1048576L; // 1024 x 1024

            //! parallel count : 1 - 64
            int parallelCount = Math.Min(Math.Max(parallel, 1), 64);

            try
            {
                Logger.AddConsoleTraceListener();
                Logger.AddListener(logPath);

                Logger.WriteLine("--------------------------------------------------");
                Logger.WriteLine("region : {0}", systemName);
                Logger.WriteLine("part size (byte) : {0}", partBytes);
                Logger.WriteLine("part size (MB) : {0}", partSize);
                Logger.WriteLine("parallel count : {0}", parallelCount);
                Logger.WriteLine("timeout (sec) : {0}", timeout);
                Logger.WriteLine("server side encryption (AES-256) : {0}", serverEncryption.ToString());

                Logger.WriteLine("file path : {0}", filePath);

                FileInfo fileInfo = new FileInfo(filePath);
                long fileLength = fileInfo.Length;

                Logger.WriteLine("file size (byte) : {0}", fileLength.ToString("#,0"));
                Logger.WriteLine("{0} ({1} byte) → {2}/{3}", filePath, fileLength.ToString("#,0"), bucketName, keyName);
                Logger.WriteLine("backet name : {0}", bucketName);
                Logger.WriteLine("key name : {0}", keyName);

                DateTime startDateTime = DateTime.Now;
                Logger.WriteLine("start datetime : {0}", startDateTime.ToString("yyyy-MM-dd HH:mm:ss:fff"));


                S3MultipartUpload s3 = new S3MultipartUpload();
                s3.Timeout = TimeSpan.FromSeconds(timeout);
                s3.ThreadCount = parallelCount;
                s3.Region = systemName;

                s3.UploadFile(awsAccessKeyId, awsSecretAccessKey, bucketName, keyName, filePath, partBytes, serverEncryption);
                //multipartUploader.Upload(awsAccessKey, awsSecretAccessKey, bucketName, keyName, filePath, partBytes, serverEncryption);

                //! get metadata
                //ObjectMetadata objectMetadata = new ObjectMetadata(awsAccessKey, awsSecretAccessKey, regionURL, bucketName, keyName);

                double transferBps = (fileLength / (s3.Elapsed.TotalMilliseconds * 0.001)) * 8d;
                double transferMBps = transferBps / 1048576d;

                //Logger.WriteLine("Stopwatch : {0}({1} msec)", sw.Elapsed, sw.ElapsedMilliseconds);
                //Logger.WriteLine("{0} ({1} byte) → {2}/{3}  {4} bps ({5} Mbps)", filePath, fileLength.ToString("#,0"), bucketName, keyName, transferBps, transferMBps.ToString("F"));
                //Logger.WriteLine("{0}/{1} ETag : {2}", bucketName, keyName, objectMetadata.ETag);
                Logger.WriteLine("stopwatch : {0}", s3.Elapsed);
                Logger.WriteLine("average rate (bps) : {0}", transferBps);
                Logger.WriteLine("average rate (Mbps) : {0}", transferMBps.ToString("F"));
                return 0;
            }
            catch (Exception ex)
            {
                Logger.WriteLine(ex);
                return 1;
            }
            finally
            {
                Logger.ClearListener();
            }
        }


        static void ShowExceptionMessage(OptionException optionException)
        {
            Console.Error.Write("{0}: ", System.Reflection.Assembly.GetEntryAssembly().GetName().Name);
            Console.Error.WriteLine(optionException.Message);
            Console.Error.WriteLine("Try `{0} --help' for more information.", System.Reflection.Assembly.GetEntryAssembly().GetName().Name);
        }

        static void ShowUsage(OptionSet optionSet)
        {
            Console.Error.WriteLine("Usage: {0} [options]", System.Reflection.Assembly.GetEntryAssembly().GetName().Name);
            Console.Error.WriteLine();
            Console.Error.WriteLine("Options:");
            optionSet.WriteOptionDescriptions(Console.Error);
        }

    }
}
