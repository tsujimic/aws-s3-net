using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace Framework.Cloud.Aws
{

    class S3MultipartUpload
    {
        public TimeSpan Timeout;
        public string Region;
        public TimeSpan Elapsed;
        public int ThreadCount;
        public int RetryCount;
        public TimeSpan RetryInterval;

        readonly object QUEUE_ACECSS_LOCK = new object();
        readonly object WAIT_FOR_COMPLETION_LOCK = new object();
        Queue<UploadPartRequest> _partsToUpload = new Queue<UploadPartRequest>();
        Thread[] _executedThreads;
        UploadPartInvoker[] _uploadPartInvoker;
        List<UploadPartResponse> _uploadResponses = new List<UploadPartResponse>();
        int _totalNumberOfParts;


        public S3MultipartUpload()
        {
            this.Timeout = TimeSpan.FromSeconds(300);
            this.ThreadCount = 10;
            this.Region = "ap-northeast-1";
            this.RetryCount = 3;
            this.RetryInterval = TimeSpan.FromSeconds(1);
        }


        public void UploadFile(string awsAccessKey, string awsSecretAccessKey, string bucketName, string keyName, string uploadFilePath, long partSize, bool serverEncryption)
        {
            //! Start Stopwatch
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                var regionEndpoint = RegionEndpoint.GetBySystemName(Region);
                var amazonS3Config = new AmazonS3Config()
                {
                    Timeout = this.Timeout,
                    RegionEndpoint = regionEndpoint
                };

                var amazonS3Client = new AmazonS3Client(awsAccessKey, awsSecretAccessKey, amazonS3Config);

                try
                {
                    internalMultipartUpload(amazonS3Client, bucketName, keyName, uploadFilePath, partSize, serverEncryption);
                }
                catch
                {
                    throw;
                }
                finally
                {
                    amazonS3Client.Dispose();
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                //! Stop Stopwatch
                sw.Stop();
                this.Elapsed = sw.Elapsed;

            }

        }


        void internalMultipartUpload(IAmazonS3 amazonS3, string bucketName, string keyName, string uploadFilePath, long partSize, bool serverEncryption)
        {
            //! file size
            FileInfo fileInfo = new FileInfo(uploadFilePath);
            long contentLength = fileInfo.Length;

            //! Low-Level interface
            //! 1. Initialize
            InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = keyName,
                //CannedACL = S3CannedACL.PublicRead,
                ServerSideEncryptionMethod = serverEncryption ? ServerSideEncryptionMethod.AES256 : ServerSideEncryptionMethod.None 
            };


            InitiateMultipartUploadResponse initiateMultipartUploadResponse = amazonS3.InitiateMultipartUpload(initiateMultipartUploadRequest);
            string uploadId = initiateMultipartUploadResponse.UploadId;

            //! 2. Upload parts
            try
            {
                long filePosition = 0;
                for (int i = 1; filePosition < contentLength; i++)
                {
                    //partSize = Math.Min(partSize, (contentLength - filePosition));

                    //! Create request to upload a part
                    UploadPartRequest uploadPartRequest = new UploadPartRequest
                    {
                        BucketName = bucketName,
                        Key = keyName,
                        UploadId = uploadId,
                        PartNumber = i,
                        PartSize = partSize,
                        FilePosition = filePosition,
                        FilePath = uploadFilePath,
                    };

                    _partsToUpload.Enqueue(uploadPartRequest);

                    filePosition += partSize;
                }

                int threadCount = this.ThreadCount;
                _totalNumberOfParts = _partsToUpload.Count;
                _uploadPartInvoker = new UploadPartInvoker[threadCount];
                _executedThreads = new Thread[threadCount];

                for (int i = 0; i < threadCount; i++)
                {
                    _uploadPartInvoker[i] = new UploadPartInvoker(amazonS3, this);

                    Thread thread = new Thread(new ThreadStart(_uploadPartInvoker[i].Execute));
                    thread.Name = "Uploader " + i; 
                    thread.IsBackground = true;
                    _executedThreads[i] = thread;

                    thread.Start();

                }

                waitTillAllThreadsComplete();


                //! 3 Complete
                CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = keyName,
                    UploadId = uploadId,
                    //PartETags = partEtags

                };

                completeMultipartUploadRequest.AddPartETags(_uploadResponses);
                CompleteMultipartUploadResponse completeUploadResponse = amazonS3.CompleteMultipartUpload(completeMultipartUploadRequest);


            }
            catch
            {
                //Console.WriteLine("Exception occurred: {0}", exception.Message);
                shutdown(amazonS3, bucketName, keyName, uploadId);

                throw;
            }

        }


        void shutdown(IAmazonS3 amazonS3, string bucketName, string keyName, string uploadId)
        {
            bool anyAlive = true;
            for (int i = 0; anyAlive && i < 5; i++)
            {
                anyAlive = false;
                foreach (Thread thread in _executedThreads)
                {
                    try
                    {
                        if (thread.IsAlive)
                        {
                            thread.Abort();
                            anyAlive = true;
                        }
                    }
                    catch { }
                }

            }


            AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = keyName,
                UploadId = uploadId
            };

            amazonS3.AbortMultipartUpload(abortMultipartUploadRequest);


        }


        void addResponse(UploadPartResponse response)
        {
            lock (WAIT_FOR_COMPLETION_LOCK)
            {
                _uploadResponses.Add(response);
            }

        }

        void waitTillAllThreadsComplete()
        {
            lock (WAIT_FOR_COMPLETION_LOCK)
            {
                while (_uploadResponses.Count != _totalNumberOfParts)
                {
                    Monitor.Wait(WAIT_FOR_COMPLETION_LOCK, 100);

                    foreach (UploadPartInvoker invoker in _uploadPartInvoker)
                    {
                        if (invoker.LastException != null)
                            throw invoker.LastException;
                    }
                }
            }
        }



        class UploadPartInvoker
        {
            IAmazonS3 _s3Client;
            S3MultipartUpload _upload;
            Exception _lastException;

            internal UploadPartInvoker(IAmazonS3 s3Client, S3MultipartUpload upload)
            {
                _s3Client = s3Client;
                _upload = upload;

            }

            internal Exception LastException
            {
                get
                { 
                    return _lastException; 
                }

            }

            UploadPartRequest getNextPartRequest()
            {
                lock (_upload.QUEUE_ACECSS_LOCK)
                {
                    if (_upload._partsToUpload.Count == 0)
                        return null;

                    return _upload._partsToUpload.Dequeue();
                }
            }

            internal void _Execute()
            {
                UploadPartRequest request = null;
                while ((request = getNextPartRequest()) != null)
                {
                    _lastException = null;
                    try
                    {
                        _upload.addResponse(_s3Client.UploadPart(request));
                    }
                    catch (ThreadAbortException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        _lastException = e;

                        lock (_upload.WAIT_FOR_COMPLETION_LOCK)
                        {
                            Monitor.Pulse(_upload.WAIT_FOR_COMPLETION_LOCK);
                        }

                        break;
                    }
                }
            }

            internal void Execute()
            {
                UploadPartRequest request = null;
                while ((request = getNextPartRequest()) != null)
                {
                    uploadPart(request);

                    if (_lastException != null)
                    {
                        lock (_upload.WAIT_FOR_COMPLETION_LOCK)
                        {
                            Monitor.Pulse(_upload.WAIT_FOR_COMPLETION_LOCK);
                        }

                        break;
                    }
                }
            }

            internal void uploadPart(UploadPartRequest request)
            {
                Exception uploadException = null;

                for (int i = 0; i < _upload.RetryCount; i++)
                {
                    try
                    {
                        _upload.addResponse(_s3Client.UploadPart(request));
                        uploadException = null;
                        return;
                    }
                    catch (ThreadAbortException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        uploadException = ex;
                        Thread.Sleep(_upload.RetryInterval);
                    }
                }

                if (uploadException != null)
                    _lastException = uploadException;

            }


            //internal void addNextPartRequest(UploadPartRequest request)
            //{
            //    lock (_upload.QUEUE_ACECSS_LOCK)
            //    {
            //        _upload._partsToUpload.Enqueue(request);
            //    }
            //}



        }





    }
}
