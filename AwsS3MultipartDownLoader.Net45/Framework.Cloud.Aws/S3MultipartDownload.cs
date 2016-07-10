using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace Framework.Cloud.Aws
{

    class S3MultipartDownload
    {
        public TimeSpan Timeout;
        public int ThreadCount;
        public string Region;
        public TimeSpan Elapsed;
        public int RetryCount;
        public TimeSpan RetryInterval;

        Exception _lastException;
        long _bytesDownload;

        //readonly object WRITE_ACECSS_LOCK = new object();
        ReaderWriterLock _readerWriterLock = new ReaderWriterLock();
        //ReaderWriterLockSlim readerWriterLockSlim = new ReaderWriterLockSlim();
        FileStream _outputStream = null;

        public S3MultipartDownload()
        {
            this.Timeout = TimeSpan.FromSeconds(300);
            this.ThreadCount = 10;
            this.Region = "ap-northeast-1";
            this.RetryCount = 3;
            this.RetryInterval = TimeSpan.FromSeconds(1);

            _lastException = null;
        }


        public long DownloadFile(string awsAccessKey, string awsSecretAccessKey, string bucketName, string keyName, string filePath, long partSize)
        {
            //! Start Stopwatch
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                var regionEndpoint = RegionEndpoint.GetBySystemName(this.Region);
                var amazonS3Config = new AmazonS3Config()
                {
                    Timeout = this.Timeout,
                    RegionEndpoint = regionEndpoint
                };

                var amazonS3Client = new AmazonS3Client(awsAccessKey, awsSecretAccessKey, amazonS3Config);

                try
                {
                    return internalDownloadFile(amazonS3Client, bucketName, keyName, filePath, partSize);
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


        long internalDownloadFile(IAmazonS3 amazonS3, string bucketName, string keyName, string filePath, long partSize)
        {
            GetObjectMetadataResponse getObjectMetadataResponse = GetObjectMetadata(amazonS3, bucketName, keyName);
            long contentLength = getObjectMetadataResponse.ContentLength;

            List<S3PartFile> partFiles = new List<S3PartFile>();
            long filePosition = 0;
            long fileLength;
            for (int i = 0; filePosition < contentLength; i++)
            {
                fileLength = Math.Min(partSize, (contentLength - filePosition));

                partFiles.Add(new S3PartFile()
                {
                    //path = Path.GetRandomFileName(),
                    start = filePosition,
                    end = filePosition + fileLength - 1,
                    length = fileLength
                });

                filePosition += fileLength;
            }


            FileInfo fi = new FileInfo(filePath);
            Directory.CreateDirectory(fi.DirectoryName);

            _outputStream = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            _outputStream.SetLength(contentLength);


            List<S3MultipartDownloadRequest> downloadRequests = new List<S3MultipartDownloadRequest>();
            for (int n = 0; n < this.ThreadCount; n++)
            {
                S3MultipartDownloadRequest request = new S3MultipartDownloadRequest();
                request.AmazonS3 = amazonS3;
                request.BucketName = bucketName;
                request.KeyName = keyName;

                downloadRequests.Add(request);
            }

            for (int n = 0; n < partFiles.Count; n++)
            {
                int i = n % this.ThreadCount;
                downloadRequests[i].PartFiles.Add(partFiles[n]);
            }


            //! download
            _bytesDownload = 0;

            List<WaitHandle> waitHandles = new List<WaitHandle>();
            for (int n = 0; n < this.ThreadCount; n++)
            {
                Thread thread = new Thread(new ParameterizedThreadStart(GetObject));
                thread.IsBackground = true;
                waitHandles.Add(downloadRequests[n].WaitHandle);
                thread.Start((object)downloadRequests[n]);
            }


            WaitHandle.WaitAll(waitHandles.ToArray());

            if (_outputStream != null)
                _outputStream.Close();

            //! is exception
            if (_lastException != null)
            {
                File.Delete(filePath);
                throw _lastException;
            }

            return _bytesDownload;
        }




        GetObjectMetadataResponse GetObjectMetadata(IAmazonS3 amazonS3, string bucketName, string keyName)
        {
            GetObjectMetadataRequest objectMetadataRequest = new GetObjectMetadataRequest();
            objectMetadataRequest.BucketName = bucketName;
            objectMetadataRequest.Key = keyName;

            GetObjectMetadataResponse objectMetadataResponse = amazonS3.GetObjectMetadata(objectMetadataRequest);
            return objectMetadataResponse;

        }


        void GetObject(object param)
        {
            S3MultipartDownloadRequest request = (S3MultipartDownloadRequest)param;

            try
            {
                IAmazonS3 amazonS3 = request.AmazonS3;
                string bucketName = request.BucketName;
                string keyName = request.KeyName;

                foreach (S3PartFile part in request.PartFiles)
                {
                    GetObject(amazonS3, bucketName, keyName, part.start, part.length, this.RetryCount, this.RetryInterval);

                    if (_lastException != null)
                        return;
                }


            }
            catch (Exception ex)
            {
                _lastException = ex;

            }
            finally
            {
                request.SignalWaitHandle();
            }

        }



        void GetObject(IAmazonS3 amazonS3, string bucketName, string keyName, long start, long length, int retryCount, TimeSpan retryInterval)
        {
            Exception getObjectException = null;

            GetObjectRequest request = new GetObjectRequest();
            request.BucketName = bucketName;
            request.Key = keyName;
            long end = start + length - 1;
            request.ByteRange = new ByteRange(start, end);

            for (int i = 0; i < retryCount; i++)
            {
                try
                {
                    using (GetObjectResponse response = amazonS3.GetObject(request))
                    {
                        //long bytesWrite = WriteStream(getObjectResponse.ResponseStream, outputStream, start, length);
                        long bytesWrite = WriteStream(response.ResponseStream, _outputStream, start, length);

                        Interlocked.Add(ref _bytesDownload, bytesWrite);
                    }

                    getObjectException = null;
                    return;

                }
                catch (Exception ex)
                {
                    getObjectException = ex;
                    Thread.Sleep(retryInterval);
                }

            }

            if (getObjectException != null)
                throw getObjectException;

        }


        long WriteStream(Stream responseStream, Stream output, long start)
        {
            //int bufferSize = 16384;
            int bufferSize = 8192;
            BufferedStream bufferedStream = new BufferedStream(responseStream);
            //BufferedStream bufferedStream = new BufferedStream(responseStream, bufferSize);
            byte[] buffer = new byte[bufferSize];

            long current = 0;
            int bytesRead = 0;
            while ((bytesRead = bufferedStream.Read(buffer, 0, buffer.Length)) > 0)
            {
                //lock (WRITE_ACECSS_LOCK)
                //{
                _readerWriterLock.AcquireWriterLock(System.Threading.Timeout.Infinite);
                //readerWriterLockSlim.EnterWriteLock();

                output.Seek(start + current, SeekOrigin.Begin);
                output.Write(buffer, 0, bytesRead);

                _readerWriterLock.ReleaseWriterLock();
                //readerWriterLockSlim.ExitWriteLock();
                //}
                current += bytesRead;

            }

            return current;

        }


        long WriteStream(Stream responseStream, Stream output, long start, long length)
        {
            byte[] buffer = new byte[length];
            BufferedStream bufferedStream = new BufferedStream(responseStream);

            int current = 0;
            int bytesRead = 0;
            int count = buffer.Length;
            while ((bytesRead = bufferedStream.Read(buffer, current, count)) > 0)
            {
                current += bytesRead;
                count -= bytesRead;
            }

            _readerWriterLock.AcquireWriterLock(System.Threading.Timeout.Infinite);
            output.Seek(start, SeekOrigin.Begin);
            output.Write(buffer, 0, buffer.Length);
            _readerWriterLock.ReleaseWriterLock();

            return current;

        }




    }
}
