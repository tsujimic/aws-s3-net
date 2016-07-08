using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Amazon;
using Amazon.S3;

namespace Framework.Cloud.Aws
{

    public class S3MultipartDownloadRequest
    {

        IAmazonS3 _amazonS3;
        string _bucketName;
        string _keyName;
        AutoResetEvent _waitHandle = new AutoResetEvent(false);
        List<S3PartFile> _partFiles = new List<S3PartFile>();

        public IAmazonS3 AmazonS3
        {
            get { return _amazonS3; }
            set { _amazonS3 = value; }
        }

        public string BucketName
        {
            get { return _bucketName; }
            set { _bucketName = value; }
        }

        public string KeyName
        {
            get { return _keyName; }
            set { _keyName = value; }
        }

        public WaitHandle WaitHandle
        {
            get { return _waitHandle; }
        }

        public List<S3PartFile> PartFiles
        {
            get { return _partFiles; }
        }


        internal void SignalWaitHandle()
        {
            _waitHandle.Set();
        }


    }

}
