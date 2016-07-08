using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Amazon;
using Amazon.S3;

namespace Framework.Cloud.Aws
{

    public class S3PartFile
    {
        public string path;
        public long start;
        public long end;
        public long length;
    }


}
