
/**
* @author Umair Qayyum
*
* @date - 5/22/2019 1:51:05 PM 
*/

namespace AWS_SUITE.Models.S3
{
    public class S3File
    {
        public string Bucket { get; set; }
        public string LocalFilePath { get; set; }
        public string RemoteFilePath { get; set; }

        #region Constructors
        public S3File()
        {
        }

        public S3File(string bucket, string local_path, string remote_path)
        {
            this.Bucket = bucket;
            this.LocalFilePath = local_path;
            this.RemoteFilePath = remote_path;
        }
        #endregion

    }
}