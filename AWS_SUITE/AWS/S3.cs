using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using AWS_SUITE.Exceptions;
using AWS_SUITE.Models;
using AWS_SUITE.Models.S3;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

/**
* @author Umair Qayyum
*
* @date - 5/22/2019 1:03:22 PM 
*/

namespace AWS_SUITE
{
    public class S3
    {
        public AWS_Credentials Credentials { get; set; }

        #region Constructors
        public S3()
        {
            this.Credentials = new AWS_Credentials();
        }

        public S3(AWS_Credentials credentials)
        {
            this.Credentials = credentials;
        }
        #endregion

        #region getS3Client
        public AmazonS3Client getS3Client()
        {
            try
            {
                return new AmazonS3Client();
            }
            catch(Exception ex)
            {
                throw ex.InnerException;
            }
        }

        public AmazonS3Client getS3Client(AWS_Credentials credentials)
        {
            if (credentials is null || credentials.AWS_AccessKey is null || credentials.AWS_SecretKey is null || credentials.Region is null)
                throw new CredentialsNotProvidedException();

            try
            {
                return new AmazonS3Client(credentials.AWS_AccessKey, credentials.AWS_SecretKey, credentials.Region);
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
        }

        public AmazonS3Client getS3Client(string access_key, string secret_key)
        {
            try
            {
                return new AmazonS3Client(access_key, secret_key);
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
        }

        public AmazonS3Client getS3Client(string access_key, string secret_key, RegionEndpoint region)
        {
            try
            {
                return new AmazonS3Client(access_key, secret_key, region);
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
        }

        public AmazonS3Client getS3Client(RegionEndpoint region)
        {
            try
            {
                return new AmazonS3Client(region);
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
        }
        #endregion

        #region Upload
        public void UploadToS3(S3File file, AWS_Credentials credentials)
        {
            if (file is null || file.RemoteFilePath is null || file.Bucket is null || file.LocalFilePath is null)
                throw new Exception("S3 File is NULL or has some NULL attributes");

            if (credentials is null || credentials.AWS_AccessKey is null || credentials.AWS_SecretKey is null || credentials.Region is null)
                throw new CredentialsNotProvidedException();

            try
            {
                TransferUtility fileTransferUtility = new
                    TransferUtility(getS3Client(credentials));

                TransferUtilityUploadRequest fileTransferUtilityRequest = new TransferUtilityUploadRequest
                {
                    BucketName = file.Bucket,
                    FilePath = file.LocalFilePath,
                    Key = file.RemoteFilePath,
                };
                fileTransferUtility.Upload(fileTransferUtilityRequest);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void UploadToS3(S3File file, AmazonS3Client client)
        {
            if (file is null || file.RemoteFilePath is null || file.Bucket is null || file.LocalFilePath is null)
                throw new Exception("S3 File is NULL or has some NULL attributes");

            try
            {
                TransferUtility fileTransferUtility = new
                    TransferUtility(client);

                TransferUtilityUploadRequest fileTransferUtilityRequest = new TransferUtilityUploadRequest
                {
                    BucketName = file.Bucket,
                    FilePath = file.LocalFilePath,
                    Key = file.RemoteFilePath,
                };
                fileTransferUtility.Upload(fileTransferUtilityRequest);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void UploadToS3(S3File file)
        {
            if (file is null || file.RemoteFilePath is null || file.Bucket is null || file.LocalFilePath is null)
                throw new Exception("S3 File is NULL or has some NULL attributes");

            if(Credentials is null || Credentials.AWS_AccessKey is null || Credentials.AWS_SecretKey is null || Credentials.Region is null)
                throw new CredentialsNotProvidedException();

            try
            {
                TransferUtility fileTransferUtility = new
                    TransferUtility(getS3Client(Credentials));

                TransferUtilityUploadRequest fileTransferUtilityRequest = new TransferUtilityUploadRequest
                {
                    BucketName = file.Bucket,
                    FilePath = file.LocalFilePath,
                    Key = file.RemoteFilePath,
                };
                fileTransferUtility.Upload(fileTransferUtilityRequest);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Download
        public void DownloadFromS3(S3File file, AWS_Credentials credentials)
        {
            if (file is null || file.RemoteFilePath is null || file.Bucket is null || file.LocalFilePath is null)
                throw new Exception("S3 File is NULL or has some NULL attributes");

            if (credentials is null || credentials.AWS_AccessKey is null || credentials.AWS_SecretKey is null || credentials.Region is null)
                throw new CredentialsNotProvidedException();

            try
            {
                TransferUtility fileTransferUtility = new
                    TransferUtility(getS3Client(credentials));

                TransferUtilityDownloadRequest fileTransferUtilityRequest = new TransferUtilityDownloadRequest
                {
                    BucketName = file.Bucket,
                    FilePath = file.LocalFilePath,
                    Key = file.RemoteFilePath,
                };
                fileTransferUtility.Download(fileTransferUtilityRequest);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void DownloadFromS3(S3File file, AmazonS3Client client)
        {
            if (file is null || file.RemoteFilePath is null || file.Bucket is null || file.LocalFilePath is null)
                throw new Exception("S3 File is NULL or has some NULL attributes");

            try
            {
                TransferUtility fileTransferUtility = new
                    TransferUtility(client);

                TransferUtilityDownloadRequest fileTransferUtilityRequest = new TransferUtilityDownloadRequest
                {
                    BucketName = file.Bucket,
                    FilePath = file.LocalFilePath,
                    Key = file.RemoteFilePath,
                };
                fileTransferUtility.Download(fileTransferUtilityRequest);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void DownloadFromS3(S3File file)
        {
            if (file is null || file.RemoteFilePath is null || file.Bucket is null || file.LocalFilePath is null)
                throw new Exception("S3 File is NULL or has some NULL attributes");

            if (Credentials is null || Credentials.AWS_AccessKey is null || Credentials.AWS_SecretKey is null || Credentials.Region is null)
                throw new CredentialsNotProvidedException();

            try
            {
                TransferUtility fileTransferUtility = new
                    TransferUtility(getS3Client(Credentials));

                TransferUtilityDownloadRequest fileTransferUtilityRequest = new TransferUtilityDownloadRequest
                {
                    BucketName = file.Bucket,
                    FilePath = file.LocalFilePath,
                    Key = file.RemoteFilePath,
                };
                fileTransferUtility.Download(fileTransferUtilityRequest);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
        
        #region GetBuckets
        public List<string> GetAllBuckets()
        {
            if (Credentials is null || Credentials.AWS_AccessKey is null || Credentials.AWS_SecretKey is null || Credentials.Region is null)
                throw new CredentialsNotProvidedException();

            List<string> buckets = new List<string>();
            using (AmazonS3Client client = getS3Client(Credentials))
            {
                ListBucketsRequest bucketFetchUtility = new
                    ListBucketsRequest();

                Task<ListBucketsResponse> bucketsResponse = client.ListBucketsAsync(bucketFetchUtility);
                bucketsResponse.Wait();

                if (bucketsResponse.IsCompleted)
                {
                    foreach (S3Bucket bucket in bucketsResponse.Result.Buckets)
                    {
                        buckets.Add(bucket.BucketName);
                    }
                }
            }
            return buckets;
        }

        public List<string> GetAllBuckets(AWS_Credentials credentials)
        {
            if (credentials is null || credentials.AWS_AccessKey is null || credentials.AWS_SecretKey is null || credentials.Region is null)
                throw new CredentialsNotProvidedException();

            List<string> buckets = new List<string>();
            using (AmazonS3Client client = getS3Client(credentials))
            {
                ListBucketsRequest bucketFetchUtility = new
                    ListBucketsRequest();

                Task<ListBucketsResponse> bucketsResponse = client.ListBucketsAsync(bucketFetchUtility);
                bucketsResponse.Wait();

                if (bucketsResponse.IsCompleted)
                {
                    foreach (S3Bucket bucket in bucketsResponse.Result.Buckets)
                    {
                        buckets.Add(bucket.BucketName);
                    }
                }
            }
            return buckets;
        }

        public List<string> GetAllBuckets(AmazonS3Client client)
        {

            List<string> buckets = new List<string>();
            ListBucketsRequest bucketFetchUtility = new
                ListBucketsRequest();

            Task<ListBucketsResponse> bucketsResponse = client.ListBucketsAsync(bucketFetchUtility);
            bucketsResponse.Wait();

            if (bucketsResponse.IsCompleted)
            {
                foreach (S3Bucket bucket in bucketsResponse.Result.Buckets)
                {
                    buckets.Add(bucket.BucketName);
                }
            }
            return buckets;
        }
        #endregion
    }
}