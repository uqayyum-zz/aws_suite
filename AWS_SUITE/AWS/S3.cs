using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using AWS_SUITE.Exceptions;
using AWS_SUITE.Models;
using AWS_SUITE.Models.S3;
using System;
using System.Collections.Generic;
using System.Linq;
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
            catch (Exception ex)
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

            if (Credentials is null || Credentials.AWS_AccessKey is null || Credentials.AWS_SecretKey is null || Credentials.Region is null)
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

        #region Delete
        public void DeleteFromS3(S3File file, AWS_Credentials credentials)
        {
            if (file is null || file.RemoteFilePath is null || file.Bucket is null)
                throw new Exception("S3 File is NULL or has some NULL attributes");

            if (credentials is null || credentials.AWS_AccessKey is null || credentials.AWS_SecretKey is null || credentials.Region is null)
                throw new CredentialsNotProvidedException();

            try
            {
                using (AmazonS3Client client = getS3Client(credentials))
                {
                    DeleteObjectRequest request = new
                        DeleteObjectRequest();

                    request.BucketName = file.Bucket;
                    request.Key = file.RemoteFilePath;

                    Task<DeleteObjectResponse> task = client.DeleteObjectAsync(request);
                    task.Wait();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void DeleteFromS3(S3File file, AmazonS3Client client)
        {
            if (file is null || file.RemoteFilePath is null || file.Bucket is null || file.LocalFilePath is null)
                throw new Exception("S3 File is NULL or has some NULL attributes");

            try
            {
                DeleteObjectRequest request = new
                    DeleteObjectRequest();

                request.BucketName = file.Bucket;
                request.Key = file.RemoteFilePath;

                Task<DeleteObjectResponse> task = client.DeleteObjectAsync(request);
                task.Wait();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void DeleteFromS3(S3File file)
        {
            if (file is null || file.RemoteFilePath is null || file.Bucket is null || file.LocalFilePath is null)
                throw new Exception("S3 File is NULL or has some NULL attributes");

            if (Credentials is null || Credentials.AWS_AccessKey is null || Credentials.AWS_SecretKey is null || Credentials.Region is null)
                throw new CredentialsNotProvidedException();

            try
            {
                using (AmazonS3Client client = getS3Client(Credentials))
                {
                    DeleteObjectRequest request = new
                        DeleteObjectRequest();

                    request.BucketName = file.Bucket;
                    request.Key = file.RemoteFilePath;

                    Task<DeleteObjectResponse> task = client.DeleteObjectAsync(request);
                    task.Wait();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region CreateBucket
        public void CreateBucket(string bucket_name)
        {
            if (Credentials is null || Credentials.AWS_AccessKey is null || Credentials.AWS_SecretKey is null || Credentials.Region is null)
                throw new CredentialsNotProvidedException();

            if (bucket_name is null)
            {
                throw new ArgumentNullException();
            }

            using (AmazonS3Client client = getS3Client(Credentials))
            {
                List<string> existing_buckets = GetAllBuckets();
                try
                {
                    if (existing_buckets.Where(b => b.ToString() == bucket_name).FirstOrDefault() is null)
                    {
                        var request = new PutBucketRequest
                        {
                            BucketName = bucket_name,
                            UseClientRegion = true
                        };

                        Task<PutBucketResponse> task = client.PutBucketAsync(request);
                        task.Wait();
                    }
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        public void CreateBucket(string bucket_name, AWS_Credentials credentials)
        {
            if (credentials is null || credentials.AWS_AccessKey is null || credentials.AWS_SecretKey is null || credentials.Region is null)
                throw new CredentialsNotProvidedException();

            if (bucket_name is null)
            {
                throw new ArgumentNullException();
            }

            using (AmazonS3Client client = getS3Client(credentials))
            {
                List<string> existing_buckets = GetAllBuckets(credentials);
                try
                {
                    if (existing_buckets.Where(b => b.ToString() == bucket_name).FirstOrDefault() is null)
                    {
                        var request = new PutBucketRequest
                        {
                            BucketName = bucket_name,
                            UseClientRegion = true
                        };

                        Task<PutBucketResponse> task = client.PutBucketAsync(request);
                        task.Wait();
                    }
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        public void CreateBucket(string bucket_name, AmazonS3Client client)
        {
            if (bucket_name is null)
            {
                throw new ArgumentNullException();
            }
            List<string> existing_buckets = GetAllBuckets(client);
            try
            {
                if (existing_buckets.Where(b => b.ToString() == bucket_name).FirstOrDefault() is null)
                {
                    var request = new PutBucketRequest
                    {
                        BucketName = bucket_name,
                        UseClientRegion = true
                    };

                    Task<PutBucketResponse> task = client.PutBucketAsync(request);
                    task.Wait();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        #endregion

        #region DeleteBucket
        public void DeleteBucket(string bucket_name)
        {
            if (Credentials is null || Credentials.AWS_AccessKey is null || Credentials.AWS_SecretKey is null || Credentials.Region is null)
                throw new CredentialsNotProvidedException();

            if (bucket_name is null)
            {
                throw new ArgumentNullException();
            }

            using (AmazonS3Client client = getS3Client(Credentials))
            {
                List<string> existing_buckets = GetAllBuckets();
                try
                {
                    if (!(existing_buckets.Where(b => b.ToString() == bucket_name).FirstOrDefault() is null))
                    {
                        var request = new DeleteBucketRequest
                        {
                            BucketName = bucket_name,
                            UseClientRegion = true
                        };

                        Task<DeleteBucketResponse> task = client.DeleteBucketAsync(request);
                        task.Wait();
                    }
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        public void DeleteBucket(string bucket_name, AWS_Credentials credentials)
        {
            if (credentials is null || credentials.AWS_AccessKey is null || credentials.AWS_SecretKey is null || credentials.Region is null)
                throw new CredentialsNotProvidedException();

            if (bucket_name is null)
            {
                throw new ArgumentNullException();
            }

            using (AmazonS3Client client = getS3Client(credentials))
            {
                List<string> existing_buckets = GetAllBuckets(credentials);
                try
                {
                    if (!(existing_buckets.Where(b => b.ToString() == bucket_name).FirstOrDefault() is null))
                    {
                        var request = new DeleteBucketRequest
                        {
                            BucketName = bucket_name,
                            UseClientRegion = true
                        };

                        Task<DeleteBucketResponse> task = client.DeleteBucketAsync(request);
                        task.Wait();
                    }
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        public void DeleteBucket(string bucket_name, AmazonS3Client client)
        {
            if (bucket_name is null)
            {
                throw new ArgumentNullException();
            }

            List<string> existing_buckets = GetAllBuckets(client);
            try
            {
                if (!(existing_buckets.Where(b => b.ToString() == bucket_name).FirstOrDefault() is null))
                {
                    var request = new DeleteBucketRequest
                    {
                        BucketName = bucket_name,
                        UseClientRegion = true
                    };

                    Task<DeleteBucketResponse> task = client.DeleteBucketAsync(request);
                    task.Wait();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        #endregion

        #region GetBucketKeys
        public List<string> GetBucketKeys(string bucket_name, string prefix = null, long max_count = -1)
        {
            if (Credentials is null || Credentials.AWS_AccessKey is null || Credentials.AWS_SecretKey is null || Credentials.Region is null)
                throw new CredentialsNotProvidedException();

            if (bucket_name is null)
            {
                throw new ArgumentNullException();
            }

            List<string> keys = new List<string>();
            try
            {
                using (var client = new AmazonS3Client(Credentials.AWS_AccessKey, Credentials.AWS_SecretKey, Credentials.Region))
                {
                    ListObjectsV2Request request = new ListObjectsV2Request();
                    request.BucketName = bucket_name;
                    request.Prefix = prefix;

                    if (max_count > 0 && max_count < 1000)
                        request.MaxKeys = (int)max_count;

                    Task<ListObjectsV2Response> task;
                    ListObjectsV2Response response;
                    long iterator = 0;
                    do
                    {
                        task = client.ListObjectsV2Async(request);
                        task.Wait();

                        response = task.Result;
                        keys.AddRange(response.S3Objects.Select(x => x.Key));
                        iterator += response.S3Objects.Count;
                        request.ContinuationToken = response.NextContinuationToken;
                    } while (response.IsTruncated == true && ((max_count == -1) ? true : iterator < max_count));
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return keys;
        }

        public List<string> GetBucketKeys(string bucket_name, AWS_Credentials credentials, string prefix = null, long max_count = 100_000)
        {
            if (credentials is null || credentials.AWS_AccessKey is null || credentials.AWS_SecretKey is null || credentials.Region is null)
                throw new CredentialsNotProvidedException();

            if (bucket_name is null)
            {
                throw new ArgumentNullException();
            }

            List<string> keys = new List<string>();
            try
            {
                using (var client = new AmazonS3Client(credentials.AWS_AccessKey, credentials.AWS_SecretKey, credentials.Region))
                {
                    ListObjectsV2Request request = new ListObjectsV2Request();
                    request.BucketName = bucket_name;
                    request.Prefix = prefix;

                    if (max_count > 0 && max_count < 1000)
                        request.MaxKeys = (int)max_count;

                    Task<ListObjectsV2Response> task;
                    ListObjectsV2Response response;
                    long iterator = 0;
                    do
                    {
                        task = client.ListObjectsV2Async(request);
                        task.Wait();

                        response = task.Result;
                        keys.AddRange(response.S3Objects.Select(x => x.Key));
                        iterator += response.S3Objects.Count;
                        request.ContinuationToken = response.NextContinuationToken;
                    } while (response.IsTruncated == true && ((max_count == -1) ? true : iterator < max_count));
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return keys;
        }

        public List<string> GetBucketKeys(string bucket_name, AmazonS3Client client, string prefix = null, long max_count = 100_000)
        {
            if (bucket_name is null)
            {
                throw new ArgumentNullException();
            }

            List<string> keys = new List<string>();
            try
            {
                ListObjectsV2Request request = new ListObjectsV2Request();
                request.BucketName = bucket_name;
                request.Prefix = prefix;

                if (max_count > 0 && max_count < 1000)
                    request.MaxKeys = (int)max_count;

                Task<ListObjectsV2Response> task;
                ListObjectsV2Response response;
                long iterator = 0;
                do
                {
                    task = client.ListObjectsV2Async(request);
                    task.Wait();

                    response = task.Result;
                    keys.AddRange(response.S3Objects.Select(x => x.Key));
                    iterator += response.S3Objects.Count;
                    request.ContinuationToken = response.NextContinuationToken;
                } while (response.IsTruncated == true && ((max_count == -1) ? true : iterator < max_count));
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return keys;
        }
        #endregion
    }
}