using AWS_SUITE.Models;
using System;

/**
* @author Umair Qayyum
*
* @date - 5/22/2019 1:03:22 PM 
*/

namespace AWS_SUITE
{
    public class S3
    {
        AWS_Credentials Credentials;

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
    }
}