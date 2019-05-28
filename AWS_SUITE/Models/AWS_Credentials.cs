using System;
using Amazon;

/**
* @author Umair Qayyum
*
* @date - 5/22/2019 1:12:59 PM 
*/
namespace AWS_SUITE.Models
{
    public class AWS_Credentials
    {
        public string AWS_AccessKey { get; set; }
        public string AWS_SecretKey { get; set; }
        public RegionEndpoint Region { get; set; }

        #region Constructors
        public AWS_Credentials()
        {
        }

        public AWS_Credentials(string aWS_AccessKey, string aWS_SecretKey)
        {
            AWS_AccessKey = aWS_AccessKey ?? throw new ArgumentNullException(nameof(aWS_AccessKey));
            AWS_SecretKey = aWS_SecretKey ?? throw new ArgumentNullException(nameof(aWS_SecretKey));
        }

        public AWS_Credentials(string aWS_AccessKey, string aWS_SecretKey, RegionEndpoint region) : this(aWS_AccessKey, aWS_SecretKey)
        {
            Region = region ?? throw new ArgumentNullException(nameof(region));
        }
        #endregion
    }
}