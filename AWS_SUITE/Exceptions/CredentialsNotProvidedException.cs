using System;
using System.Collections.Generic;
using System.Text;


/**
* @author Umair Qayyum
*
* @date - 5/24/2019 12:16:52 PM 
*/

namespace AWS_SUITE.Exceptions
{
    class CredentialsNotProvidedException : Exception
    {
        public CredentialsNotProvidedException()
            : base(string.Format("AWS credentials not found."))
        {

        }

    }
}
