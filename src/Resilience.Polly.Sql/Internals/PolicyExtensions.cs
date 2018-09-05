using System.Collections.Generic;
using Polly;

namespace Resilience.Polly.Sql.Internals
{
    internal static class PolicyExtensions
    {
        public static string GetPolicyName(this List<IAsyncPolicy> policies)
        {
            return string.Join("_", policies);
        }

        public static string GetPolicyName(this List<ISyncPolicy> policies)
        {
            return string.Join("_", policies);
        }
    }
}
