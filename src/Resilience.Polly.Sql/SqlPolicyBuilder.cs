using Resilience.Polly.Sql.Internals;

namespace Resilience.Polly.Sql
{
    public class SqlPolicyBuilder
    {
        public ISqlAsyncPolicyBuilder UseAsyncExecutor()
        {
            return new SqlAsyncPolicyBuilder();
        }

        public ISqlAsyncPolicyBuilder UseAsyncExecutorWithSharedPolicies()
        {
            return new SqlAsyncPolicyBuilder(true);
        }

        public ISqlSyncPolicyBuilder UseSyncExecutor()
        {
            return new SqlSyncPolicyBuilder();
        }

        public ISqlSyncPolicyBuilder UseSyncExecutorWithSharedPolicies()
        {
            return new SqlSyncPolicyBuilder(true);
        }
    }
}
