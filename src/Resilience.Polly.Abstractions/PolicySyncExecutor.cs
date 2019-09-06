using System;
using System.Collections.Generic;
using System.Linq;
using Polly;
using Polly.Registry;

namespace Resilience.Polly.Abstractions
{
    public class PolicySyncExecutor : IPolicySyncExecutor
    {
        public PolicyRegistry PolicyRegistry { get; set; }

        public PolicySyncExecutor(IEnumerable<ISyncPolicy> policies)
        {
            var syncPolicies = policies ?? throw new ArgumentNullException(nameof(policies));

            PolicyRegistry = new PolicyRegistry
            {
                [nameof(PolicySyncExecutor)] = Policy.Wrap(syncPolicies.ToArray())
            };
        }

        public T Execute<T>(Func<T> action)
        {
            var policy = PolicyRegistry.Get<ISyncPolicy>(nameof(PolicySyncExecutor));
            return policy.Execute(action);
        }

        public void Execute(Action action)
        {
            var policy = PolicyRegistry.Get<ISyncPolicy>(nameof(PolicySyncExecutor));
            policy.Execute(action);
        }

        public T Execute<T>(Func<Context, T> action, Context context)
        {
            var policy = PolicyRegistry.Get<ISyncPolicy>(nameof(PolicySyncExecutor));
            return policy.Execute(action, context);
        }

        public void Execute(Action<Context> action, Context context)
        {
            var policy = PolicyRegistry.Get<ISyncPolicy>(nameof(PolicySyncExecutor));
            policy.Execute(action, context);
        }
    }
}
