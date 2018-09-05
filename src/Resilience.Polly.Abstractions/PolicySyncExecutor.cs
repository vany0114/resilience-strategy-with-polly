using System;
using System.Collections.Generic;
using System.Linq;
using Polly;

namespace Resilience.Polly.Abstractions
{
    public class PolicySyncExecutor : IPolicySyncExecutor
    {
        private readonly IEnumerable<ISyncPolicy> _syncPolicies;

        public PolicySyncExecutor(IEnumerable<ISyncPolicy> policies)
        {
            _syncPolicies = policies ?? throw new ArgumentNullException(nameof(policies));
        }

        public T Execute<T>(Func<T> action)
        {
            return Executor(action);
        }

        public void Execute(Action action)
        {
            Executor(action);
        }

        private T Executor<T>(Func<T> action)
        {
            // Executes the action applying all the policies defined in the wrapper
            var policyWrap = Policy.Wrap(_syncPolicies.ToArray());
            return policyWrap.Execute(action);
        }

        private void Executor(Action action)
        {
            // Executes the action applying all the policies defined in the wrapper
            var policyWrap = Policy.Wrap(_syncPolicies.ToArray());
            policyWrap.Execute(action);
        }
    }
}
