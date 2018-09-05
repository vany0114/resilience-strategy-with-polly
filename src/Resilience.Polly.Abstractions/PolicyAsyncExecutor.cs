using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Polly;

namespace Resilience.Polly.Abstractions
{
    public class PolicyAsyncExecutor : IPolicyAsyncExecutor
    {
        private readonly IEnumerable<IAsyncPolicy> _asyncPolicies;

        public PolicyAsyncExecutor(IEnumerable<IAsyncPolicy> policies)
        {
            _asyncPolicies = policies ?? throw new ArgumentNullException(nameof(policies));
        }

        public async Task<T> ExecuteAsync<T>(Func<Task<T>> action)
        {
            return await AsyncExecutor(action);
        }

        public async Task ExecuteAsync(Func<Task> action)
        {
            await AsyncExecutor(action);
        }

        private async Task AsyncExecutor(Func<Task> action)
        {
            // Executes the action applying all the policies defined in the wrapper
            var policyWrap = Policy.WrapAsync(_asyncPolicies.ToArray());
            await policyWrap.ExecuteAsync(action);
        }

        private async Task<T> AsyncExecutor<T>(Func<Task<T>> action)
        {
            // Executes the action applying all the policies defined in the wrapper
            var policyWrap = Policy.WrapAsync(_asyncPolicies.ToArray());
            return await policyWrap.ExecuteAsync(action);
        }
    }
}
