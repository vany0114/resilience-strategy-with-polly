using System;

namespace Resilience.Polly.Abstractions
{
    public interface IPolicySyncExecutor
    {
        T Execute<T>(Func<T> action);

        void Execute(Action action);
    }
}
