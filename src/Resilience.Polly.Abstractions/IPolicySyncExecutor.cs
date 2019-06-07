using Polly;
using Polly.Registry;
using System;

namespace Resilience.Polly.Abstractions
{
    public interface IPolicySyncExecutor
    {
        PolicyRegistry PolicyRegistry { get; set; }

        T Execute<T>(Func<T> action);

        T Execute<T>(Func<Context, T> action, Context context);

        void Execute(Action action);

        void Execute(Action<Context> action, Context context);
    }
}
