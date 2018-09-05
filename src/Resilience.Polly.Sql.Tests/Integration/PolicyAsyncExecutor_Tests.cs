using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;
using System.Threading.Tasks;
using NUnit.Framework;
using Polly.CircuitBreaker;
using Polly.Timeout;

namespace Resilience.Polly.Sql.Tests.Integration
{
    [TestFixture, Explicit]
    // ReSharper disable once InconsistentNaming
    public class PolicyAsyncExecutor_Tests
    {
        private int _retriesCounter;
        private int _threshold;
        private bool _fallbackExecuted;
        private static SqlException _lastTransactionException;
        private static SqlException _lastTransientException;
        private static readonly Random RandomExceptionProvider = new Random();
        private static readonly Dictionary<int, SqlException> TransientExceptions = new Dictionary<int, SqlException>
        {
            {1, CreateSqlException(40613)},
            {2, CreateSqlException(40197)},
            {3, CreateSqlException(40501)},
            {4, CreateSqlException(49918)}
        };
        private static readonly Dictionary<int, SqlException> TransactionExceptions = new Dictionary<int, SqlException>
        {
            {1, CreateSqlException(40549)},
            {2, CreateSqlException(40550)}
        };

        [SetUp]
        public void SetUp()
        {
            _retriesCounter = 0;
            _threshold = 5;
            _fallbackExecuted = false;
        }

        public static SqlException GetRandomException()
        {
            lock (RandomExceptionProvider)
            {
                // To ensure it won't break the circuit.
                var ex = TransientExceptions[RandomExceptionProvider.Next(1, 5)];
                if (ex == _lastTransientException)
                    return GetRandomException();

                _lastTransientException = ex;
                return _lastTransientException;
            }
        }

        public static SqlException GetRandomTransactionException()
        {
            lock (RandomExceptionProvider)
            {
                // To ensure it won't break the circuit.
                var ex = TransactionExceptions[RandomExceptionProvider.Next(1, 3)];
                if (ex == _lastTransactionException)
                    return GetRandomTransactionException();

                _lastTransactionException = ex;
                return _lastTransactionException;
            }
        }

        private async Task<bool> DoSomethingAsync(int defaultTime = 500)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(defaultTime));

            if (++_retriesCounter <= _threshold - 1)
                throw GetRandomException();

            return true;
        }

        private async Task<bool> DoSomethingWithTransactionAsync(int defaultTime = 500)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(defaultTime));

            if (++_retriesCounter <= _threshold - 1)
                throw GetRandomTransactionException();

            return true;
        }

        private async Task<bool> DoSomethingToBreakTheCircuitAsync()
        {
            ++_retriesCounter;
            await Task.Delay(TimeSpan.FromMilliseconds(500));

            // to break the circuit we need to throw 3 exceptions (for this test) in a row
            throw TransientExceptions[1];
        }

        private async Task<bool> DoSomethingToBreakTheCircuitAndContinueAsync()
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500));

            if (++_retriesCounter <= 3)
                throw TransientExceptions[1]; // to break the circuit we need to throw 3 exceptions (for this test) in a row

            return true;
        }

        private async Task<bool> DoSomethingWithTransactionToBreakTheCircuitAsync()
        {
            ++_retriesCounter;
            await Task.Delay(TimeSpan.FromMilliseconds(500));

            // to break the circuit we need to throw 3 exceptions (for this test) in a row
            throw TransactionExceptions[1];
        }

        private async Task<bool> DoSomethingWithTransactionToBreakTheCircuitAndContinueAsync()
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500));

            if (++_retriesCounter <= 3)
                throw TransactionExceptions[1]; // to break the circuit we need to throw 3 exceptions (for this test) in a row

            return true;
        }

        private async Task<bool> DoFallbackAsync()
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            _fallbackExecuted = true;
            return true;
        }

        #region Builder

        [Test]
        [Category("resilientAsyncPolicy.Builder")]
        public void Should_Fails_Due_To_There_Are_No_Policies()
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
            {
                var builder = new SqlPolicyBuilder();
                var resilientAsyncPolicy = builder
                    .UseAsyncExecutor()
                    .Build();
            });

            Assert.IsInstanceOf<InvalidOperationException>(exception);
            Assert.AreEqual(exception.Message, "There are no policies to execute.");
        }

        [Test]
        [Category("resilientAsyncPolicy.Builder")]
        public void Should_Fails_Due_To_There_Are_Duplicated_Policies()
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
            {
                var builder = new SqlPolicyBuilder();
                var resilientAsyncPolicy = builder
                    .UseAsyncExecutor()
                    .WithDefaultPolicies()
                    .WithTransientErrors()
                    .Build();
            });

            Assert.IsInstanceOf<InvalidOperationException>(exception);
            Assert.AreEqual(exception.Message, "There are duplicated policies. When you use WithDefaultPolicies method, you can't use either WithTransientErrors. WithCircuitBreaker or WithOverallTimeout methods at the same time, because those policies are already included.");
        }

        [Test]
        [Category("resilientAsyncPolicy.Builder")]
        public void Should_Fails_Due_To_There_Is_TimeOut_Per_Retry_Policy_But_There_Are_NotRetry_Policies()
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
            {
                var builder = new SqlPolicyBuilder();
                var resilientAsyncPolicy = builder
                    .UseAsyncExecutor()
                    .WithTimeoutPerRetry(TimeSpan.FromSeconds(20))
                    .Build();
            });

            Assert.IsInstanceOf<InvalidOperationException>(exception);
            Assert.AreEqual(exception.Message, "You're trying to use Timeout per retries but you don't have Retry policies configured.");
        }

        #endregion

        #region Executor

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies")]
        public async Task Should_Retry_Four_Times_Without_Break_The_Circuit_And_Does_Not_Raise_An_Exception()
        {
            var expectedRetries = 4;
            var expectedTime = GetExpectedDelay(expectedRetries) + GetExpectedExecutionTime(expectedRetries);

            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .Build();

            var watch = Stopwatch.StartNew();
            var result = await resilientAsyncPolicy.ExecuteAsync(async () =>
            {
                // won't break the circuit since throws random exceptions.
                await DoSomethingAsync();
                return true;
            });

            watch.Stop();
            var currentTime = TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).Seconds;

            Assert.AreEqual(true, result);
            Assert.AreEqual(expectedTime.Seconds, currentTime);
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies")]
        public void Should_Retry_Three_Times_The_Same_Exception_And_The_Fourth_Attempt_Break_The_Circuit_And_Raise_A_BrokenCircuitException()
        {
            var expectedRetries = 3;
            var expectedTime = GetExpectedDelay(expectedRetries) + GetExpectedExecutionTime(expectedRetries);

            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .Build();

            var watch = Stopwatch.StartNew();
            var ex = Assert.ThrowsAsync<BrokenCircuitException>(async () =>
            {
                await resilientAsyncPolicy.ExecuteAsync(async () =>
                {
                    // will break the circuit since throws 3 times in a row the same exception
                    await DoSomethingToBreakTheCircuitAsync();
                });
            });

            watch.Stop();
            var currentTime = TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).Seconds;

            Assert.IsInstanceOf<BrokenCircuitException>(ex);
            Assert.AreEqual(ex.Message, "The circuit is now open and is not allowing calls.");
            Assert.AreEqual(expectedTime.Seconds, currentTime);
            Assert.AreEqual(expectedRetries, _retriesCounter);
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies")]
        public void Should_Do_Not_Retry_Due_To_The_Operation_Exceeds_The_Timeout_And_Raises_A_TimeoutRejectedException()
        {
            var expectedRetries = 0;
            var expectedTimeout = GetExpectedTimeout(5);

            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .Build();

            var watch = Stopwatch.StartNew();
            var ex = Assert.ThrowsAsync<TimeoutRejectedException>(async () =>
            {
                await resilientAsyncPolicy.ExecuteAsync(async () =>
                {
                    await DoSomethingAsync(120000);
                });
            });

            watch.Stop();
            var currentTime = TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds;

            Assert.IsInstanceOf<TimeoutRejectedException>(ex);
            Assert.AreEqual(expectedTimeout.TotalSeconds, Math.Round(currentTime));
            Assert.AreEqual(expectedRetries, _retriesCounter);
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies")]
        public void Should_Retry_Once_And_Interrupt_The_Attempt_Due_To_The_Retries_Exceeds_The_Timeout_And_Raises_A_TimeoutRejectedException()
        {
            var expectedRetries = 1;
            var expectedTimeout = GetExpectedTimeout(5);

            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .Build();

            var watch = Stopwatch.StartNew();
            var ex = Assert.ThrowsAsync<TimeoutRejectedException>(async () =>
            {
                await resilientAsyncPolicy.ExecuteAsync(async () =>
                {
                    await DoSomethingAsync(45000);
                });
            });

            watch.Stop();
            var currentTime = TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds;

            Assert.IsInstanceOf<TimeoutRejectedException>(ex);
            Assert.AreEqual(expectedTimeout.TotalSeconds, Math.Round(currentTime));
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies.WithTransaction")]
        public async Task Should_Retry_Transaction_Errors_Four_Times_Without_Break_The_Circuit_And_Does_Not_Raise_An_Exception()
        {
            var expectedRetries = 4;
            var expectedTime = GetExpectedDelay(expectedRetries) + GetExpectedExecutionTime(expectedRetries);

            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .WithTransaction()
                .Build();

            var watch = Stopwatch.StartNew();
            var result = await resilientAsyncPolicy.ExecuteAsync(async () =>
            {
                await DoSomethingWithTransactionAsync();
                return true;
            });

            watch.Stop();
            var currentTime = TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).Seconds;

            Assert.AreEqual(true, result);
            Assert.AreEqual(_threshold, _retriesCounter);
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies.WithTransaction")]
        public void Should_Retry_Transaction_Errors_Three_Times_The_Same_Exception_And_The_Fourth_Attempt_Break_The_Circuit_And_Raise_A_BrokenCircuitException()
        {
            var expectedRetries = 3;
            var expectedTime = GetExpectedDelay(expectedRetries) + GetExpectedExecutionTime(expectedRetries);

            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .WithTransaction()
                .Build();

            var watch = Stopwatch.StartNew();
            var ex = Assert.ThrowsAsync<BrokenCircuitException>(async () =>
            {
                await resilientAsyncPolicy.ExecuteAsync(async () =>
                {
                    // will break the circuit since throws 3 times in a row the same exception
                    await DoSomethingWithTransactionToBreakTheCircuitAsync();
                });
            });

            watch.Stop();
            var currentTime = TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).Seconds;

            Assert.IsInstanceOf<BrokenCircuitException>(ex);
            Assert.AreEqual(ex.Message, "The circuit is now open and is not allowing calls.");
            Assert.AreEqual(expectedTime.Seconds, currentTime);
            Assert.AreEqual(expectedRetries, _retriesCounter);
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies.WithTransaction")]
        public async Task Should_Transaction_Errors_Break_The_Circuit_And_Wait_For_The_Break_And_Try_Again_And_Does_Not_Raise_An_Exception()
        {
            var expectedRetries = 3;
            // 3 retries with exponential back-off + 4 executions (last is the successful one) + circuit break time
            var expectedTime = GetExpectedDelay(expectedRetries) + GetExpectedExecutionTime(4) + TimeSpan.FromSeconds(30);

            // IMPORTANT NOTE: in a real scenario, this behavior will only happen if we share the same instance of policies among requests.
            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .WithTransaction()
                .Build();

            var watch = Stopwatch.StartNew();
            var exFirstAttempt = Assert.ThrowsAsync<BrokenCircuitException>(async () =>
            {
                await resilientAsyncPolicy.ExecuteAsync(async () =>
                {
                    // will break the circuit since throws 3 times in a row the same exception
                    await DoSomethingWithTransactionToBreakTheCircuitAndContinueAsync();
                });
            });

            var watch2 = Stopwatch.StartNew();
            var exSecondAttempt = Assert.ThrowsAsync<BrokenCircuitException>(async () =>
            {
                await resilientAsyncPolicy.ExecuteAsync(async () =>
                {
                    // will fail immediately
                    await DoSomethingWithTransactionToBreakTheCircuitAndContinueAsync();
                });
            });
            watch2.Stop();

            // await the break time
            await Task.Delay(TimeSpan.FromSeconds(30));

            // will executes successfully because the circuit is half-open now
            var result = await resilientAsyncPolicy.ExecuteAsync(async () =>
            {
                await DoSomethingWithTransactionToBreakTheCircuitAndContinueAsync();
                return true;
            });

            watch.Stop();
            var currentTime = TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).Seconds;
            var attemptTimeAfterCircuitBroke = TimeSpan.FromMilliseconds(watch2.ElapsedMilliseconds).Seconds;

            Assert.AreEqual(true, result);
            Assert.IsInstanceOf<BrokenCircuitException>(exFirstAttempt);
            Assert.IsInstanceOf<BrokenCircuitException>(exSecondAttempt);
            Assert.AreEqual(expectedTime.Seconds, currentTime);
            Assert.AreEqual(0, attemptTimeAfterCircuitBroke);
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies.WithTransaction.WithFallback")]
        public async Task Should_Fallback_After_Timeout_Exception()
        {
            var result = false;
            var expectedRetries = 0;
            var expectedTimeout = GetExpectedTimeout(5);

            // the limitation with this way to handle a fallback is if we want to return a result we need to handle that result separately from the main method and the fallback method
            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .WithTransaction()
                .WithFallback(async () => result = await DoFallbackAsync())
                .Build();

            var watch = Stopwatch.StartNew();
            await resilientAsyncPolicy.ExecuteAsync(async () =>
            {
                result = await DoSomethingWithTransactionAsync(120000);
            });

            watch.Stop();
            var currentTime = TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).Seconds;

            Assert.AreEqual(true, result);
            Assert.AreEqual(true, _fallbackExecuted);
            Assert.AreEqual(expectedRetries, _retriesCounter);
            Assert.AreEqual(expectedTimeout.Seconds, currentTime);
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies.WithTransaction.WithFallback")]
        public async Task Should_Fallback_After_Sql_Exception()
        {
            var result = false;

            // the limitation with this way to handle a fallback is if we want to return a result we need to handle that result separately from the main method and the fallback method
            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .WithTransaction()
                .WithFallback(async () => result = await DoFallbackAsync())
                .Build();

            await resilientAsyncPolicy.ExecuteAsync(async () =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                throw GetRandomException();
            });

            Assert.AreEqual(true, result);
            Assert.AreEqual(true, _fallbackExecuted);
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies.WithTransaction.WithFallback")]
        public async Task Should_Fallback_After_Circuit_Breaker_Exception()
        {
            var result = false;
            var expectedRetries = 3;

            // the limitation with this way to handle a fallback is if we want to return a result we need to handle that result separately from the main method and the fallback method
            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .WithTransaction()
                .WithFallback(async () => result = await DoFallbackAsync())
                .Build();

            await resilientAsyncPolicy.ExecuteAsync(async () =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                ++_retriesCounter;
                throw TransientExceptions[1];
            });

            Assert.AreEqual(true, result);
            Assert.AreEqual(true, _fallbackExecuted);
            Assert.AreEqual(expectedRetries, _retriesCounter);
        }

        [Test]
        [Category("resilientAsyncPolicy.WithDefaultPolicies.WithTimeoutPerRetry")]
        public void Should_Interrupt_Retry_Due_To_Operation_Exceeds_Timeout_Per_Retry()
        {
            var expectedTime = TimeSpan.FromMilliseconds(300);

            var builder = new SqlPolicyBuilder();
            var resilientAsyncPolicy = builder
                .UseAsyncExecutor()
                .WithDefaultPolicies()
                .WithTimeoutPerRetry(TimeSpan.FromMilliseconds(300))
                .Build();

            var watch = new Stopwatch();
            var ex = Assert.ThrowsAsync<TimeoutRejectedException>(async () =>
            {
                await resilientAsyncPolicy.ExecuteAsync(async () =>
                {
                    watch.Start();
                    await Task.Delay(TimeSpan.FromMilliseconds(400));
                });
            });

            watch.Stop();
            Assert.IsInstanceOf<TimeoutRejectedException>(ex);
            Assert.IsTrue(watch.ElapsedMilliseconds >= expectedTime.Milliseconds && watch.ElapsedMilliseconds < 350);
        }

        [Test]
        [Category("resilientAsyncPolicy.UseAsyncExecutorWithSharedPolicies")]
        public async Task Should_Break_The_Circuit_Across_Requests_And_Fail_Faster_Then_Wait_For_The_Break_And_Try_Again_Then_Does_Not_Raise_An_Exception()
        {
            var builderFirstRequest = new SqlPolicyBuilder();
            var resilientAsyncPolicyFirstRequest = builderFirstRequest
                .UseAsyncExecutorWithSharedPolicies()
                .WithDefaultPolicies()
                .Build();

            var exFirstAttempt = Assert.ThrowsAsync<BrokenCircuitException>(async () =>
            {
                await resilientAsyncPolicyFirstRequest.ExecuteAsync(async () =>
                {
                    // will break the circuit since throws 3 times in a row the same exception
                    await DoSomethingToBreakTheCircuitAndContinueAsync();
                });
            });

            // simulates a different request
            var builderSecondRequest = new SqlPolicyBuilder();
            var resilientAsyncPolicySecondRequest = builderSecondRequest
                .UseAsyncExecutorWithSharedPolicies()
                .WithDefaultPolicies()
                .Build();

            var watch2 = Stopwatch.StartNew();
            var exSecondAttempt = Assert.ThrowsAsync<BrokenCircuitException>(async () =>
            {
                await resilientAsyncPolicySecondRequest.ExecuteAsync(async () =>
                {
                    // will fail immediately
                    await DoSomethingToBreakTheCircuitAndContinueAsync();
                });
            });
            watch2.Stop();

            // await the break time
            await Task.Delay(TimeSpan.FromSeconds(30));

            // simulates a different request
            var builderThirdRequest = new SqlPolicyBuilder();
            var resilientAsyncPolicyThirdRequest = builderThirdRequest
                .UseAsyncExecutorWithSharedPolicies()
                .WithDefaultPolicies()
                .Build();

            // will executes successfully because the circuit is half-open now
            var result = await resilientAsyncPolicyThirdRequest.ExecuteAsync(async () =>
            {
                await DoSomethingToBreakTheCircuitAndContinueAsync();
                return true;
            });

            var attemptTimeAfterCircuitBroke = TimeSpan.FromMilliseconds(watch2.ElapsedMilliseconds).Seconds;

            Assert.AreEqual(true, result);
            Assert.IsInstanceOf<BrokenCircuitException>(exFirstAttempt);
            Assert.IsInstanceOf<BrokenCircuitException>(exSecondAttempt);
            Assert.AreEqual(0, attemptTimeAfterCircuitBroke);
        }

        #endregion

        // http://blog.gauffin.org/2014/08/how-to-create-a-sqlexception/
        private static SqlException CreateSqlException(int number)
        {
            var collectionConstructor = typeof(SqlErrorCollection)
                .GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, //visibility
                    null, //binder
                    new Type[0],
                    null);

            var addMethod = typeof(SqlErrorCollection).GetMethod("Add", BindingFlags.NonPublic | BindingFlags.Instance);
            var errorCollection = (SqlErrorCollection)collectionConstructor.Invoke(null);
            var errorConstructor = typeof(SqlError).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null,
                new[]
                {
                    typeof (int), typeof (byte), typeof (byte), typeof (string), typeof(string), typeof (string),
                    typeof (int), typeof (uint), typeof(Exception)
                }, null);

            var error = errorConstructor.Invoke(new object[] { number, (byte)0, (byte)0, "server", "errMsg", "proccedure", 100, (uint)0, null });
            addMethod.Invoke(errorCollection, new[] { error });

            var constructor = typeof(SqlException)
                .GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, //visibility
                    null, //binder
                    new[] { typeof(string), typeof(SqlErrorCollection), typeof(Exception), typeof(Guid) },
                    null); //param modifiers
            return (SqlException)constructor.Invoke(new object[] { $"Error message: {number}", errorCollection, new DataException(), Guid.NewGuid() });
        }

        private TimeSpan GetExpectedDelay(int numberOfRetries)
        {
            var retry = 1;
            var delay = TimeSpan.Zero;
            while (retry <= numberOfRetries)
            {
                delay += TimeSpan.FromSeconds(Math.Pow(2, retry));
                retry++;
            }

            return delay;
        }

        private TimeSpan GetExpectedExecutionTime(int numberOfRetries)
        {
            return TimeSpan.FromMilliseconds(500 * numberOfRetries);
        }

        private static TimeSpan GetExpectedTimeout(int defaultRetries)
        {
            var retry = 1;
            var delay = TimeSpan.Zero;
            while (retry <= defaultRetries)
            {
                delay += TimeSpan.FromSeconds(Math.Pow(2, retry));
                retry++;
            }

            // plus an arbitrary max time the operation could take
            return delay + TimeSpan.FromSeconds(10);
        }
    }
}
