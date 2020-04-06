﻿using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace IotTelemetrySimulator.Test
{
    public class SenderBaseTest
    {
        [Fact]
        public async Task When_SendMessageAsync_SendAsync_Succeeds_MessagesSent_Is_Incremented()
        {
            const string deviceId = "device1";
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>())
                .Build();

            var stats = new RunnerStats();
            var config = RunnerConfiguration.Load(configuration, NullLogger.Instance);
            var sender = new TestSender(false, false, deviceId, config);

            await sender.SendMessageAsync(stats, CancellationToken.None);

            Assert.Equal(1, stats.MessagesSent);
            Assert.Equal(0, stats.TotalSendTelemetryErrors);
            Assert.Single(sender.TestMessages);
        }

        [Fact]
        public async Task When_SendMessageAsync_SendAsync_Throws_TransientExcption_SendTelemetryErrors_Is_Incremented()
        {
            const string deviceId = "device1";
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>())
                .Build();

            var stats = new RunnerStats();
            var config = RunnerConfiguration.Load(configuration, NullLogger.Instance);
            var sender = new TestSender(true, false, deviceId, config);

            var stopwatch = Stopwatch.StartNew();
            await sender.SendMessageAsync(stats, CancellationToken.None);
            stopwatch.Stop();

            Assert.InRange(stopwatch.ElapsedMilliseconds, 0, sender.TransientErrorWaitTime);
            Assert.Equal(0, stats.MessagesSent);
            Assert.True(1 < stats.TotalSendTelemetryErrors);
            Assert.Empty(sender.TestMessages);
        }

        [Fact]
        public async Task When_SendMessageAsync_SendAsync_Throws_NonTransientExcption_SendTelemetryErrors_Is_Incremented()
        {
            const string deviceId = "device1";
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>())
                .Build();

            var stats = new RunnerStats();
            var config = RunnerConfiguration.Load(configuration, NullLogger.Instance);
            var sender = new TestSender(false, true, deviceId, config);

            var stopwatch = Stopwatch.StartNew();
            await sender.SendMessageAsync(stats, CancellationToken.None);
            stopwatch.Stop();

            Assert.InRange(stopwatch.ElapsedMilliseconds, sender.TransientErrorWaitTime, (sender.MaxNumberOfSendAttempts + 1) * sender.TransientErrorWaitTime);
            Assert.Equal(0, stats.MessagesSent);
            Assert.InRange(stats.TotalSendTelemetryErrors, 1, int.MaxValue);
            Assert.Empty(sender.TestMessages);
        }

        [Fact]
        public async Task When_SendMessageAsync_With_Header_Property_Is_Added()
        {
            const string deviceId = "device1";
            const string propertyKey = "myPropertyKey";
            const string propertyValue = "My Propert Value";
            var rawTemplate = $"{{ \"{propertyKey}\": \"{propertyValue}\" }}";
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>()
                {
                    { "Header", rawTemplate}
                })
                .Build();

            var stats = new RunnerStats();
            var config = RunnerConfiguration.Load(configuration, NullLogger.Instance);
            var sender = new TestSender(false, false, deviceId, config);

            await sender.SendMessageAsync(stats, CancellationToken.None);

            Assert.Equal(propertyValue, sender.TestMessages.Single().Properties[propertyKey]);
            Assert.Equal(1, stats.MessagesSent);
            Assert.Equal(0, stats.TotalSendTelemetryErrors);
            Assert.Single(sender.TestMessages);
        }


        class TestMessage
        {
            public IDictionary<string, object> Properties { get; private set; } = new Dictionary<string, object>();
            public byte[] MessageBytes { get; }

            public TestMessage(byte[] messageBytes)
            {
                MessageBytes = messageBytes;
            }
        }

        class TestException : Exception
        {
        }

        class TestSender : SenderBase<TestMessage>
        {
            private readonly bool throwTransientException;
            private readonly bool throwNonTransientException;

            public int TransientErrorWaitTime => WaitTimeOnTransientError;
            public int MaxNumberOfSendAttempts => MaxSendAttempts;

            public ConcurrentBag<TestMessage> TestMessages { get; private set; } = new ConcurrentBag<TestMessage>();

            public TestSender(bool throwTransientException, bool throwNonTransientException, string deviceId, RunnerConfiguration config) : base(deviceId, config)
            {
                this.throwTransientException = throwTransientException;
                this.throwNonTransientException = throwNonTransientException;
            }

            public override Task OpenAsync()
            {
                return Task.CompletedTask;
            }

            protected override TestMessage BuildMessage(byte[] messageBytes)
            {
                return new TestMessage(messageBytes);
            }

            protected override bool IsTransientException(Exception exception)
            {
                return exception is TestException;
            }

            protected override Task SendAsync(TestMessage msg, CancellationToken cancellationToken)
            {
                if (throwTransientException)
                {
                    throw new TestException();
                }

                if (throwNonTransientException)
                {
                    throw new InvalidOperationException();
                }

                TestMessages.Add(msg);
                return Task.CompletedTask;
            }

            protected override void SetMessageProperty(TestMessage msg, string key, string value)
            {
                msg.Properties[key] = value;
            }
        }
    }
}
