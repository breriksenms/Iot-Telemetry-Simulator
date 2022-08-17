namespace IotTelemetrySimulator
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ApplicationInsights;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.Extensibility.Implementation;
    using Microsoft.Extensions.Logging;

    public class SimulatedDevice
    {
        private readonly ISender sender;
        private readonly int[] interval;
        private readonly RunnerConfiguration config;
        private readonly TelemetryClient telemetryClient;
        private readonly IRandomizer random = new DefaultRandomizer();

        public string DeviceID { get; private set; }

        public SimulatedDevice(string deviceId, RunnerConfiguration config, ISender sender, TelemetryClient telemetryClient)
        {
            this.DeviceID = deviceId;
            this.config = config;
            this.sender = sender;
            this.telemetryClient = telemetryClient;
            this.interval = config.GetMessageIntervalForDevice(deviceId);
        }

        public Task Start(RunnerStats stats, CancellationToken cancellationToken)
        {
            return Task.Run(() => this.RunnerAsync(stats, cancellationToken), cancellationToken);
        }

        async Task RunnerAsync(RunnerStats stats, CancellationToken cancellationToken)
        {
            try
            {
                await this.sender.OpenAsync();
                stats.IncrementDeviceConnected();

                // Delay first event by a random amount to avoid bursts
                int currentInterval = this.interval[0];

                await Task.Delay(this.random.Next(currentInterval), cancellationToken);

                var stopwatch = new Stopwatch();
                stopwatch.Start();
                long totalIntervalTime = 0;
                var messageCount = (ulong)this.config.MessageCount;
                for (ulong i = 0; !cancellationToken.IsCancellationRequested && (messageCount == 0 || i < messageCount); i++)
                {
                    if (i % 1000 == 0)
                    {
                        totalIntervalTime = 0;
                        stopwatch.Restart();
                    }

                    using (var op = this.telemetryClient.StartOperation<RequestTelemetry>("D2CMessage"))
                    {
                        op.Telemetry.Context.GlobalProperties["DeviceId"] = this.DeviceID;
                        await this.sender.SendMessageAsync(op.Telemetry.Context.Operation.Id, stats, cancellationToken);
                        this.telemetryClient.TrackEvent("MessageSent");
                    }

                    currentInterval = this.interval[i % (ulong)this.interval.Length];
                    totalIntervalTime += currentInterval;

                    var millisecondsDelay = Math.Max(0, totalIntervalTime - stopwatch.ElapsedMilliseconds);
                    await Task.Delay((int)millisecondsDelay, cancellationToken);
                }

                stats.IncrementCompletedDevice();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex);
            }
        }
    }
}
