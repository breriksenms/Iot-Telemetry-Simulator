namespace IotTelemetrySimulator
{
    using System;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using CloudNative.CloudEvents;
    using Microsoft.ApplicationInsights;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Exceptions;

    internal class IotHubSender : SenderBase<Message>
    {
        const string ApplicationJsonContentType = "application/json";
        const string Utf8Encoding = "utf-8";
        private readonly TelemetryClient telemetryClient;
        private readonly string deviceId;
        private readonly JsonSerializerOptions jsonSerializerOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        private readonly DeviceClient deviceClient;

        public IotHubSender(DeviceClient deviceClient, string deviceId, RunnerConfiguration config, TelemetryClient telemetryClient)
            : base(deviceId, config)
        {
            this.deviceClient = deviceClient;
            this.telemetryClient = telemetryClient;
            this.deviceId = deviceId;
        }

        public override async Task OpenAsync()
        {
            await this.deviceClient.OpenAsync();
            await this.deviceClient.SetMethodDefaultHandlerAsync(
                (req, context) =>
            {
                try
                {
                    var ev = JsonSerializer.Deserialize<CloudEvent>(req.DataAsJson, this.jsonSerializerOptions);
                    using (var op = this.telemetryClient.StartOperation<RequestTelemetry>("DirectMethod", ev.Id))
                    {
                        return Task.FromResult(new MethodResponse(200));
                    }
                }
                catch (Exception)
                {
                    return Task.FromResult(new MethodResponse(500));
                }
            }, null);
            await this.deviceClient.SetReceiveMessageHandlerAsync(
                async (msg, context) =>
                {
                    try
                    {
                        var operationId = msg.MessageId ?? msg.CorrelationId;
                        if (string.IsNullOrWhiteSpace(operationId))
                        {
                            var ev = await JsonSerializer.DeserializeAsync<CloudEvent>(msg.BodyStream, this.jsonSerializerOptions);
                            operationId = ev.Id;
                        }

                        using (var op = this.telemetryClient.StartOperation<RequestTelemetry>("C2DMessage", operationId))
                        {
                            op.Telemetry.Context.GlobalProperties["DeviceId"] = this.deviceId;
                        }
                    }
                    catch (Exception)
                    {
                    }

                    await this.deviceClient.CompleteAsync(msg);
                }, null);
        }

        protected override async Task SendAsync(Message msg, CancellationToken cancellationToken)
        {
            await this.deviceClient.SendEventAsync(msg, cancellationToken);
        }

        protected override Message BuildMessage(byte[] messageBytes)
        {
            var msg = new Message(messageBytes)
            {
                CorrelationId = Guid.NewGuid().ToString(),
            };

            msg.ContentEncoding = Utf8Encoding;
            msg.ContentType = ApplicationJsonContentType;

            return msg;
        }

        protected override void SetMessageProperty(Message msg, string key, string value)
        {
            msg.Properties[key] = value;
        }

        protected override bool IsTransientException(Exception exception)
        {
            return exception is IotHubCommunicationException;
        }
    }
}
