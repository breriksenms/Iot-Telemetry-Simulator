namespace IotTelemetrySimulator
{
    using System.Threading;
    using System.Threading.Tasks;

    public interface ISender
    {
        Task OpenAsync();

        Task SendMessageAsync(string messageId, RunnerStats stats, CancellationToken cancellationToken);
    }
}
