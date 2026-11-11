namespace FastCASPaxos.Server;

/// <summary>Receives a failed local storage operation, after which the process stops.</summary>
/// <remarks>
/// After a local flush fails, the process cannot know what the device holds, so only a fresh process recovering from
/// the device answers again. A component reports the failure here and answers nothing afterward. The host's handler
/// ends the process; a test's handler records the stop.
/// </remarks>
public interface IFatalErrorHandler
{
    /// <summary>Reports that <paramref name="operation"/> on <paramref name="path"/> failed with <paramref name="exception"/>.</summary>
    void OnLocalStorageFailure(string operation, string path, Exception exception);
}
