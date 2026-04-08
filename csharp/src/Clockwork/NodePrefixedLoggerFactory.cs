using Microsoft.Extensions.Logging;

namespace Clockwork;

/// <summary>
/// A logger factory wrapper that prepends a node name to all log messages.
/// This enables identifying which node produced each log entry when multiple
/// nodes share the same underlying logger factory.
/// </summary>
public sealed class NodePrefixedLoggerFactory : ILoggerFactory
{
    private readonly ILoggerFactory _innerFactory;
    private readonly string _nodeName;

    /// <summary>
    /// Initializes a new instance of the <see cref="NodePrefixedLoggerFactory"/> class.
    /// Creates a new node-prefixed logger factory.
    /// </summary>
    /// <param name="innerFactory">The underlying logger factory.</param>
    /// <param name="nodeName">The node name to prepend to all log messages.</param>
    public NodePrefixedLoggerFactory(ILoggerFactory innerFactory, string nodeName)
    {
        ArgumentNullException.ThrowIfNull(innerFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeName);

        _innerFactory = innerFactory;
        _nodeName = nodeName;
    }

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName)
    {
        var innerLogger = _innerFactory.CreateLogger(categoryName);
        return new NodePrefixedLogger(innerLogger, _nodeName);
    }

    /// <inheritdoc />
    public void AddProvider(ILoggerProvider provider) => _innerFactory.AddProvider(provider);

    /// <inheritdoc />
    public void Dispose()
    {
        // Don't dispose the inner factory - it's shared across nodes
    }
}

/// <summary>
/// A logger wrapper that prepends a node name to all log messages.
/// </summary>
/// <remarks>
/// Creates a new node-prefixed logger.
/// </remarks>
/// <param name="innerLogger">The underlying logger.</param>
/// <param name="nodeName">The node name to prepend to all log messages.</param>
public sealed class NodePrefixedLogger(ILogger innerLogger, string nodeName) : ILogger
{
    private readonly ILogger _innerLogger = innerLogger;
    private readonly string _nodeName = nodeName;

    /// <inheritdoc />
    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull
        => _innerLogger.BeginScope(state);

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel) => _innerLogger.IsEnabled(logLevel);

    /// <inheritdoc />
    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;

        _innerLogger.Log(
            logLevel,
            eventId,
            state,
            exception,
            (s, e) => $"[{_nodeName}] {formatter(s, e)}");
    }
}
