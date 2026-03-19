namespace Clockwork;

/// <summary>
/// A simple scheduled item that wraps an Action callback.
/// Used for general-purpose delayed execution.
/// </summary>
public sealed class ScheduledActionItem(Action callback) : ScheduledItem
{
    /// <inheritdoc />
    protected internal override void Invoke() => callback();
}
