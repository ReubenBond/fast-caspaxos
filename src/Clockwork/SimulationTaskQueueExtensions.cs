namespace Clockwork;

/// <summary>
/// Extension members for <see cref="SimulationTaskQueue"/> providing query functionality
/// over scheduled items.
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1034:Nested types should not be visible", Justification = "C# 14 extension members require this pattern")]
public static class SimulationTaskQueueExtensions
{
    extension(SimulationTaskQueue queue)
    {
        /// <summary>
        /// Gets all items of a specific type from the queue, passing each to an extractor function.
        /// </summary>
        /// <typeparam name="TItem">The type of scheduled item to find.</typeparam>
        /// <typeparam name="TResult">The type of result to extract from each item.</typeparam>
        /// <param name="extractor">A function to extract the result from each matching item.</param>
        /// <returns>An enumerable of extracted results.</returns>
        public IReadOnlyList<TResult> GetItemsOfType<TItem, TResult>(Func<TItem, TResult?> extractor)
            where TItem : ScheduledItem
        {
            ArgumentNullException.ThrowIfNull(extractor);

            var results = new List<TResult>();

            foreach (var item in queue.ScheduledItems)
            {
                if (item is TItem typedItem)
                {
                    var result = extractor(typedItem);
                    if (result is not null)
                    {
                        results.Add(result);
                    }
                }
            }

            return results;
        }

        /// <summary>
        /// Gets the count of ready items of a specific type (due time &lt;= current time).
        /// </summary>
        /// <typeparam name="T">The type of scheduled item to count.</typeparam>
        /// <returns>The count of ready items of the specified type.</returns>
        public int GetReadyCount<T>()
            where T : ScheduledItem
        {
            var count = 0;
            foreach (var item in queue.ScheduledItems)
            {
                if (item.DueTime > queue.UtcNow)
                    break; // Queue is sorted by due time, no more ready items
                if (item is T)
                {
                    count++;
                }
            }

            return count;
        }

        /// <summary>
        /// Gets the number of waiting items of a specific type in the queue (not yet due).
        /// </summary>
        /// <typeparam name="T">The type of scheduled item to count.</typeparam>
        /// <returns>The count of waiting items of the specified type.</returns>
        public int GetWaitingCount<T>()
            where T : ScheduledItem
        {
            var count = 0;
            foreach (var item in queue.ScheduledItems)
            {
                if (item.DueTime > queue.UtcNow && item is T)
                {
                    count++;
                }
            }

            return count;
        }

        /// <summary>
        /// Gets all waiting items of a specific type from the queue (not yet due).
        /// </summary>
        /// <typeparam name="T">The type of scheduled item to find.</typeparam>
        /// <returns>A list of waiting items of the specified type.</returns>
        public IReadOnlyList<T> GetWaitingItems<T>()
            where T : ScheduledItem
        {
            var results = new List<T>();

            foreach (var item in queue.ScheduledItems)
            {
                if (item.DueTime > queue.UtcNow && item is T typedItem)
                {
                    results.Add(typedItem);
                }
            }

            return results;
        }
    }
}
