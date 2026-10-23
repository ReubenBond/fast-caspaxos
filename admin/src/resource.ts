import { errorText } from './api';

export interface ResourceState<T> { value?: T; loading: boolean; stale: boolean; error?: string }

// Soft invalidations keep successful observations moving while a hard generation
// fence protects disconnects, resets, replacement, and disposal.
export class Resource<T> {
  state: ResourceState<T> = { loading: true, stale: true };
  private revision = 0;
  private generation = 0;
  private disposed = false;
  private pending = false;
  private controller?: AbortController;
  private timer?: ReturnType<typeof setTimeout>;

  constructor(private readonly load: (signal: AbortSignal) => Promise<T>, private readonly changed: () => void) {}

  markStale(): void {
    if (this.disposed) return;
    this.generation++;
    this.revision++;
    this.pending = false;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.controller?.abort();
    this.state = { ...this.state, loading: false, stale: true };
    this.changed();
  }

  reset(): void {
    this.markStale();
    this.invalidate();
  }

  invalidate(): void {
    if (this.disposed) return;
    this.revision++;
    this.pending = true;
    this.state = { ...this.state, stale: true };
    this.schedule();
    this.changed();
  }

  private schedule(): void {
    if (this.controller || this.timer || this.disposed) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.run();
    }, 300);
  }

  private async run(): Promise<void> {
    const revision = this.revision;
    const generation = this.generation;
    this.pending = false;
    const controller = this.controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(new Error('Read timed out. Refresh to try again.')), 15_000);
    this.state = { ...this.state, loading: true, error: undefined };
    this.changed();
    try {
      const value = await this.load(controller.signal);
      if (!this.disposed && generation === this.generation) {
        if (controller.signal.aborted) throw controller.signal.reason;
        this.state = { value, loading: false, stale: revision !== this.revision };
      }
    } catch (error) {
      if (!this.disposed && generation === this.generation) {
        this.state = { ...this.state, loading: false, stale: true, error: errorText(error) };
      }
    } finally {
      clearTimeout(timeout);
      this.controller = undefined;
      if (!this.disposed) {
        if (revision !== this.revision) this.state = { ...this.state, loading: false };
        this.changed();
        if (this.pending) this.schedule();
      }
    }
  }

  dispose(): void {
    this.disposed = true;
    clearTimeout(this.timer);
    this.controller?.abort();
  }
}
