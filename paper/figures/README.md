Generated vector PDF figures for the paper.

These `.pdf` vector figures are produced by the benchmark suite and included in the
LaTeX source via `\includegraphics{figures/...}`.

## Regenerating figures

From the repo root:

```powershell
cd csharp
.\run_comparative_benchmarks.ps1 -PgfOutput ..\paper\figures
```

Or regenerate charts only (without re-running simulations):

```powershell
cd csharp
uv run scripts\plot_benchmarks.py --input artifacts\comparative --comparative --format all --pgf-output ..\paper\figures
```

Or use the paper build script:

```powershell
cd paper
.\build.ps1 figures
```
