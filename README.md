# Are Newer Library Versions Greener?
# An Empirical Study on Energy Patterns of Data Processing Libraries

This repository contains the full experimental infrastructure, raw measurements, and analysis pipeline for an empirical study on the energy consumption and execution time of four Python dataframe libraries — **Pandas**, **Polars**, **Dask**, and **Vaex** — across multiple versions, six benchmark datasets, and 19 dataframe operations spanning I/O, missing-data handling, row/column manipulation, and statistical aggregation.

Energy is measured at the hardware level using **PyJoules** (Intel RAPL), and results are validated with a non-parametric statistical pipeline (Shapiro–Wilk, Friedman, Wilcoxon signed-rank, Mann–Whitney U, Cliff's Delta).

---

## Repository Structure

```
.
├── Code/                              # Benchmark scripts, analysis helper, notebook
│   ├── measure_<dataset>_<library>.py     # Energy measurement scripts (one per dataset × library);
│   │                                       # system prep + itr(30) loop are self-contained in each script
│   ├── clear_cache_util.py                # Filesystem/CPU-cache clearing utility, called before every run
│   ├── compute_p.py                       # Wilcoxon rank-sum p-value computation between variants
│   └── summary_generator_green_energy.ipynb   # Full statistical pipeline + all paper figures/tables
│
├── datasets/                           # Benchmark datasets (CSV + JSON) and prep utilities
│   ├── adult.csv / adult.json
│   ├── bank.csv / bank.json
│   ├── drugs.csv / drugs.json
│   ├── exam_score.csv / exam_score.json
│   ├── water_potability.csv / water_potability.json
│   ├── csv_to_json.py, importCsv.py, jsonToNdjson.py,   # Dataset conversion / preprocessing helpers
│   │   json_count.py, jsoncompress.py
│   └── (USCensus1990.csv / USCensus1990.json — see note below)
│
└── Results/                             # All raw measurements, statistical outputs, and figures
    ├── Pandas/Pandas_v<version>/Pandas_v<version>_itr(10|20|30)/*.csv
    ├── Polars/Polars_v<version>/...
    ├── Dask/Dask_v<version>/...
    ├── Vaex/Vaex_v<version>/...
    ├── summary_results/summary_<Library>/         # Per-tag mean/median/std summaries
    ├── Statistical_Analysis_Results/               # Shapiro, Friedman, Wilcoxon, Cliff's Delta workbooks
    ├── Output_tables/                              # Consolidated CSV/LaTeX tables used in the paper
    ├── Variance_results/                           # Per-function variance / IQR-filtered variance summaries
    └── figures/                                    # All generated PNG/PDF figures
```

Library versions and their raw-data folder names:

| Library | Versions covered |
|---|---|
| Pandas | `v1.0.0`, `v2.0.0`, `v2.2.3` |
| Polars | `v0.20.0`, `v1.0.0`, `v1.31.0` |
| Dask   | `v2022.1.0`, `v2024.8.0`, `v2025.1.0` |
| Vaex   | `v4.13.0`, `v4.15.0`, `v4.17.0` |

Each version was benchmarked at three independent iteration settings — `itr(10)`, `itr(20)`, `itr(30)` — treated as separate measurement campaigns.

---

## US Census 1990 Dataset

> **Due to GitHub's file size limitations, the US Census 1990 dataset files are not included in this repository.**
>
> Download the dataset from the [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/116/us+census+data+1990), and place the prepared files inside the `datasets/` directory as:
>
> ```
> datasets/USCensus1990.csv
> datasets/USCensus1990.json
> ```
>
> These exact filenames are hard-coded into `measure_census_pandas.py`, `measure_census_dask.py`, `measure_census_polars.py`, and `measure_census_vaex.py`. The `datasets/importCsv.py` and `datasets/import json.py` scripts can be used to subsample the full census file down to the row count used in the study (1,700,000 rows) before conversion. `datasets/csv_to_json.py` converts a prepared CSV into the matching JSON file.
>
> All other datasets (Adult, Bank Marketing, Drug Review, Student Exam Scores, Water Potability) are already included in `datasets/`.

---

## Reproducing the Experiments

### 1. Hardware and OS requirements

Energy measurement relies on **Intel RAPL** counters via PyJoules, so this must be run on:

- An **Intel CPU** with RAPL support (`/sys/devices/system/cpu/cpu*/cpufreq` and `/sys/devices/system/cpu/intel_pstate` present).
- **Linux** (the original study used Ubuntu 22.04.5 LTS). RAPL access via PyJoules is not available on Windows/macOS or on AMD/ARM processors.
- `sudo` privileges — cache clearing, CPU governor changes, and Turbo Boost toggling all require root.

### 2. Software environment

There is one measurement script per `(dataset, library)` pair, and the library **version** is pinned via whichever environment that script is executed in — the version string is baked into each script's output filename (e.g. `pandas_adult_v1.0.0_itr(30).csv`). To reproduce a specific library version, create an isolated environment per version and install accordingly, e.g.:

```bash
python3 -m venv venv-pandas-1.0.0
source venv-pandas-1.0.0/bin/activate
pip install pandas==1.0.0 pyJoules
```

Repeat for each version of each library you want to reproduce (`pandas==2.0.0`, `pandas==2.2.3`, `polars==0.20.0`, `polars==1.0.0`, `polars==1.31.0`, `dask[dataframe]==2022.1.0`, `dask[dataframe]==2024.8.0`, `dask[dataframe]==2025.1.0`, `vaex==4.13.0`, `vaex==4.15.0`, `vaex==4.17.0`). Note the Python interpreter constraints used in the study: Pandas v1.0.0 requires Python 3.7–3.8; all other library versions were run on Python 3.10.

Core packages needed in every environment:

```bash
pip install pyJoules pandas
```

plus the target library (`polars`, `dask[dataframe]`, or `vaex`) at the pinned version. No `requirements.txt` is bundled — dependencies must be installed per version as above.

### 3. System preparation (per run)

There are no separate driver shell scripts in this repository — each `measure_<dataset>_<library>.py` script calls `clear_cache_util.py`'s `clear_caches()` itself, both once at the start of the run and again before every individual benchmark operation. `clear_caches()` performs the same system-isolation steps used in the paper:

```bash
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches         # drop filesystem caches
echo performance | sudo tee .../scaling_governor    # fix CPU governor
echo 1 | sudo tee .../intel_pstate/no_turbo         # disable Turbo Boost
```

Because `clear_caches()` shells out to `sudo`, the measurement script itself must be invoked with `sudo` (see below). For faithful reproduction, also disable networking, background services, and OS notifications, and keep ambient temperature stable, as described in the paper's methodology.

### 4. Running a benchmark

From inside `Code/`, with the appropriate versioned environment activated:

```bash
cd Code
sudo python measure_adult_pandas.py
```

This runs all 19 operations (I/O, missing-data, row/column, aggregation) on the Adult dataset, sleeping 30 seconds between operations for thermal stabilization, and writes a PyJoules CSV such as `pandas_adult_v1.0.0_itr(30).csv` to the current directory. Every measurement script's iteration loop is currently fixed at `for i in range(30)` (i.e. `itr(30)`); to reproduce the `itr(10)`/`itr(20)` campaigns, edit the loop count and the `itr(N)` portion of the `CSVHandler(...)` filename near the top of the script before rerunning.

There is one script per `(dataset, library)` pair and no batch/driver script to chain them — run each dataset's script for a given library individually:

```bash
cd Code
sudo python measure_adult_pandas.py
sudo python measure_bank_pandas.py
sudo python measure_drug_pandas.py
sudo python measure_exam_pandas.py
sudo python measure_water_pandas.py
```

Repeat with `_dask`, `_polars`, or `_vaex` in place of `_pandas` for the other libraries (in the appropriate versioned environment).

The **US Census 1990** dataset is handled by its own script per library (`measure_census_pandas.py`, `measure_census_dask.py`, `measure_census_polars.py`, `measure_census_vaex.py`), invoked the same way:

```bash
cd Code
sudo python measure_census_dask.py
```

Each census script currently writes to a single fixed output filename rather than incrementing a run id automatically — to reproduce multiple runs, edit the `CSVHandler(...)` filename (or the commented-out `run_id`-based variant near the top of the script) between invocations so outputs don't overwrite each other.

Before switching to the next library version, edit the `CSVHandler(...)` filename string near the top of the script (or install the new version into a fresh venv and rerun) so output files don't overwrite each other, then repeat the same protocol for `itr(10)`, `itr(20)`, and `itr(30)` by adjusting the iteration loop count in the script.

Each run produces a semicolon-delimited CSV with columns:

```
timestamp;tag;duration;package_0;dram_0;core_0;uncore_0
```

where `tag` identifies the operation (`load_csv`, `groupby`, `mean`, …), `duration` is execution time in seconds, and `package_0`/`dram_0` are CPU package and DRAM energy in microjoules.

### 5. Summarizing and analyzing results

All summarization, statistical testing, and figure/table generation for this study have already been completed. Every figure and table reported in the paper was produced from `Code/summary_generator_green_energy.ipynb`, which contains the full pipeline: IQR/MAD-based outlier filtering, Shapiro–Wilk normality testing, the Friedman omnibus test, Wilcoxon signed-rank pairwise tests with Mann–Whitney U fallback, Cliff's Delta effect sizes, and Spearman energy–time correlation.

The outputs of this pipeline are already included in this repository and do not need to be regenerated:

- `Results/summary_results/summary_<Library>/` — per-tag mean/median/std summaries
- `Results/Statistical_Analysis_Results/` — Shapiro, Friedman, Wilcoxon, and Cliff's Delta result workbooks
- `Results/Output_tables/` — consolidated CSV/LaTeX tables (`table_I`–`table_IV`, per-`itr(N)`, plus `combined_results*.csv`)
- `Results/Variance_results/` — per-function duration/energy variance and IQR-filtered variance/outlier summaries
- `Results/figures/` — every generated PNG/PDF figure (`fig01`–`fig24`, `figure2`–`figure6`, density and exec-vs-energy plots, etc.)

If you collect new raw measurements (Step 4) and want to extend or rerun the analysis, place the new CSVs into `Results/<Library>/<Library>_v<version>/<Library>_v<version>_itr(<N>)/`, matching the existing folder convention, then rerun the relevant cells of `Code/summary_generator_green_energy.ipynb` against that folder, pointing the `ROOT_DIR`/`OUTPUT_DIR` variables at the top of each cell to your local `Results/` directory.

`Code/compute_p.py` (pairwise Wilcoxon rank-sum p-values) is a lighter-weight standalone script kept in the repo for ad-hoc checks on individual CSVs, but it is not required to reproduce the paper's results — the notebook supersedes it.

### 6. Required Python packages for analysis

The analysis scripts and notebook (separate from the measurement environments above) need:

```bash
pip install pandas numpy scipy matplotlib seaborn openpyxl
```

---

## Benchmark Operations

| Category | Operations |
|---|---|
| I/O | `load_csv`, `load_json`, `save_csv`, `save_json` |
| Missing-data | `isna`, `dropna`, `fillna`, `replace` |
| Row/Column | `drop`, `groupby`, `merge`, `sort`, `concat` (`concat_dataframes`) |
| Aggregation | `count`, `sum`, `mean`, `min`, `max`, `unique` |

## Datasets

| Dataset | Rows | Size class | Source |
|---|---|---|---|
| Water Potability | 3,276 | Small | Kaggle |
| Student Exam Scores | 30,641 | Small | Kaggle |
| Adult | 48,842 | Small | UCI |
| Bank Marketing | 45,211 | Medium | Kaggle |
| Drug Review | 161,297 | Medium | UCI |
| US Census 1990 | 2,458,285 | Large | UCI (not bundled — see above) |

## Citation

If you use this repository, please cite the accompanying paper, *"Are Newer Library Versions Greener? An Empirical Study on Energy Patterns of Data Processing Libraries."*
