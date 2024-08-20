# Data Engineering I: course project

This repository contains code and results for the Data Engineering I course project, spring 2024.


## Data
The dataset used here comes from [European Climate Assessment](https://www.ecad.eu/) and contains blended daily minimum (TN) and maximum (TX) temperature series available [here](https://www.ecad.eu/dailydata/predefinedseries.php). Retrieved on 2024-07-13.

To reproduce the analysis, the archives need to be placed in a `data` subdirectory in the repo root and named `ECA_blend_tn.zip` and `ECA_blend_tx.zip`.


## Preprocessing and analysis
Each of the two archives contains metadata files and roughly 7400 temperature series files, one per meteorological station. All files are plain text.

Here are the main steps:
1. Filter the data files to find stations that have foth TN and TX data between 1980-01-01 and 2024-05-31. This way 2064 stations were selected. Done by [scripts/exploratory.py](/scripts/exploratory.py).
2. Extract selected station data with [scripts/extract.py](/scripts/extract.py).
3. Run the analysis itself in [eca_trend.ipynb](/eca_trend.ipynb).
4. Collect performance metrics (see [results/times.csv](/results/times.csv)), plot with [scripts/plot-times.py](/scripts/plot-times.py) (possibly adjusting parameters in the nearby JSON file).


## Environment
Station selection in [Step 1](#preprocessing-and-analysis) was done on a standalone machine. Files extracted in [Step 2](#preprocessing-and-analysis) were placed on an HDFS file system, and the analysis in [Step 3](#preprocessing-and-analysis) was run in Apache Spark deployed in standalone mode.

### Software used
* OpenJDK 8
* Apache Hadoop 3.3.6
* Apache Spark 3.5.1
* JupyterLab 4.2.4
* Python 3.8.10 with modules:
	* pyspark 3.5.1
	* pandas 2.0.3
	* numpy 1.24.4
	* matplotlib 3.7.5
