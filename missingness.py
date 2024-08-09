import matplotlib.pyplot as plt
import os
import pandas as pd
import zipfile

from common import read_datafile, DATE_START, DATE_END


DATE_START = "1980-01-01"
DATE_END   = "2024-05-31"


stations = pd.read_csv("exploratory/station_data.txt", usecols=[0])
stations = list(stations.STAID)

percent_missing = {}
fig, ax = plt.subplots(ncols=2, sharex=True, sharey=True, figsize=(10, 4))
plt_title = {"tn": "Daily minimum temperature",
             "tx": "Daily maximum temperature"}
plt_file = os.path.join("exploratory/missingness.png")

for j, data_id in enumerate(("tn", "tx")):
	print(f"\nProcessing: {data_id}")

	zip_file  = os.path.join("data", f"ECA_blend_{data_id}.zip")
	zf = zipfile.ZipFile(zip_file)
	files = zf.namelist()[4:]
	prefix = data_id.upper()+"_STAID"

	percent_missing[data_id] = []

	for i,file in enumerate(files):
		staid = int(file.replace(prefix, "").replace(".txt", ""))
		if staid not in stations:
			continue

		print(f"Processing file {i+1}", end="\r")
		data = read_datafile(file, zf, remove_missing=False, remove_suspect=False)

		data = data[data.DATE >= DATE_START]
		data = data[data.DATE <= DATE_END]

		pc_miss = sum(data.iloc[:,-1] != 0) / data.shape[0]
		percent_missing[data_id].append(pc_miss*100)

	ax[j].hist(percent_missing[data_id], bins=25, color="salmon")
	ax[j].set_xlabel("Missing or suspect data, %")
	ax[j].set_ylabel("Number of stations")
	ax[j].grid()
	ax[j].set_title(plt_title[data_id])

	zf.close()

ax[1].tick_params(axis="y", left=True, labelleft=True)
plt.savefig(plt_file, dpi=300, bbox_inches="tight")
plt.close()
print(f"\n\nHistogram saved to {plt_file}")
