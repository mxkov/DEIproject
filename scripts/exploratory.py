"""Some exploratory data analysis & data filtering.

Reads:
data/ECA_blend_tn.zip
data/ECA_blend_tx.zip

Writes:
exploratory/station_data.csv
exploratory/stations.png
exploratory/stations_common.png

- Finds stations that have the data between 1980-01-01 and 2024-05-31
  (accounting for missing data).
- Converts latitude & longitude from degrees:minutes:seconds to degrees.
- Builds a latitude distribution of selected stations for TN and TX data
  separately, saves to exploratory/stations.png.
- Selects the stations that fulfill this condition for BOTH TN and TX,
  writes them to exploratory/station_data.csv.
- Builds a latitude distribution of this final selection of stations,
  saves to exploratory/stations_common.png.
"""

import io
import os
import zipfile
import pandas as pd
import matplotlib.pyplot as plt

from common import (
	read_datafile, convert_coord,
	DATA_DIR, EXPL_DIR,
	DATE_START, DATE_END
)


valid_stations = {}
valid_files = {}
station_data = {}
os.makedirs(EXPL_DIR, exist_ok=True)

fig, ax = plt.subplots(ncols=2, sharex=True, sharey=True, figsize=(10, 4))
plt_title = {"tn": "Daily minimum temperature",
             "tx": "Daily maximum temperature"}
plt_file = os.path.join(EXPL_DIR, "stations.png")

for j, data_id in enumerate(("tn", "tx")):
	print(f"\nProcessing: {data_id}")

	zip_file  = os.path.join(DATA_DIR, f"ECA_blend_{data_id}.zip")

	valid_stations[data_id] = []
	valid_files[data_id] = []

	# in spark, just decompress the dir. 6 GB each.
	zf = zipfile.ZipFile(zip_file)
	files = zf.namelist()[4:]
	print(f"Total files: {len(files)}")

	for i,file in enumerate(files):
		print(f"Processing file {i+1}", end="\r")
		data = read_datafile(file, zf)
		data = data[data.DATE >= DATE_START]
		start, end = data.DATE.min(), data.DATE.max()
		if pd.isnull(start) or pd.isnull(end):
			continue
		if ( end < pd.to_datetime(DATE_END)
		  or start > pd.to_datetime(DATE_START)):
			continue
		station = data.STAID.unique()
		assert len(station)==1, f"{file}: {station}"
		valid_stations[data_id].append(station[0])
		valid_files[data_id].append(file)

	print(f"\nValid stations: {len(valid_stations[data_id])}")

	f = io.TextIOWrapper(zf.open("stations.txt"))
	lines = f.read().split("\n")
	lines = [lines[17]] + lines[19:]
	sts = pd.read_csv(io.StringIO("\n".join(lines)))
	sts.columns = [x.strip() for x in sts.columns]
	sts = sts[sts.STAID.isin(valid_stations[data_id])]
	for col in ["STANAME", "CN"]:
		sts[col] = sts[col].apply(lambda x: x.strip())
	for col in ["LAT", "LON"]:
		sts[col] = sts[col].apply(convert_coord).astype("float")
	sts = sts.round(6)
	sts.HGHT = sts.HGHT.astype("int")
	station_data[data_id] = sts.copy(deep=True)

	ax[j].hist(sts.LAT, bins=25)
	ax[j].set_xlabel("Latitude, degrees")
	ax[j].set_ylabel("Number of stations")
	ax[j].grid()
	ax[j].set_title(plt_title[data_id])

	zf.close()

ax[1].tick_params(axis="y", left=True, labelleft=True)
plt.savefig(plt_file, dpi=300, bbox_inches="tight")
plt.close()
print(f"\nHistogram saved to {plt_file}")


valid_stations_common = set(valid_stations["tn"]) & set(valid_stations["tx"])
valid_stations_common = sorted(list(valid_stations_common))
print(f"\nCommon valid stations: {len(valid_stations_common)}")

for data_id in ("tn", "tx"):
	station_data[data_id] = station_data[data_id][
	    station_data[data_id].STAID.isin(valid_stations_common)]
	station_data[data_id] = station_data[data_id].reset_index(drop=True)
assert station_data["tn"].equals(station_data["tx"])
sts_file = os.path.join(EXPL_DIR, f"station_data.csv")
station_data["tn"].to_csv(sts_file, index=False)
print(f"Station info written to {sts_file}")

fig, ax = plt.subplots(figsize=(5, 4))
ax.hist(station_data["tn"].LAT, bins=25)
ax.set_xlabel("Latitude, degrees")
ax.set_ylabel("Number of stations")
ax.grid()
plt_file2 = os.path.join(EXPL_DIR, "stations_common.png")
plt.savefig(plt_file2, dpi=300, bbox_inches="tight")
plt.close()
print(f"Histogram saved to {plt_file2}")
