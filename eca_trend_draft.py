import io
import zipfile
import pandas as pd
import matplotlib.pyplot as plt
from math import floor


def convert_latitude(x):
	"""Convert latitude from degrees:minutes:seconds to degrees with decimals"""
	x1 = x[1:].split(":")
	x1 = [int(val) for val in x1]
	res = x1[0] + x1[1]/60 + x1[2]/3600
	assert floor(res)==x1[0]
	return res


# VALID stations:
# TX: 2272
# TN: 2321

startdate = "1980-01-01"
enddate   = "2022-12-31"

for data_id in ("tn", "tx"):
	print(f"\nProcessing: {data_id}")

	zip_file  = f"data/ECA_blend_{data_id}.zip"
	fls_file  = f"filelist_{data_id}.txt"
	plt_file  = f"stations_{data_id}.png"

	valid_stations = []
	valid_files_list = []

	# in spark, just decompress the dir. 6 GB each.
	zf = zipfile.ZipFile(zip_file)
	files = zf.namelist()[4:]
	print(f"Total files: {len(files)}")

	for i,file in enumerate(files):
		print(f"Processing file {i}", end="\r")
		f = io.TextIOWrapper(zf.open(file))
		lines = f.read().split("\n")[20:]
		assert lines[0].startswith("STAID, SOUID,    DATE,")
		data = pd.read_csv(io.StringIO("\n".join(lines)))
		data.columns = [x.strip() for x in data.columns]
	
		data.DATE = pd.to_datetime(data.DATE, format="%Y%m%d")
		data = data[data.DATE >= startdate]
		data = data[data.iloc[:,-1] != 9]
		start, end = data.DATE.min(), data.DATE.max()
		if pd.isnull(start) or pd.isnull(end):
			continue
		if ( end < pd.to_datetime(enddate)
		  or start > pd.to_datetime(startdate)):
			continue
		station = data.STAID.unique()
		assert len(station)==1, f"{file}: {station}"
		valid_stations.append(station[0])
		valid_files_list.append(file)

	print(f"\nValid stations: {len(valid_files_list)}")

	f = open(fls_file, "w")
	f.write("\n".join(valid_files_list))
	f.close()
	print(f"List of valid files saved to {fls_file}")

	f = io.TextIOWrapper(zf.open("stations.txt"))
	lines = f.read().split("\n")
	lines = [lines[17]] + lines[19:]
	sts = pd.read_csv(io.StringIO("\n".join(lines)))
	sts.columns = [x.strip() for x in sts.columns]
	sts = sts[sts.STAID.isin(valid_stations)]

	sts.LAT = sts.LAT.apply(convert_latitude)
	plt.hist(sts.LAT, bins=30)
	plt.xlabel("Latitude")
	plt.ylabel("Stations")
	plt.grid()
	plt.savefig(plt_file, bbox_inches="tight")
	plt.close()
	print(f"Histogram saved to {plt_file}")
