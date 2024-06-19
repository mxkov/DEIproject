import io
import zipfile
import pandas as pd
import matplotlib.pyplot as plt
from math import floor


tn_data = "data/ECA_blend_tn.zip"
tx_data = "data/ECA_blend_tx.zip"

zip_file  = tx_data
plt_file  = "stations_tx.png"
startdate = "1980-01-01"
enddate   = "2022-12-31"
# VALID stations:
# TX: 2272
# TN: 2321

valid_files = 0
valid_stations = []

# in spark, just decompress the dir. 6 GB each.
zf = zipfile.ZipFile(zip_file)
files = zf.namelist()[4:]
print(len(files))
print(files[:10])
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
	valid_files += 1

print(f"\nValid stations: {valid_files}")

f = io.TextIOWrapper(zf.open("stations.txt"))
lines = f.read().split("\n")
lines = [lines[17]] + lines[19:]
sts = pd.read_csv(io.StringIO("\n".join(lines)))
sts.columns = [x.strip() for x in sts.columns]
sts = sts[sts.STAID.isin(valid_stations)]

def convert_latitude(x):
	x1 = x[1:].split(":")
	x1 = [int(val) for val in x1]
	res = x1[0] + x1[1]/60 + x1[2]/3600
	assert floor(res)==x1[0]
	return res

sts.LAT = sts.LAT.apply(convert_latitude)
plt.hist(sts.LAT, bins=30)
plt.xlabel("Latitude")
plt.ylabel("Stations")
plt.savefig(plt_file, bbox_inches="tight")
plt.show()
plt.close()
