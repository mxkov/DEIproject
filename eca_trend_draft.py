import io
import zipfile
import pandas as pd


tn_data = "data/ECA_blend_tn.zip"
tx_data = "data/ECA_blend_tx.zip"

zip_file  = tx_data
plt_file  = "stations_tx.png"
startdate = "1980-01-01"
enddate   = "2022-12-31"

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
	data = pd.read_csv(io.StringIO("\n".join(lines)))
	data.columns = [x.strip() for x in data.columns]

	data.DATE = pd.to_datetime(data.DATE, format="%Y%m%d")
	data = data[data.DATE >= startdate]
	start, end = data.DATE.min(), data.DATE.max()
	if ( end < pd.to_datetime(enddate)
	  or start > pd.to_datetime(startdate)):
		continue
	station = data.STAID.unique()
	valid_stations.append(station[0])
	valid_files += 1

print(f"\nValid stations: {valid_files}")
