import os, sys
import zipfile


if len(sys.argv) < 2:
	sys.exit("Missing required argument: extraction path")
extract_root = sys.argv[1]
os.makedirs(extract_root, exist_ok=True)

f = open("exploratory/stations_valid.txt")
stations = f.read().strip().split("\n")
f.close()
stations = [int(x) for x in stations]

for data_id in ("tn", "tx"):
	print(f"\nExtracting {data_id}")
	filecount = 0
	extract_path = os.path.join(extract_root, data_id)
	os.makedirs(extract_path, exist_ok=True)

	zip_file  = os.path.join("data", f"ECA_blend_{data_id}.zip")
	zf = zipfile.ZipFile(zip_file)
	zf.extract("stations.txt", path=extract_path)
	files = zf.namelist()[4:]
	prefix = data_id.upper()+"_STAID"

	for file in files:
		staid = int(file.replace(prefix, "").replace(".txt", ""))
		if staid not in stations:
			continue
		zf.extract(file, path=extract_path)
		filecount += 1

	zf.close()
	print(f"Successfully extracted {filecount} files")
