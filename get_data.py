import os
import re
import pandas as pd
import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
# 1) ADJUST THIS PATH to point at your top‐level “NEON_struct-plant” folder:

# ─────────────────────────────────────────────────────────────────────────────

# 2) We'll walk the entire folder tree under BASE_DIR and collect:
#    - All “vst_mappingandtagging” CSV paths (there may be just one)
#    - All “vst_apparentindividual” CSV paths, keyed by their YYYY-MM tag
#    - All “vst_perplotperyear” CSV paths, keyed by their YYYY-MM tag

site_names = ['ABBY', 'BART', 'BLAN', 'DSNY', 'GUAN', 'HARV', 'JERC', 'LAJA', 'SCBI', 'SERC', 'TEAK', 'OSBS', 'STEI', 'TREE', 'UNDE', 'KONZ', 'UKFS', 'GRSM', 'MLBS', 'ORNL', 'DELA', 'LENO', 'TALL', 'DCFS', 'NOGP', 'WOOD', 'CPER', 'RMNP', 'CLBJ', 'YELL', 'MOAB', 'NIWO', 'JORN', 'SRER', 
'ONAQ', 'WREF', 'SJER', 'SOAP','BONA', 'DEJU', 'HEAL', 'PUUM']

for site_name in site_names:

    mapping_paths = []
    ai_paths: dict[str, str] = {}
    pp_paths: dict[str, str] = {}
    BASE_DIR = rf"/Users/yav13/Work/sdsc/SDSC-FIREREADY-BENCHMARK/NEON_{site_name}"

    # Helper regex to pull out “YYYY-MM” from filenames like “… .2015-08. …”
    month_re = re.compile(r"\.(\d{4}-\d{2})\.")

    for root, dirs, files in os.walk(BASE_DIR):
        for f in files:
            if not f.endswith(".csv"):
                continue

            full_path = os.path.join(root, f)

            if "vst_mappingandtagging" in f:
                # All mapping+tagging CSVs (we’ll concat/dedupe later)
                mapping_paths.append(full_path)

            elif "vst_apparentindividual" in f:
                m = month_re.search(f)
                if m:
                    ym = m.group(1)  # e.g. "2015-08"
                    ai_paths[ym] = full_path

            elif "vst_perplotperyear" in f:
                m = month_re.search(f)
                if m:
                    ym = m.group(1)
                    pp_paths[ym] = full_path

    # 3) Load and combine all “vst_mappingandtagging” CSVs (if there are multiple, keep latest by filename timestamp)
    if len(mapping_paths) == 0:
        raise RuntimeError("No vst_mappingandtagging CSV found under BASE_DIR!")

    # If there are multiple files, pick the one with highest lexicographic name (assuming latest timestamp)
    mapping_paths.sort()
    mapping_csv_path = mapping_paths[-1]
    df_mt = pd.read_csv(mapping_csv_path, low_memory=False)

    # 4) Prepare a list to collect each month’s merged DataFrame
    merged_monthly = []

    # 5) For each YYYY-MM where both AI and PP exist, merge:
    for ym in sorted(set(ai_paths.keys()) & set(pp_paths.keys())):
        ai_file = ai_paths[ym]
        pp_file = pp_paths[ym]

        print(f"Processing month {ym} ->")
        print(f"  • vst_apparentindividual: {ai_file}")
        print(f"  • vst_perplotperyear:     {pp_file}")

        # 5a) Load CSVs
        df_ai = pd.read_csv(ai_file, low_memory=False)
        df_pp = pd.read_csv(pp_file, low_memory=False)

        # 5b) Verify join keys
        #    ai needs at least ["eventID","plotID","individualID"]
        #    pp needs at least ["eventID","plotID"]
        #    mt needs at least ["individualID"]
        for col in ("eventID", "plotID", "individualID"):
            if col not in df_ai.columns:
                raise RuntimeError(f"vst_apparentindividual missing column '{col}' in file {ai_file}")
        for col in ("eventID", "plotID"):
            if col not in df_pp.columns:
                raise RuntimeError(f"vst_perplotperyear missing column '{col}' in file {pp_file}")
        if "individualID" not in df_mt.columns:
            raise RuntimeError(f"vst_mappingandtagging missing column 'individualID' in file {mapping_csv_path}")

        # 5c) Merge AI <- PP on ["eventID","plotID"]
        df_apppp = pd.merge(
            df_ai,
            df_pp,
            on=["eventID", "plotID"],
            how="left",
            suffixes=("_ai", "_pp"),
        )

        # 5d) Merge that result <- MT on ["individualID"]
        df_all = pd.merge(
            df_apppp,
            df_mt,
            on=["individualID"],
            how="left",
            suffixes=("", "_mt"),
        )

        # 5e) Extract exactly the columns requested + “filename”
        #    Columns from vst_apparentindividual:
        #       measurementHeight, stemDiameter, baseCrownHeight
        #    Columns from vst_perplotperyear:
        #       easting, northing, utmZone, decimalLatitude, decimalLongitude
        #    Columns from vst_mappingandtagging:
        #       stemAzimuth, stemDistance
        #    Add a “filename” column equal to the YYYY-MM string
        wanted_cols = [
            "plotID", 
            "individualID", 
            "growthForm", 
            "plantStatus", 
            "maxCrownDiameter",
            "scientificName", 
            "taxonID", 
            "genus", 
            "family",
            "filename",
            "height",
            "stemDiameter",
            "baseCrownHeight",
            "easting",
            "northing",
            "utmZone",
            "decimalLatitude",
            "decimalLongitude",
            "stemAzimuth",
            "stemDistance",
        ]

        df_all["filename"] = ym

        # Warn if any expected column is missing (it will appear as all NaN)
        for col in wanted_cols:
            if col not in df_all.columns:
                print(f"  ⚠ Column '{col}' not found after merging {ym}; filling with NaN.")

        df_subset = df_all.reindex(columns=wanted_cols)

        merged_monthly.append(df_subset)

    # 6) Concatenate all months into one big DataFrame
    if len(merged_monthly) == 0:
        raise RuntimeError("No matching month‐pairs found (vst_apparentindividual & vst_perplotperyear).")

    df_merged = pd.concat(merged_monthly, ignore_index=True)

    # 7) Write out the final merged CSV
    output_path = os.path.join(BASE_DIR, "NEON_all_months_merged.csv")
    df_merged.to_csv(output_path, index=False)
    print(f"\n✅ Wrote merged file to:\n    {output_path}")

    # Load the CSV file
    path_name = os.path.join(BASE_DIR, "NEON_all_months_merged.csv")
    df = pd.read_csv(path_name)

    df = df[[
            "plotID", 
            "individualID", 
            "growthForm", 
            "plantStatus", 
            "maxCrownDiameter",
            "scientificName", 
            "taxonID", 
            "genus", 
            "family",
            "filename",
            "height",
            "stemDiameter",
            "baseCrownHeight",
            "easting",
            "northing",
            "utmZone",
            "decimalLatitude",
            "decimalLongitude",
            "stemAzimuth",
            "stemDistance",
        ]]
    print(len(df))
    # Remove rows with any missing values
    # df_cleaned = df.dropna()
    df_cleaned = df.copy()
    print("Length of the clean dataset:", len(df_cleaned))
    # Save the cleaned data to a new CSV file
    file_name = f'NEON_{site_name}.csv'
    # df_cleaned.to_csv(file_name, index=False)

    df_cleaned['height_inches'] = df_cleaned['height'] * 0.393701
    df_cleaned['stemDiameter_inches'] = df_cleaned['stemDiameter'] * 0.393701
    df_cleaned['stemDistance_inches'] = df_cleaned['stemDistance'] * 39.3701

    # Convert azimuth (degrees) to radians
    df_cleaned['stemAzimuth_rad'] = np.radians(df_cleaned['stemAzimuth'])

    # Compute X and Y
    df_cleaned['X'] = df_cleaned['stemDistance'] * np.sin(df_cleaned['stemAzimuth_rad'])
    df_cleaned['Y'] = df_cleaned['stemDistance'] * np.cos(df_cleaned['stemAzimuth_rad'])

    # Optional: view result
    df_cleaned[['decimalLatitude', 'decimalLongitude', 'stemAzimuth', 'stemDistance', 'X', 'Y']].head()

    df_cleaned.to_csv(file_name, index=False)
