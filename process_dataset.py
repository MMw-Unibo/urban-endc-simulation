import os
import re
import glob
import sys
import datetime
import shutil

from PyInquirer import prompt
import pandas as pd

REGEX_HAS_CORRECT_GROUP = "\(\[0-9\][+*]?\)"

INITIAL_QUESTIONS = [
    {
        "name": "glob_expr",
        "type": "input",
        "message": "Enter the glob expression to get the data files",
        "default": os.path.join("DataSource", "DumpedData*.csv"),
        "validate": lambda val: len(glob.glob(val)) > 0
    },
    {
        "name": "regex",
        "type": "input",
        "message": "Enter a regex with a single capture group for an integer to identify the test number",
        "default": "DumpedData_Test([0-9]+).csv",
        "validate": lambda val: len(re.findall(REGEX_HAS_CORRECT_GROUP, val)) == 1
    },
    {
        "name": "out_dir",
        "type": "input",
        "message": "Enter the output directory. It may not exist, but if it does, it must be empty",
        "validate": lambda val: (len(val) > 0) and ((not os.path.exists(val)) or (len(glob.glob(os.path.join(val, "*"))) == 0))
    },
    {
        "name": "scenario",
        "type": "input",
        "message": "Enter the name of the ns-3 simulation scenario you tested",
        "default": "scratch/scenario-zero.cc"
    }
]

RENAMING_MAP = {
    "pm-Containers.pLMN-Identity": "PLMN ID",
    "list-of-matched-UEs.ueId": "UE ID",
    "cellObjectID": "Cell object ID",
    "timestamp": "Timestamp",
    "list-of-matched-UEs.pmType": "UE Performance Measurement type",
    "pm-Containers.dl-PRBUsage": "DL PRB usage",
    "pm-Containers.ul-PRBUsage": "UL PRB usage",
    "pm-Containers.dl-TotalofAvailablePRBs": "DL total available PRBs",
    "pm-Containers.ul-TotalofAvailablePRBs": "UL total available PRBs",
    "pm-Containers.nRCGI.nRCellIdentity": "NRCI",
    "list-of-matched-UEs.pmVal": "UE Performance Measurement value",
    "pm-Containers.qci": "QCI",
    "pm-Containers.drbqci": "DRB QCI",
    "pm-Containers.pDCPBytesDL": "DL PCDP Bytes",
    "pm-Containers.pDCPBytesUL": "UL PDCP Bytes",
    "pm-Containers.pLMN-Identity": "PLMN ID",
    "pm-Containers.interface-type": "Interface type",
    "pm-Containers.numberOfActive-UEs": "Number of active UEs",
    "list-of-matched-UEs.rrcEvent": "RRC Event",
    "list-of-matched-UEs.measResultNeighCells.resultsSSB-Cell.sinr": "Neighbor cells SINR",
    "list-of-matched-UEs.measResultNeighCells.physCellId": "Neighbor cell physical cell ID"
}

EXTRA_DROP = ['pm-Containers.type',
 'pm-Containers.nRCGI.pLMN-Identity']

def ask_basic_questions():
    return prompt(INITIAL_QUESTIONS)

if __name__ == '__main__':
    check = "redo"
    while check == "redo":
        answers = ask_basic_questions()
        OG_FILES = glob.glob(answers["glob_expr"])
        check_question = [{
            "name": "check",
            "type": "expand",
            "message": f"Loaded {len(OG_FILES)} files. Proceed?",
            "default": "y",
            "choices": [
                {
                    "key": "y",
                    "name": "Yes",
                    "value": "yes"
                },
                {
                    "key": "n",
                    "name": "No, abort the process",
                    "value": "no"
                },
                {
                    "key": "r",
                    "name": "No, change the provided values",
                    "value": "redo"
                }
            ]
        }]
        check = prompt(check_question)["check"]
        if check == "no":
            sys.exit(-1)
    print("Proceeding...")
    OUTPUT_DIR = answers["out_dir"]
    TEST_ID_REGEX = answers["regex"]

    os.makedirs(os.path.join(OUTPUT_DIR, "Raw", "Separated"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "Split", "Separated"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "Processed", "Separated"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "Time-Processed", "Separated"), exist_ok=True)

    # Raw file copying and processing
    merged_df = None
    for filename in OG_FILES:
        test_id = int(re.findall(TEST_ID_REGEX, filename)[0])
        shutil.copy2(filename, os.path.join(OUTPUT_DIR, "Raw", "Separated", f"DumpedData_Test{test_id}.csv"))
        raw_df = pd.read_csv(filename).rename(columns={"Unnamed: 0": "Original index"})
        raw_df["Test ID"] = test_id
        if merged_df is None:
            merged_df = raw_df
        else:
            merged_df = pd.concat([merged_df, raw_df], ignore_index=True)
    merged_df.to_csv(os.path.join(OUTPUT_DIR, "Raw", "DumpedData_Merged.csv"))

    # DU data
    merged_du_df = None
    merged_raw_du_df = None
    for filename in OG_FILES:
        test_id = int(re.findall(TEST_ID_REGEX, filename)[0])
        raw_df = pd.read_csv(filename).rename(columns={"Unnamed: 0": "Original index"})
        du_df = raw_df.query("`pm-Containers.type` == 'oDU'").copy().drop(columns=EXTRA_DROP)
        du_df = du_df.drop(columns=du_df.columns[du_df.isna().all()].to_list()).reset_index(drop=True)
        du_df.to_csv(os.path.join(OUTPUT_DIR, "Split", "Separated", f"DUData_Test{test_id}.csv"))
        du_raw_df = du_df.copy()
        du_raw_df["Test ID"] = test_id
        if merged_raw_du_df is None:
            merged_raw_du_df = du_raw_df
        else:
            merged_raw_du_df = pd.concat([merged_raw_du_df, du_raw_df], ignore_index=True)
        du_df = du_df.drop(columns="Original index").rename(columns=RENAMING_MAP).sort_values("Timestamp").reset_index(drop=True)
        du_df.to_csv(os.path.join(OUTPUT_DIR, "Processed", "Separated", f"DUData_Test{test_id}.csv"))
        du_df["Test ID"] = test_id
        if merged_du_df is None:
            merged_du_df = du_df
        else:
            merged_du_df = pd.concat([merged_du_df, du_df], ignore_index=True)
        
    merged_du_df.to_csv(os.path.join(OUTPUT_DIR, "Processed", "DUData_Merged.csv"))
    merged_raw_du_df.to_csv(os.path.join(OUTPUT_DIR, "Split", "DUData_Merged.csv"))

    # CU-UP data
    merged_cu_up_df = None
    merged_raw_cu_up_df = None
    for filename in OG_FILES:
        test_id = int(re.findall(TEST_ID_REGEX, filename)[0])
        raw_df = pd.read_csv(filename).rename(columns={"Unnamed: 0": "Original index"})
        cu_up_df = raw_df.query("`pm-Containers.type` == 'oCU-UP'").copy().drop(columns=EXTRA_DROP)
        cu_up_df = cu_up_df.drop(columns=cu_up_df.columns[cu_up_df.isna().all()].to_list()).reset_index(drop=True)
        cu_up_df.to_csv(os.path.join(OUTPUT_DIR, "Split", "Separated", f"CU-UPData_Test{test_id}.csv"))
        cu_up_raw_df = cu_up_df.copy()
        cu_up_raw_df["Test ID"] = test_id
        if merged_raw_cu_up_df is None:
            merged_raw_cu_up_df = cu_up_raw_df
        else:
            merged_raw_cu_up_df = pd.concat([merged_raw_cu_up_df, cu_up_raw_df], ignore_index=True)
        cu_up_df = cu_up_df.drop(columns="Original index").rename(columns=RENAMING_MAP).sort_values("Timestamp").reset_index(drop=True)
        cu_up_df.to_csv(os.path.join(OUTPUT_DIR, "Processed", "Separated", f"CU-UPData_Test{test_id}.csv"))
        cu_up_df["Test ID"] = test_id
        if merged_cu_up_df is None:
            merged_cu_up_df = cu_up_df
        else:
            merged_cu_up_df = pd.concat([merged_cu_up_df, cu_up_df], ignore_index=True)
        
    merged_cu_up_df.to_csv(os.path.join(OUTPUT_DIR, "Processed", "CU-UPData_Merged.csv"))
    merged_raw_cu_up_df.to_csv(os.path.join(OUTPUT_DIR, "Split", "CU-UPData_Merged.csv"))

    # CU-CP data
    merged_cu_cp_df = None
    merged_raw_cu_cp_df = None
    for filename in OG_FILES:
        test_id = int(re.findall(TEST_ID_REGEX, filename)[0])
        raw_df = pd.read_csv(filename).rename(columns={"Unnamed: 0": "Original index"})
        cu_cp_df = raw_df.query("`pm-Containers.type` == 'oCU-CP'").copy().drop(columns=EXTRA_DROP)
        cu_cp_df = cu_cp_df.drop(columns=cu_cp_df.columns[cu_cp_df.isna().all()].to_list()).reset_index(drop=True)
        cu_cp_df.to_csv(os.path.join(OUTPUT_DIR, "Split", "Separated", f"CU-CPData_Test{test_id}.csv"))
        cu_cp_raw_df = cu_cp_df.copy()
        cu_cp_raw_df["Test ID"] = test_id
        if merged_raw_cu_cp_df is None:
            merged_raw_cu_cp_df = cu_cp_raw_df
        else:
            merged_raw_cu_cp_df = pd.concat([merged_raw_cu_cp_df, cu_cp_raw_df], ignore_index=True)
        cu_cp_df = cu_cp_df.drop(columns="Original index").rename(columns=RENAMING_MAP).sort_values("Timestamp").reset_index(drop=True)
        cu_cp_df.to_csv(os.path.join(OUTPUT_DIR, "Processed", "Separated", f"CU-CPData_Test{test_id}.csv"))
        cu_cp_df["Test ID"] = test_id
        if merged_cu_cp_df is None:
            merged_cu_cp_df = cu_cp_df
        else:
            merged_cu_cp_df = pd.concat([merged_cu_cp_df, cu_cp_df], ignore_index=True)
        
    merged_cu_cp_df.to_csv(os.path.join(OUTPUT_DIR, "Processed", "CU-CPData_Merged.csv"))
    merged_raw_cu_cp_df.to_csv(os.path.join(OUTPUT_DIR, "Split", "CU-CPData_Merged.csv"))

    # Time processing
    PROCESSED_SEPARATED_CSVs = glob.glob(os.path.join(OUTPUT_DIR, "Processed", "Separated", "*.csv"))
    REPROCESS_REGEX = "Test([0-9]+).csv"
    all_time_cu_cp_processed_df = None
    all_time_cu_up_processed_df = None
    all_time_du_processed_df = None
    for filename in PROCESSED_SEPARATED_CSVs:
        bname = os.path.basename(filename)
        type_id = bname.split('_')[0]
        test_id = int(re.findall(REPROCESS_REGEX, filename)[0])
        data_df = pd.read_csv(filename).drop(columns=["Unnamed: 0"])
        first_ts = data_df["Timestamp"].min()
        data_df["Timestamp"] = data_df["Timestamp"] - first_ts
        data_df.to_csv(os.path.join(OUTPUT_DIR, "Time-Processed", "Separated", bname))
        data_df["Test ID"] = test_id
        if type_id == "CU-CPData":
            if all_time_cu_cp_processed_df is None:
                all_time_cu_cp_processed_df = data_df
            else:
                all_time_cu_cp_processed_df = pd.concat([all_time_cu_cp_processed_df, data_df])
        elif type_id == "CU-UPData":
            if all_time_cu_up_processed_df is None:
                all_time_cu_up_processed_df = data_df
            else:
                all_time_cu_up_processed_df = pd.concat([all_time_cu_up_processed_df, data_df])
        elif type_id == "DUData":
            if all_time_du_processed_df is None:
                all_time_du_processed_df = data_df
            else:
                all_time_du_processed_df = pd.concat([all_time_du_processed_df, data_df])
    all_time_cu_cp_processed_df.sort_values(["Test ID", "Timestamp"]).reset_index(drop=True).to_csv(os.path.join(OUTPUT_DIR, "Time-Processed", "CU-CPData_Merged.csv"))
    all_time_cu_up_processed_df.sort_values(["Test ID", "Timestamp"]).reset_index(drop=True).to_csv(os.path.join(OUTPUT_DIR, "Time-Processed", "CU-UPData_Merged.csv"))
    all_time_du_processed_df.sort_values(["Test ID", "Timestamp"]).reset_index(drop=True).to_csv(os.path.join(OUTPUT_DIR, "Time-Processed", "DUData_Merged.csv"))

    # Markdown generation
    now = datetime.datetime.now(datetime.timezone.utc)
    pretty_time = now.strftime("%A, %-d %B %Y, at %H:%M (%Z)")
    iso_time = now.isoformat()
    template_lines = [
        f"# xInfoDump dataset {OUTPUT_DIR}\n",
        f"Exported on {pretty_time}\n\n",
        "## Technical data\n",
        f"Test scenario: `{answers['scenario']}`\n"
        f"Exportation time: `{iso_time}`\n",
        f"Number of tests: {len(OG_FILES)}\n\n",
        "> Automatically generated by the data processing tool"
    ]
    with open(os.path.join(OUTPUT_DIR, "README.md"), 'w') as out_md:
        out_md.writelines(template_lines)
    print(f"Completed successfully! Find your processed dataset at {OUTPUT_DIR}")
