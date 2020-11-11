import logging
import os
import unicodedata
from datetime import datetime
import pandas as pd

from hzm.STCC_Master_RPL import write_file, get_data, duplicate_key_of_col, filepath, zip_file
from hzm.constants import IRFHMDB1_CONFIG
import query_data as ex
logger = logging.getLogger()

def get_data(df: pd.DataFrame, group: list) -> pd.DataFrame:
    df_temp = pd.DataFrame()
    for i in group:
        df_temp[i] = df[i].values
    df_temp = pd.DataFrame(list(df_temp.copy().groupby(group).groups.keys())).groupby(by=0).aggregate(lambda x: list(x))
    return df_temp


def calculate_data(df: pd.DataFrame):
    list_df = []
    list_duplicate = ["CPSN_SEQ", "IPSN_SEQ", "NPSN_SEQ"]
    for i in list_duplicate:
        key = i[:4]
        df_temp = get_data(df, ['HAZMAT_CODE', i, key])
        df_temp = duplicate_key_of_col(df_temp, [i, key], 5)
        list_df.append(df_temp)
    df = df.fillna(value='').drop_duplicates('HAZMAT_CODE', keep='first', ignore_index=True)
    list_df.insert(0, df)
    df_result = pd.DataFrame()
    for key, loc, s, fill_char in IRFHMDB1_CONFIG["result_column"]:
        if s == "R":
            s = "right"
        else:
            s = "left"
        for df_temp in list_df:
            if key in df_temp.columns:
                df_result[key] = df_temp[key].apply(
                    lambda val: unicodedata.normalize('NFKD', val).encode('ascii', 'ignore').decode()).astype(
                    str).str.pad(loc, side=s, fillchar=fill_char)
                break
    return df_result


def run():
    df = ex.execute_sql("HAZMAT_DATABASE", "hmdb_xb9130_extract_hmat")
    logger.info('calculate_data', datetime.now())
    df.columns = IRFHMDB1_CONFIG["columns"]
    df["SPACE"] = ' '
    df = calculate_data(df)
    file_path = os.path.join(filepath + os.sep,
                             f'{IRFHMDB1_CONFIG["FILE_NAME"]}'
                             f'{IRFHMDB1_CONFIG["TXT_EXT"]}')
    # Create text file
    logger.info("create text file")
    write_file(file_path, df, header=IRFHMDB1_CONFIG["IRFHMDB1_CONFIG"])
    # Create zip file
    logger.info("create zip file")
    zip_path = os.path.join(filepath + os.sep, '{archive_path}{sep}{file_name}'.format(
        archive_path=IRFHMDB1_CONFIG["ARCHIVE_PATH"],
        sep=os.sep,
        file_name=IRFHMDB1_CONFIG["FILE_NAME"]))
    zip_file(zip_path, file_path)
    return


if __name__ == '__main__':
    run()
