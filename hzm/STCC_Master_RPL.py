import os
from datetime import datetime

import numpy as np
import pandas as pd

from zipfile import ZipFile

from hzm.constants import STCC_NEW_MASTER_RPL_CONFIG
import query_data as ex

filepath = os.path.abspath('')


def zip_file(file_path: str, file_txt_path: str):
    """
    descriptions: compress a text file to zip file
    :param file_path: location of zip file
    :param file_txt_path: location of file text
    :return: {file_path}.zip
    """
    f_path = os.sep.join(file_path.split(os.sep)[:-1])
    is_dir = os.path.isdir(f_path)
    if not is_dir:
        os.mkdir(f_path)
    with ZipFile(f'{file_path}{STCC_NEW_MASTER_RPL_CONFIG["ZIP_EXT"]}', 'w') as zip:
        # writing each file one by one
        zip.write(file_txt_path)


def write_file(file_path: str, df: pd.DataFrame, header=''):
    """
    descriptions: write a text file
    :param file_path: location of file text
    :param df: Dataframe
    :param header: first line on text file
    :return: {file_path}.txt
    """
    np.savetxt(f'{file_path}', df.values, fmt='%s', delimiter='', header=header, comments="$$")


def get_data(df: pd.DataFrame, group: list) -> pd.DataFrame:
    df_temp = pd.DataFrame()
    for i in group:
        df_temp[i] = df[i].values
    df_temp = pd.DataFrame(list(df_temp.copy().groupby(group).groups.keys())).groupby(by=0).aggregate(lambda x: list(x))
    return df_temp


def duplicate_key_of_col(df: pd.DataFrame, col: list, len_of_group: int):
    df.columns = col
    arr_df = []
    for name in col:
        df_temp = df[name].copy().apply(lambda x: pd.Series(list(x))).rename(
            columns=lambda x: f'{name}{x + 1}').fillna('')
        count_col = len(df_temp.columns)
        if count_col < 10:
            for i in range(1, (len_of_group - count_col) + 1):
                df_temp[f'{name}{count_col + i}'] = ''
        arr_df.append(df_temp)
    df = pd.concat(arr_df, axis=1).fillna('').reset_index()
    return df


def stcc_std_indstr_classification(df: pd.DataFrame):
    group = ['STCC_ID', 'ISIC_CODE', 'SIC_CODE']
    df_code = get_data(df, group)
    df_code = duplicate_key_of_col(df_code.copy(), group[1:],
                                   STCC_NEW_MASTER_RPL_CONFIG["duplicate_key"]["STCC_STD_INDSTR_CLASSIFICATION"])
    return df_code


def stcc_sctg(df: pd.DataFrame):
    harmonies_group = ['STCC_ID', 'HARMONY_CODE']
    df_harmony = get_data(df, harmonies_group)
    df_harmony = duplicate_key_of_col(df_harmony, [harmonies_group[1]],
                                      STCC_NEW_MASTER_RPL_CONFIG["duplicate_key"]["STCC_SCTG"])
    return df_harmony


def get_df_date(df: pd.DataFrame):
    datetime_group = ['TRANS_HOUR', 'TRANS_MINUTE', 'TRANS_SECOND']
    print(datetime_group, datetime.now())
    df_rs = pd.DataFrame()
    df['TRANS_TIME'] = df['TRANS_TIME'].dt.round('1s')
    for i in datetime_group:
        type_of_time = i[6:].lower()
        df_rs[i] = getattr(df['TRANS_TIME'].dt, type_of_time)
    df_rs.columns = datetime_group
    return df_rs


def add_stcc_code(df: pd.DataFrame):
    for i in range(2, 6):
        df[f'STCC_CODE{i}'] = df['STCC_CODE'].apply(lambda x: str(x)[:i])
    return df


def calculate_data(df: pd.DataFrame):
    print('stcc_sctg', datetime.now())
    df_harmony = stcc_sctg(df)
    df_code = stcc_std_indstr_classification(df)
    remove_group = ['HARMONY_CODE', 'SIC_CODE', 'ISIC_CODE']
    df = df.drop(columns=remove_group).fillna(value='').drop_duplicates('STCC_ID', ignore_index=True)
    df_date = get_df_date(df)
    df = df.drop(columns='TRANS_TIME')
    df = add_stcc_code(df)
    df_result = pd.DataFrame()
    list_df = [df, df_harmony, df_code, df_harmony, df_date]
    print('run', datetime.now())
    for key, loc, s, fill_char in STCC_NEW_MASTER_RPL_CONFIG["result_column"]:
        print(key, loc, s, fill_char)
        if s == "R":
            s = "right"
        else:
            s = "left"
        for df_temp in list_df:
            if key in df_temp.columns:
                df_result[key] = df_temp[key].fillna(value='')
                df_result[key] = df_result[key].astype(str).str.pad(loc, side=s, fillchar=fill_char)
                break
    df_result["DELETE_DATE"] = 99991231
    return df_result


def run(month_of_run: int, year_of_run: int):
    df = ex.execute_sql("IRF_DATABASE", "stcc_master_rpl")
    df.columns = STCC_NEW_MASTER_RPL_CONFIG["columns"]
    df["SPACE"] = ''
    print('calculate_data', datetime.now())
    df = calculate_data(df)

    file_path = os.path.join(filepath + os.sep,
                             f'{STCC_NEW_MASTER_RPL_CONFIG["FILE_NAME"]}'
                             f'{STCC_NEW_MASTER_RPL_CONFIG["TXT_EXT"]}')
    # Create text file
    write_file(file_path, df)
    # Create zip file
    zip_path = os.path.join(filepath + os.sep, '{archive_path}{sep}{file_name}{year_of_run}{month_of_run}'.format(
        archive_path=STCC_NEW_MASTER_RPL_CONFIG["ARCHIVE_PATH"],
        sep=os.sep,
        file_name=STCC_NEW_MASTER_RPL_CONFIG["FILE_NAME"],
        year_of_run=str(year_of_run)[2:],
        month_of_run=month_of_run))
    zip_file(zip_path, file_path)
    return df


if __name__ == '__main__':
    run(10, 2020)
