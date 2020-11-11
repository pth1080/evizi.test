import logging
import os
from datetime import datetime

import pandas as pd

from hazmat.helpers import dataframe_by_sql as dbs
from hazmat.helpers import email as e
from hazmat.helpers import parse_env_file as ef
from hazmat.helpers import string_utilities as su
from hazmat.helpers import time_utilities as tu
from hazmat.modules import constants as cons

env = ef.parse_env_file()
logger = logging.getLogger()


def work_erg_guide_phrase():
    """
    get work_erg_guide_phrase dataframe by sql query
    :return: work_erg_guide_phrase dataframe
    """

    query = "behm_master_erg_guide_phrase"
    result = dbs.dataframe_by_sql("ORACLE_COREDEV", query)
    return result


def hazmat(middle_day_of_next_month):
    """
    get hazmat dataframe by sql query
    :param middle_day_of_next_month: middle day of next month
    :return: hazmat dataframe
    """

    query = "behm_master_hazmat"
    params = {"cur_date": middle_day_of_next_month}
    result = dbs.dataframe_by_sql("ORACLE_COREDEV", query, **params)
    return result


def hazmat_un_na_nums():
    """
    get hazmat_un_na_nums dataframe by sql query
    :return: hazmat_un_na_nums dataframe
    """

    query = "behm_master_hazmat_un_na_nums"
    result = dbs.dataframe_by_sql("ORACLE_COREDEV", query)
    return result


def behmfile_all_records(middle_day_of_next_month):
    query = "behm_master_behmfile_all_records"
    params = {"cur_date": middle_day_of_next_month}
    columns = [
        "EMR_TEXT",
        "HAZMAT_CODE",
        "RECORD_TYPE",
        "UN_NA_NUM",
        "ORDER_NBR",
        "GUIDE_ID",
        "EMR_ORDER",
        "SEQ_NO",
    ]
    behmfile_all_records_df = dbs.dataframe_by_sql(
        "ORACLE_COREDEV", query, *columns, **params
    )
    return behmfile_all_records_df


def insert_into_behmfile_all_records_1(middle_day_of_next_month):
    """
    get insert_into_behmfile_all_records_1 dataframe by sql query
    :param middle_day_of_next_month: middle day of next month
    :return: insert_into_behmfile_all_records_1 dataframe
    """

    query = "behm_master_insert_into_behmfile_all_records_1"
    params = {"cur_date": middle_day_of_next_month}
    columns = [
        "EMR_TEXT",
        "HAZMAT_CODE",
        "RECORD_TYPE",
        "UN_NA_NUM",
        "ORDER_NBR",
        "GUIDE_ID",
        "EMR_ORDER",
        "SEQ_NO",
    ]
    result = dbs.dataframe_by_sql("ORACLE_COREDEV", query, *columns, **params)
    return result


def insert_into_behmfile_all_records_2(middle_day_of_next_month):
    """
    get insert_into_behmfile_all_records_2 dataframe by sql query
    :param middle_day_of_next_month: middle day of next month
    :return: insert_into_behmfile_all_records_2 dataframe
    """

    query = "behm_master_insert_into_behmfile_all_records_2"
    params = {"cur_date": middle_day_of_next_month}
    columns = [
        "EMR_TEXT",
        "HAZMAT_CODE",
        "RECORD_TYPE",
        "UN_NA_NUM",
        "ORDER_NBR",
        "GUIDE_ID",
        "EMR_ORDER",
        "SEQ_NO",
    ]
    result = dbs.dataframe_by_sql("ORACLE_COREDEV", query, *columns, **params)
    return result


def behmfile_all_records_final(
        behmfile_all_records_df, data_to_insert_1_df, data_to_insert_2_df
):
    """
    get behmfile_all_records_final dataframe by sub dataframes
    :param behmfile_all_records_df: behmfile_all_records dataframe
    :param data_to_insert_1_df: dataframe 1 to insert to behmfile_all_records dataframe
    :param data_to_insert_2_df: dataframe 2 to insert to behmfile_all_records dataframe
    :return: behmfile_all_records_final dataframe
    """

    behmfile_all_records_final_df = pd.concat(
        [behmfile_all_records_df, data_to_insert_1_df, data_to_insert_2_df],
        ignore_index=True,
    )
    result = behmfile_all_records_final_df.sort_values(
        by=[
            "HAZMAT_CODE",
            "GUIDE_ID",
            "SEQ_NO",
            "RECORD_TYPE",
            "ORDER_NBR",
            "EMR_ORDER",
        ]
    ).reset_index(drop=True)
    return result


def write_behmfile_by_code_to_file(behmfile_by_code_df, hazmat_code, *files_to_write):
    """
    break input dataframe into smaller pieces and write them to file
    :param behmfile_by_code_df: input dataframe
    :param hazmat_code: hazmat code
    :param files_to_write: list file to write
    """

    for index, row in behmfile_by_code_df.iterrows():
        if row.EMR_TEXT:
            if row.UN_NA_NUM is None:
                row.UN_NA_NUM = ""
            if index == 0:
                recs_written = 0
                stcc_line_nbr = 0
                print_line_nbr = 0
                type_line_nbr = 0
                un_na_1 = row.UN_NA_NUM[0:2]
                un_na_2 = row.UN_NA_NUM[2:6]
            else:
                if un_na_2 == row.UN_NA_NUM[2:6] and un_na_1 != row.UN_NA_NUM[0:2]:
                    row.ORDER_NBR = "0"

            if row.ORDER_NBR == "1":
                x = 0
                for i in range(1, 8):
                    emr_tmp_text = su.strip_string(row.EMR_TEXT, x, 72)
                    if not emr_tmp_text:
                        break
                    x += len(emr_tmp_text) + 1
                    stcc_line_nbr += 1
                    print_line_nbr += 1
                    type_line_nbr += 1
                    if i == 1:
                        emr_tmp_text83 = (
                                " " + emr_tmp_text.ljust(72) + hazmat_code
                        ).ljust(83)
                    else:
                        emr_tmp_text83 = ("   " + emr_tmp_text.ljust(72)).ljust(83)
                    out = (
                            emr_tmp_text83
                            + hazmat_code
                            + str(stcc_line_nbr).rjust(3)
                            + row.RECORD_TYPE
                            + str(type_line_nbr).rjust(2)
                            + str(print_line_nbr).rjust(3)
                            + row.UN_NA_NUM
                    )
                    for file_to_write in files_to_write:
                        file_to_write.write(f"{out}\n")
                    recs_written += 1

            elif row.ORDER_NBR == "2":
                x = 0
                for i in range(1, 8):
                    emr_tmp_text = su.strip_string(row.EMR_TEXT, x, 72)
                    if not emr_tmp_text:
                        break
                    x += len(emr_tmp_text) + 1
                    stcc_line_nbr += 1
                    print_line_nbr += 1
                    type_line_nbr += 1
                    if i == 1:
                        emr_tmp_text83 = (
                                " " + emr_tmp_text.ljust(72) + row.UN_NA_NUM
                        ).ljust(83)
                    else:
                        emr_tmp_text83 = ("   " + emr_tmp_text.ljust(72)).ljust(83)
                    out = (
                            emr_tmp_text83
                            + hazmat_code
                            + str(stcc_line_nbr).rjust(3)
                            + row.RECORD_TYPE
                            + str(type_line_nbr).rjust(2)
                            + str(print_line_nbr).rjust(3)
                            + row.UN_NA_NUM
                    )
                    for file_to_write in files_to_write:
                        file_to_write.write(f"{out}\n")
                    recs_written += 1
            elif row.ORDER_NBR == "3":
                x = 0
                for i in range(1, 3):
                    emr_tmp_text = su.strip_string(row.EMR_TEXT, x, 72)
                    if not emr_tmp_text:
                        break
                    x += len(emr_tmp_text) + 1
                    stcc_line_nbr += 1
                    print_line_nbr += 1
                    type_line_nbr += 1
                    emr_tmp_text83 = (" " + emr_tmp_text.ljust(72)).ljust(83)
                    out = (
                            emr_tmp_text83
                            + hazmat_code
                            + str(stcc_line_nbr).rjust(3)
                            + row.RECORD_TYPE
                            + str(type_line_nbr).rjust(2)
                            + str(print_line_nbr).rjust(3)
                            + row.UN_NA_NUM
                    )
                    for file_to_write in files_to_write:
                        file_to_write.write(f"{out}\n")
                    recs_written += 1

            if row.RECORD_TYPE == "A" and row.LAST_RECORD_TYPE:
                emr_tmp_text83 = " " * 83
                stcc_line_nbr += 1
                type_line_nbr += 1
                out = (
                        emr_tmp_text83
                        + hazmat_code
                        + str(stcc_line_nbr).rjust(3)
                        + row.RECORD_TYPE
                        + str(type_line_nbr).rjust(2)
                        + "   "
                        + row.UN_NA_NUM
                )
                for file_to_write in files_to_write:
                    file_to_write.write(f"{out}\n")
                type_line_nbr = 0

            # Order 4 lines are Preformatted in database
            if row.ORDER_NBR == "4":
                x = 0
                for i in range(1, 18):
                    j = 79 * (i - 1)
                    emr_tmp_text = row.EMR_TEXT[j: j + 79]
                    if not emr_tmp_text:
                        break
                    stcc_line_nbr += 1
                    print_line_nbr += 1
                    type_line_nbr += 1
                    emr_tmp_text83 = (" " + emr_tmp_text.ljust(79)).ljust(83)
                    out = (
                            emr_tmp_text83
                            + hazmat_code
                            + str(stcc_line_nbr).rjust(3)
                            + row.RECORD_TYPE
                            + str(type_line_nbr).rjust(2)
                            + str(print_line_nbr).rjust(3)
                            + row.UN_NA_NUM
                    )
                    for file_to_write in files_to_write:
                        file_to_write.write(f"{out}\n")
                emr_tmp_text83 = " " * 83
                type_line_nbr += 1
                stcc_line_nbr += 1
                out = (
                        emr_tmp_text83
                        + hazmat_code
                        + str(stcc_line_nbr).rjust(3)
                        + row.RECORD_TYPE
                        + str(type_line_nbr).rjust(2)
                        + "   "
                        + row.UN_NA_NUM
                )
                for file_to_write in files_to_write:
                    file_to_write.write(f"{out}\n")
                recs_written += 1
            elif row.ORDER_NBR == "5" and recs_written > 0:
                x = 0
                if (
                        row.RECORD_TYPE != row.PREV_RECORD_TYPE
                        and row.PREV_RECORD_TYPE != " "
                        and row.PREV_HAZMAT_CODE == hazmat_code
                ):
                    emr_tmp_text83 = " " * 83
                    stcc_line_nbr += 1
                    type_line_nbr += 1
                    out = (
                            emr_tmp_text83
                            + hazmat_code
                            + str(stcc_line_nbr).rjust(3)
                            + row.PREV_RECORD_TYPE
                            + str(type_line_nbr).rjust(2)
                            + "   "
                            + row.UN_NA_NUM
                    )
                    for file_to_write in files_to_write:
                        file_to_write.write(f"{out}\n")
                    type_line_nbr = 0
                if row.RECORD_TYPE != row.PREV_RECORD_TYPE:
                    type_line_nbr = 0
                for i in range(1, 8):
                    if i == 1:
                        emr_tmp_text = su.strip_string(row.EMR_TEXT, x, 78)
                    else:
                        emr_tmp_text = su.strip_string(row.EMR_TEXT, x, 76)
                    if not emr_tmp_text:
                        break
                    x += len(emr_tmp_text) + 1
                    changes = [
                        ("PEQUE?", "PEQUEN"),
                        ("DA?", "DAN"),
                        ("SE?AL", "SENAL"),
                        ("DISE?", "DISEN"),
                        ("?", " "),
                        ("[", " "),
                        ("]", " "),
                    ]
                    emr_tmp_text = su.replace_string(emr_tmp_text, changes)
                    emr_tmp_text79 = emr_tmp_text.ljust(79)
                    if row.EMR_ORDER:
                        emr_tmp_text83 = (" " + emr_tmp_text79).ljust(83)
                    elif i == 1:
                        emr_tmp_text83 = ("   " + emr_tmp_text79).ljust(83)
                    else:
                        emr_tmp_text83 = ("     " + emr_tmp_text.ljust(78)).ljust(83)

                    stcc_line_nbr += 1
                    print_line_nbr += 1
                    type_line_nbr += 1
                    out = (
                            emr_tmp_text83
                            + hazmat_code
                            + str(stcc_line_nbr).rjust(3)
                            + row.RECORD_TYPE
                            + str(type_line_nbr).rjust(2)
                            + str(print_line_nbr).rjust(3)
                            + row.UN_NA_NUM
                    )
                    for file_to_write in files_to_write:
                        file_to_write.write(f"{out}\n")
                if index == behmfile_by_code_df.shape[0] - 1:
                    emr_tmp_text83 = " " * 83
                    for i in range(0, 2):
                        stcc_line_nbr += 1
                        type_line_nbr += 1
                        out = (
                                emr_tmp_text83
                                + hazmat_code
                                + str(stcc_line_nbr).rjust(3)
                                + row.RECORD_TYPE
                                + str(type_line_nbr).rjust(2)
                                + str(print_line_nbr).rjust(3)
                                + row.UN_NA_NUM
                        )
                        for file_to_write in files_to_write:
                            file_to_write.write(f"{out}\n")


def behmfile_all_records_to_file(behmfile_all_records_df):
    """
    write file from behmfile_all_records datafram
    :param behmfile_all_records_df: behmfile_all_records dataframe
    :return: file txt
    """

    behmfile_all_records_df["HAZMAT_CODE"] = behmfile_all_records_df[
        "HAZMAT_CODE"
    ].astype(str)
    hazmat_codes = sorted(behmfile_all_records_df["HAZMAT_CODE"].unique())
    behmfile_all_records_df[
        "PREV_RECORD_TYPE"
    ] = behmfile_all_records_df.RECORD_TYPE.shift(1)
    behmfile_all_records_df[
        "PREV_HAZMAT_CODE"
    ] = behmfile_all_records_df.HAZMAT_CODE.shift(1)

    behm_file_name = cons.BEHM_MASTER["BEHM_FILE_NAME"]
    tempfile = os.path.join(env.path("ARCHIVE_PATH"), behm_file_name)
    behm_bnsffile_name = cons.BEHM_MASTER["BEHM_BNSFFILE_NAME"]
    behmbn = os.path.join(env.path("ARCHIVE_PATH"), behm_bnsffile_name)
    archive_name = cons.BEHM_MASTER["ARCHIVE_NAME"]
    archive_path = os.path.join(env.path("ARCHIVE_PATH"), archive_name)
    header = cons.BEHM_MASTER["HEADER"]

    logger.info(
        "write " + behm_file_name + " and " + behm_bnsffile_name + " files started"
    )
    with open(tempfile, "wt") as f1, open(behmbn, "wt") as f2:
        f2.write(f"{header}\n")
        for hazmat_code in hazmat_codes:
            behmfile_by_code_df = behmfile_all_records_df.loc[
                behmfile_all_records_df.HAZMAT_CODE.eq(hazmat_code)
            ].reset_index(drop=True)
            if not behmfile_by_code_df.empty:
                behmfile_by_code_df["LAST_RECORD_TYPE"] = (
                        behmfile_by_code_df.RECORD_TYPE
                        != behmfile_by_code_df.RECORD_TYPE.shift(-1)
                ).astype(int)
                files_to_write = (f1, f2)
                write_behmfile_by_code_to_file(
                    behmfile_by_code_df, hazmat_code, *files_to_write
                )
    logger.info(
        "write " + behm_file_name + " and " + behm_bnsffile_name + " files completed"
    )


def behm_master():
    # logger config
    logger.setLevel(env.log_level("LOG_LEVEL"))
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s : %(name)s : %(levelname)s : %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # calculate time
    today = datetime.utcnow()
    middle_day_of_next_month = tu.calculate_time(today, 1, "m", "%Y-%m-%d")

    # Get email information from database for sending notifications
    email_info = e.get_email_info()
    to_notify_emails = email_info["to_notify_emails"]
    cc_notify_emails = email_info["cc_notify_emails"]
    from_email = email_info["from_email"]
    reply_to_email = email_info["reply_to_email"]

    # Calculate the required data to pull the file
    behmfile_all_records_df = behmfile_all_records(middle_day_of_next_month)
    data_to_insert_1_df = insert_into_behmfile_all_records_1(middle_day_of_next_month)
    data_to_insert_2_df = insert_into_behmfile_all_records_2(middle_day_of_next_month)
    behmfile_all_records_final_df = behmfile_all_records_final(
        behmfile_all_records_df, data_to_insert_1_df, data_to_insert_2_df
    )

    # Notify the Users that the behm master extract started
    logger.info("Notify the Users that the behm master extract started")
    e.send_email(
        to_notify_emails,
        cc_notify_emails,
        from_email,
        reply_to_email,
        cons.BEHM_MASTER["STARTED_EMAIL_SUBJECT"],
    )

    # Create file
    behmfile_all_records_to_file(behmfile_all_records_final_df)

    # Notify the Users that behm master extract completed
    logger.info("Notify the Users that behm master extract completed")
    e.send_email(
        to_notify_emails,
        cc_notify_emails,
        from_email,
        reply_to_email,
        cons.BEHM_MASTER["COMPLETED_EMAIL_SUBJECT"],
    )


if __name__ == "__main__":
    behm_master()
