from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import cx_Oracle

from parse_env_file import parse_env_file


def get_query(key):
    queries = {
        "get_config": """
        SELECT * FROM IRF.CONFIG_VALUES
        """,
        "stcc_master_rpl": """
        SELECT
            DISTINCT S.STCC_ID AS STCC_ID,
            S.STCC_CODE AS STCC_CODE,
            SSIC.SIC_CODE,
            SSIC.ISIC_CODE,
            SS.SCTG_CODE,
            S.STCC50_CODE AS STCC50_CODE,
            TO_CHAR(S.MODIFIED_TS, 'yyyymmdd') AS TRANS_DATE,
            S.MODIFIED_TS AS TRANS_TIME,
            S.TRANSACTION_CODE,
            SH.HARMONY_CODE,
            CS54.CS54_GROUP_CODE,
            CS54.CS54_GROUP_NAME,
            DEREG.DEREGULATION_CODE,
            TO_CHAR(DEREG.DEREGULATION_DATE,'yyyymmdd') AS DEREG_DATE,
            SCG.MINIMUM_CAR_GRADE,
            TO_CHAR(S.EFFECTIVE_DATE,'mmddyyyy') AS EFF_DATE,
            S.PRODUCT_DESC,
            S.PRODUCT_SHORT_DESC,
            S.ALTERNATE_NBR,
            S.NEW_STCC_CODE,
            TO_CHAR(S.EXPIRATION_DATE, 'yyyymmdd') AS EXP_DATE,
            TO_CHAR(S.MODIFIED_TS, 'yyyymmdd') AS DELETE_DATE 
        FROM
            IRF.STCC S    
        LEFT JOIN
            IRF.STCC_HARMONY SH          
                ON (
                    S.STCC_ID = SH.STCC_ID
                )  
        LEFT JOIN
            IRF.STCC_STD_INDSTR_CLASSIFICATION SSIC 
                ON (
                    S.STCC_ID = SSIC.STCC_ID
                )  
        LEFT JOIN
            IRF.STCC_SCTG SS            
                ON (
                    S.STCC_ID = SS.STCC_ID
                )  
        LEFT JOIN
            IRF.STCC_CS54_GROUP CS54      
                ON (
                    CS54.STCC_CODE_FROM <= S.STCC_CODE                
                    AND CS54.STCC_CODE_TO   >= S.STCC_CODE                
                    AND CS54.STATUS_CODE = 'A'
                )  
        LEFT JOIN
            IRF.STCC_CAR_GRADE SCG     
                ON (
                    SCG.STCC_CODE_FROM <= S.STCC_CODE                
                    AND SCG.STCC_CODE_TO   >= S.STCC_CODE                
                    AND SCG.STATUS_CODE = 'A'
                )  
        LEFT JOIN
            IRF.STCC_DEREGULATION DEREG     
                ON (
                    DEREG.STCC_CODE_FROM <= S.STCC_CODE                
                    AND DEREG.STCC_CODE_TO   >= S.STCC_CODE                
                    AND DEREG.STATUS_CODE = 'A'
                ) 
        ORDER BY
            S.STCC_ID,
            SH.HARMONY_CODE,
            SSIC.SIC_CODE,
            SSIC.ISIC_CODE,
            SS.SCTG_CODE

        """,
        "hmdb_xb9130_extract_hmat": """
        SELECT
            A.HAZMAT_CODE,
            A.STCC_CODE,
            TO_CHAR(A.TRANS_TS, 'yyyymmdd') AS TRANS_DATE,
            A.TRANS_TIME,
            '2' AS TRANS_CODE,
            TO_CHAR(A.EFF_DATE, 'yyyymmdd') AS EFF_DATE,
            A.MAJOR_GROUP AS HEADER1,
            A.MINOR_GROUP AS HEADER2,
            A.INDUSTRY_GROUP AS HEADER3,
            A.PRODUCT_GROUP AS HEADER4,
            A.PROD_DESC_15CHAR,
            A.ALTERNATIVE_NUMBER,
            TO_CHAR(A.EXP_DATE, 'yyyymmdd') AS EXP_DATE,
            A.PROP_SHIP_ALPHA,
            I.PRI_CL1 AS IMO_CLASS,
            (    CASE      
                WHEN I.NOS_IND = '1' THEN 'N'      
                ELSE ' '     
            END   ) AS INOS_IND,
            I.TECHNAME AS ITECHNAME,
            I.UN_NA_NUM AS IUN_NA_NUM,
            I.PACKING_GRP AS IPACKING_GROUP,
            I.POISON_MAT_IND AS IPOISON_MAT_IND,
            I.PRI_PSN AS IPROPNAME,
            I.PRI_PLACARD_NOT AS IPRI_PLACARD_NOT,
            C.PRI_CL1 AS CPRI_CL,
            C.SEC_CL1 AS CSEC_CL1,
            C.SEC_CL2 AS CSEC_CL2,
            C.SEC_CL3 AS CSEC_CL3,
            (    CASE      
                WHEN C.CANO_USDEST = '1' THEN 'C'      
                WHEN C.CANO_USDEST = '2' THEN 'P'     
                WHEN C.CANO_USDEST = '3' THEN 'E'      
                ELSE ' '     
            END   ) AS CCANO_USDEST,
            C.ERP_IND AS CERP_IND,
            C.PRI_PLACARD_NOT AS CPRI_PLACARD_NOT,
            C.SPEC_COMM_IND AS CSPEC_COMM_IND,
            C.SUB_RISK_IND AS CSUB_RISK_IND,
            (    CASE      
                WHEN C.NOS_IND = '1' THEN 'N'      
                ELSE ' '     
            END   ) AS CNOS_IND,
            C.SEC_PLACARD_NOT AS CSEC_PLACARD_NOT,
            C.TECHNAME AS CTECHNAME,
            C.UN_NA_NUM AS CUN_NA_NUM,
            C.PACKING_GRP AS CPACKING_GRP,
            C.POISON_MAT_IND AS CPOISON_MAT_IND,
            C.PRI_PSN AS CPRI_PSN,
            N.EPA_WASTCHAR1 AS NEPA_WASTCHAR1,
            N.EPA_WASTCHAR2 AS NEPA_WASTCHAR2,
            N.EPA_WASTCHAR3 AS NEPA_WASTCHAR3,
            N.EPA_WASTSTRM1 AS NEPA_WASTSTRM1,
            N.EPA_WASTSTRM2 AS NEPA_WASTSTRM2,
            N.EPA_WASTSTRM3 AS NEPA_WASTSTRM3,
            N.HAZ_PLA_ENDOR AS NHAZ_PLA_ENDOR,
            N.PRI_CL1 AS NPRI_CL,
            N.SEC_CL1 AS NSEC_CL1,
            N.SEC_CL2 AS NSEC_CL2,
            A.HAZ_ZONE,
            (    CASE      
                WHEN N.NOS_IND = '1' THEN 'N'      
                ELSE ' '     
            END   ) AS NNOS_IND,
            N.SEC_PLACARD_NOT AS NSEC_PLACARD_NOT,
            N.TECHNAME AS NTECHNAME,
            N.UN_NA_NUM AS NUN_NA_NUM,
            (    CASE      
                WHEN N.USO_CAND_FLAG = '1' THEN 'U'      
                WHEN N.USO_CAND_FLAG = '2' THEN 'P'      
                WHEN N.USO_CAND_FLAG = '3' THEN 'E'      
                ELSE ' '     
            END   ) AS NUSO_CAND_FLAG,
            N.PACKING_GRP AS NPACKING_GRP,
            N.POISON_MAT_IND AS NPOISON_MAT_IND,
            N.PRI_PLACARD_NOT AS NPRI_PLACARD_NOT,
            N.PRI_PSN AS NPRI_PSN,
            A.OT55_FLAG,
            (    CASE      
                WHEN A.DANGER_WHEN_WET = '1' THEN 'D'      
                ELSE ' '     
            END   ) AS DANGER_WHEN_WET,
            (    CASE      
                WHEN A.REPORTABLE_QTY = '1' THEN 'R'      
                ELSE ' '     
            END   ) AS REPORTABLE_QTY,
            (    CASE      
                WHEN A.MAR_POLL_FLAG = '0' THEN ' '      
                ELSE 'M'     
            END   ) AS MAR_POLL_FLAG,
            A.HAZ_SUB_NAME,
            A.MAR_POLL_NAME,
            C.PRI_CL2 AS CPRI_CL2,
            C.SPECIAL_PSN_FLAG AS CSPECIAL_PSN_FLAG,
            I.SPECIAL_PSN_FLAG AS ISPECIAL_PSN_FLAG,
            N.SPECIAL_PSN_FLAG AS NSPECIAL_PSN_FLAG,
            C.INTL_IND AS CINTL_IND,
            I.INTL_IND AS IINTL_IND,
            N.INTL_IND AS NINTL_IND,
            C.APPROVED_TANKCAR AS CAPPROVED_TANKCAR,
            N.APPROVED_TANKCAR AS NAPPROVED_TANKCAR,
            A.ALPHA_DESC,
            A.PCSTCC_DESC,
            I.SEC_PLACARD_NOT AS ISEC_PLACARD_NOT,
            I.SEC_CL1 AS ISEC_CL1,
            I.SEC_CL2 AS ISEC_CL2,
            I.SEC_CL3 AS ISEC_CL3,
            TO_CHAR(A.DELETE_DATE, 'yyyymmdd') AS DELETE_DATE,
            P1.SEC_PSN_DESC AS CPSN,
            P2.SEC_PSN_DESC AS IPSN,
            P3.SEC_PSN_DESC AS NPSN,
            P1.SEQ_NO AS CPSN_SEQ,
            P2.SEQ_NO AS IPSN_SEQ,
            P3.SEQ_NO AS NPSN_SEQ,
            TO_CHAR(A.ADD_TS, 'yyyymmdd') AS ADD_DATE,
            TO_CHAR(A.LAST_UPDATE_TS, 'yyyymmdd') AS LAST_UPD_DATE  
        FROM
            HAZMAT_IRF.HAZMAT A   
        LEFT JOIN
            HAZMAT_IRF.HAZMAT_AGENCY C    
                ON (
                    C.HAZMAT_KEY = A.HAZMAT_KEY 
                    AND C.REG_AGENCY = 'C'
                )  
        LEFT JOIN
            HAZMAT_IRF.HAZMAT_AGENCY I    
                ON (
                    I.HAZMAT_KEY = A.HAZMAT_KEY 
                    AND I.REG_AGENCY = 'I'
                )  
        LEFT JOIN
            HAZMAT_IRF.HAZMAT_AGENCY N    
                ON (
                    N.HAZMAT_KEY = A.HAZMAT_KEY 
                    AND N.REG_AGENCY = 'N'
                )  
        LEFT JOIN
            HAZMAT_IRF.HAZMAT_PROPER_SHIPNAME P1    
                ON (
                    P1.HAZMAT_KEY = A.HAZMAT_KEY 
                    AND P1.REG_AGENCY = 'C'
                )  
        LEFT JOIN
            HAZMAT_IRF.HAZMAT_PROPER_SHIPNAME P2    
                ON (
                    P2.HAZMAT_KEY = A.HAZMAT_KEY 
                    AND P2.REG_AGENCY = 'I'
                )  
        LEFT JOIN
            HAZMAT_IRF.HAZMAT_PROPER_SHIPNAME P3    
                ON (
                    P3.HAZMAT_KEY = A.HAZMAT_KEY 
                    AND P3.REG_AGENCY = 'N'
                )  
        WHERE
            A.HAZMAT_CODE <> '4800000'    
            AND A.EXP_DATE > TO_DATE('20201001', 'YYYYMMDD')   
            AND A.EFF_DATE <= TO_DATE('20201001', 'YYYYMMDD')   
            AND  A.STATUS_CODE ^= 'W'  
        ORDER BY
            HAZMAT_CODE,
            TRANS_CODE,
            TRANS_TIME
        """,
        "HMDB_XB9130_MEXTRACT_HMAT_temp": """
        SELECT
            A.HAZMAT_CODE,
            A.STCC_CODE,
            TO_CHAR(A.TRANS_TS,
            'yyyymmdd') AS TRANS_DATE,
            A.TRANS_TIME,
            '2' AS TRANS_CODE,
            TO_CHAR(A.EFF_DATE,
            'yyyymmdd') AS EFF_DATE,
            TO_CHAR(A.EXP_DATE,
            'yyyymmdd') AS EXP_DATE,
            A.OT55_FLAG,
            M.UN_NA_NUM  AS MUN_NA_NUM,
            M.PRI_PSN   AS MPROPNAME,
            M.TECHNAME   AS MTECHNAME,
            A.HAZ_SUB_NAME_SP,
            A.MAR_POLL_NAME_SP,
            M.PRI_CL1   AS IMO_CLASS,
            M.PACKING_GRP  AS MPACKING_GROUP,
            M.PRI_PLACARD_NOT AS MPRI_PLACARD_NOT,
            M.INTL_IND   AS MINTL_IND,
            M.POISON_MAT_IND AS MPOISON_MAT_IND,
            M.SEC_PLACARD_NOT AS MSEC_PLACARD_NOT,
            (CASE 
                WHEN M.NOS_IND = '1' THEN 'N' 
                ELSE ' ' 
            END) AS MNOS_IND,
            (CASE 
                WHEN A.DANGER_WHEN_WET = '1' THEN 'D' 
                ELSE ' ' 
            END) AS DANGER_WHEN_WET,
            M.APPROVED_TANKCAR  AS MAPPROVED_TANKCAR,
            M.SPECIAL_PSN_FLAG  AS MSPECIAL_PSN_FLAG,
            M1.SEC_PSN_DESC AS MPSN,
            M1.SEQ_NO       AS MPSN_SEQ,
            M.SEC_CL1   AS MSEC_CL1,
            M.SEC_CL2   AS MSEC_CL2,
            M.SEC_CL3   AS MSEC_CL3,
            (CASE 
                WHEN A.MAR_POLL_NAME_SP > ' ' THEN 'M' 
                ELSE ' ' 
            END) AS MAR_POLL_FLAG,
            (CASE 
                WHEN A.HAZ_SUB_NAME_SP > ' ' THEN 'R' 
                ELSE ' ' 
            END) AS REPORTABLE_QTY,
            TO_CHAR(A.ADD_TS,
            'yyyymmdd') AS ADD_DATE,
            TO_CHAR(A.last_update_ts,
            'yyyymmdd') AS LAST_UPD_DATE   
        FROM
            HAZMAT_IRF.Hazmat A    
        LEFT JOIN
            HAZMAT_IRF.HAZMAT_AGENCY M 
                ON (
                    M.HAZMAT_KEY = A.HAZMAT_KEY 
                    AND M.REG_AGENCY = 'M'
                )   
        LEFT JOIN
            HAZMAT_IRF.HAZMAT_PROPER_SHIPNAME M1 
                ON (
                    M1.HAZMAT_KEY = A.HAZMAT_KEY 
                    AND M1.REG_AGENCY = 'M'
                )  
        WHERE
            A.HAZMAT_CODE != '4800000'   
            AND A.EXP_DATE > TO_DATE('20201001', 'YYYYMMDD')    
            AND A.EFF_DATE <= TO_DATE('20201001', 'YYYYMMDD')   
            AND  A.STATUS_CODE ^= 'W'  
            AND (
                TRIM(A.HAZ_SUB_DESC_SP) != ' '   
                OR TRIM(A.HAZ_CLASS_DESC_SP) != ' '   
                OR TRIM(A.REPORTABLE_QTY_DESC_SP) != ' '   
                OR TRIM(A.EMR_ALT_PSN_DESC_SP) != ' '
            )   
        ORDER BY
            HAZMAT_CODE,
            TRANS_CODE 
    """
    }
    return queries[key]


env = parse_env_file()


def execute_sql(DB_NAME, query):
    config_db = env.str(DB_NAME)
    DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_SID = config_db.split(',')

    try:
        dsnStr = cx_Oracle.makedsn(DB_HOST, DB_PORT, DB_SID)
        # conn = cx_Oracle.connect(user=DB_USERNAME, password=DB_PASSWORD, dsn=dsnStr, encoding="UTF-8",
        #                          nencoding="UTF-8")
        connection_string = 'oracle://{DB_USERNAME}:{DB_PASSWORD}@{dsnStr}'.format(
            DB_USERNAME=DB_USERNAME,
            DB_PASSWORD=DB_PASSWORD,
            dsnStr=dsnStr
        )
        print("Connection_string:", connection_string)
        engine = create_engine(connection_string,
                               arraysize=200000,
                               convert_unicode=False,
                               pool_recycle=10,
                               pool_size=50,
                               echo=True)
        print("OK OK OK", datetime.now())
        result = engine.execute(get_query(query))
        print('data_frame', datetime.now())
        df = pd.DataFrame(result)
        # df = pd.read_sql(get_query(query), con=conn)
        return df
    except Exception as e:
        raise e
