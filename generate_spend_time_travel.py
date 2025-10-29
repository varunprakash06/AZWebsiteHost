import csv
import random
import uuid
import hashlib
from datetime import datetime, timedelta

# ------------------------------
# Configuration
# ------------------------------
random.seed(42)

N_AFTER = 2000
N_INSERT = 500
N_CARRY = N_AFTER - N_INSERT         # 1500 carried to AFTER
N_UPDATE = 600
N_UNCHANGED = N_CARRY - N_UPDATE     # 900 unchanged
N_DELETE = 300                       # present only in BEFORE

# Pools and dictionaries derived from your sample
SOURCES = ["SAP", "Oracle", "NetSuite", "Workday", "Custom"]
SPEND_CHANNEL_NM_POOL = ["Digital", "Traditional", "Hybrid", "Direct", "Partner"]
SPEND_CHANNEL_CD_POOL = ["DIG", "TRD", "HYB", "DIR"]
STATUS_CD_POOL = ["APP", "APPROVED", "PRC", "PROCESSING", "PND", "PENDING", "REJ", "REJECTED"]
CURRENCY_POOL = ["USD", "EUR", "GBP", "CAD", "AUD"]
BRANDS = ["BRAND_A", "BRAND_B", "BRAND_C", "GENERIC"]
REGIONS = ["NA", "EU", "APAC", "LATAM", "MEA"]
ACTIVITY_TYPES = ["Seminar", "Workshop", "Conference", "Training", "Meeting"]
ACT_CATS = ["Medical Education", "Professional Development", "Leadership", "Skills Training", "Continuing Education"]
COUNTRIES = ["US", "UK", "DE", "FR", "CA", "AU"]
ROLES = ["Speaker", "Moderator", "Observer", "Participant", "Vendor", "Institution"]
EVENT_DESCS = ["Annual Meeting", "Quarterly Workshop", "Special Conference", "Monthly Training", "Monthly Workshop", "Quarterly Seminar"]
SHORT_NOTES_POOL = [
    "Standard registration and expenses",
    "Data validation warning",
    "Processing notes: Priority",
    "Processing notes: Standard",
    "Processing notes: Expedited"
]
LONG_NOTES_POOL = [
    "Professional development and training event",
    "Vendor services for seminar",
    "Additional notes for workshop",
    "Vendor services for conference"
]
VENDORS = [
    "Global Services", "Professional Partners", "Premier Solutions", "Elite Partners",
    "International Solutions", "Global Events", "Professional Solutions", "Premier Events", "Elite Services"
]
ENTITY_TYPES = ["Supplier", "Consultant", "Service Provider", "Contractor"]
ITEM_SRC_SYS_NM_POOL = ["SAP", "Oracle", "NetSuite", "Workday"]
SPEND_STATUS_DESC_MAP = {
    "APP": "APPROVED",
    "APPROVED": "APPROVED",
    "PND": "PENDING",
    "PENDING": "PENDING",
    "PRC": "PROCESSING",
    "REJ": "REJECTED",
    "REJECTED": "REJECTED"
}

# Columns for output (wide subset reflecting your schema)
COLS = [
    "SPND_ID","EXPENSE_ID","PARENT_EXPENSE_ID",
    "AZ_CUST_ID","SRC_CUST_ID","CUST_SRC_SYS","SOURCE_SYSTEM_NM",
    "REC_SYS_CD","RECORD_COMPANY_CD","SPEND_TXNMY_ID","AZ_PROD_ID",
    "SRC_PROD_ID","NDC11_PROD_ID","BRAND_CD","SPEND_CHANNEL_CD","SPEND_CHANNEL_NM",
    "SPEND_STATUS_CD","SPEND_STATUS_DESC","CURRENCY_CD","SPEND_DATE",
    "AUTHOR_ID","AUTHOR_EMAIL","AUTHOR_CHANNEL","AUTHOR_FIRST_NM","AUTHOR_LAST_NM","AUTHOR_MIDDLE_NM","AUTHOR_FULL_NM","AUTHOR_AZ_EMPL_ID",
    "FULFILMENT_VENDOR_ID","FULFILMENT_VENDOR_NM","FULFILMENT_VENDOR_PRID","FULFILMENT_VENDOR_ADDR_LINE_1","FULFILMENT_VENDOR_ADDR_LINE_2","FULFILMENT_VENDOR_ADDR_LINE_3","FULFILMENT_VENDOR_CITY","FULFILMENT_VENDOR_STATE","FULFILMENT_VENDOR_ZIP","FULFILMENT_VENDOR_ZIP_EXTN","FULFILMENT_VENDOR_COUNTRY",
    "SUBMIT_DT","START_DT","END_DT","PAYMENT_DT",
    "FIELD_ACTIVITY_CD","EVENT_DESC","SHORT_NOTES","LONG_NOTES","PAYMENT_METHOD_SUPPLEMENT",
    "ATENDEE_ROLE","SPEAKER_NM","EXHIBIT_NM","EXPENSE_PAYEE_NM","EXPENSE_REPORT_NUM","PO_NUM","EXPENSE_AVG_PER_GUEST",
    "PROMO_ITEM_QTY","FMV","SPEND_AMOUNT","SPD_AMT_PRE_ALLOC","SPD_AMT_TOTAL_PARTICIPANTS","SPD_AMT_TOTAL_LIC_HCPS","SPD_AMT_TOTAL_PRESCRIBERS",
    "TOTAL_HCP","TOTAL_PARTICIPANTS","TOTAL_AZEMPLOYEES","TOTAL_HCPSTAFF","TOTAL_SPEAKERS","TOTAL_NO_OF_NOSHOWS","TOTAL_LICENCED_HCP",
    "ROW_LOAD_DT","ROW_UPDT_DT","SRC_SYS_ACTIVITY_NUM","TOTAL_PRESCRIBER",
    "SHIPPED_QTY","REQUESTED_QTY",
    "STD_GRP_NUM","STD_LEGAL_COST","ITEM_ID","UNIT_OF_MEASURE",
    "ACTIVITY_TYPE_CD","ACTIVITY_TYPE_NM","ACT_CATEGORY_NM","ACT_SUBCATEGORY_NM",
    "REGION_CD","ITEM_SRC_SYS_NM","PROGRAM_ID","PROJECT_NUMBER",
    "SUPPLIER_INVOICE_NUMBER","ALLOCATION_NUMBER","SITE_ID","PI_ID","ENTITY_PAID_ID",
    "STUDY_CODE","STUDY_NAME","COR_IND",
    "ORG_EXPENSE_ID","PERSN_TYP_ID","ORDERID",
    "SOURCE_SYSTEM_PROGRAM_ID","AZER_SHORT_NOTES",
    "FIELD_ACTIVITY_CODE","PREP_ID","CREATEDONDATE_ORDERDATE","CNCR_EXPENSE_REPORT_NUMBER",
    "DER_SUPP_INVCE_NBR","DER_WBS_CODE","DER_SRC_SYS_PROG_ID_NUM","DER_SHIPPED_DATE",
    "ACTIVITY_CODE","COMMENTS","CONTRACT_NBR","CONTROLLING_AREA","DESC_OF_MEETING_EVENT","DOC_TYPE_CODE","DOCUMENT_PAY_AMOUNT","ENGAGEMENT_OWNER",
    "EUR_SPEND_AMOUNT","FISCAL_YEAR","INTERNAL_ORDER","ORGNL_TXNMY_ID","POSTING_KEY",
    "RECIPIENT_COUNTRY","RPTBL_SPEND_AMOUNT","RPTBL_SPEND_CURRENCY_CD","RPTBLTY_IND","SAP_DOC_NBR","SOURCE_CURRENCY","USD_SPEND_AMOUNT",
    "VENDOR_NUMBER","VENDOR_TYPE","SPD_CHN_CD","SORT2","EVENT_ID","PAYMENT_TO","SERVICE_TYPE","EVENT_TYPE",
    "ERR_MSG","SAP_DOC_NO","NATURE_OF_PAYMENT","TRANSACTION_ID","TEXT1","TEXT2","TEXT3",
    "SAP_VENDOR_NAME1","SAP_VENDOR_NAME2","SAP_VENDOR_COUNTRY_CODE","BIOLOGICAL_IND",
    "LAST_MODIFIED_DT","EXCLUSION_IND","NL_ACTIVITY_TYPE","CONTRACT_DT","DEPARTMENT","THERAPEUTIC_AREA","REGION_NAME",
    "RECIPIENT_TYPE","RECIPIENT_IDENTIFIER","RECIPIENT_NAME","RECIPIENT_COUNTRY_CD"
]

# Null probabilities per column (adjust as needed; keys kept non-null)
NULL_RATES = {
    "AUTHOR_EMAIL": 0.05,
    "AUTHOR_FULL_NM": 0.03,
    "FULFILMENT_VENDOR_NM": 0.04,
    "FULFILMENT_VENDOR_COUNTRY": 0.06,
    "SPEND_CHANNEL_CD": 0.03,
    "SPEND_CHANNEL_NM": 0.02,
    "SPEND_STATUS_DESC": 0.02,
    "REGION_CD": 0.03,
    "ACTIVITY_TYPE_NM": 0.05,
    "ACT_CATEGORY_NM": 0.05,
    "RECIPIENT_COUNTRY_CD": 0.05,
    "ATENDEE_ROLE": 0.08,
    "EVENT_DESC": 0.04,
    "SHORT_NOTES": 0.12,
    "LONG_NOTES": 0.15,
    "SPEND_AMOUNT": 0.02,
    "USD_SPEND_AMOUNT": 0.05,
    "EUR_SPEND_AMOUNT": 0.08,
    "SPEND_DATE": 0.01,
    "ROW_LOAD_DT": 0.01,
    "ROW_UPDT_DT": 0.01,
    "PAYMENT_DT": 0.05,
    "SUBMIT_DT": 0.05,
    "START_DT": 0.05,
    "END_DT": 0.05,
    "SPEAKER_NM": 0.10,
    "EXPENSE_REPORT_NUM": 0.03,
    "PO_NUM": 0.03,
    "COMMENTS": 0.10,
    "ERR_MSG": 0.20
}

# ------------------------------
# Helpers
# ------------------------------
def rand_date(start_year=2019, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

def mk_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:16].upper()}"

def mk_email():
    fn = random.choice(["mike","jane","emma","john","lisa","robert","david","taylor","alex","morgan","riley","casey","avery","jordan"])
    ln = random.choice(["smith","jones","brown","williams","garcia","anderson","martinez","thompson","clark","wilson"])
    dom = random.choice(["techcorp.com","medisoft.com","bioinnovate.com","healthtech.com"])
    return f"{fn}.{ln}@{dom}"

def mk_name():
    fn = random.choice(["Mike","Jane","Emma","John","Lisa","Robert","David","Taylor","Alex","Morgan","Riley","Casey","Avery","Jordan"])
    ln = random.choice(["Smith","Jones","Brown","Williams","Garcia","Anderson","Martinez","Thompson","Clark","Wilson"])
    return f"{fn} {ln}"

def mk_vendor():
    return random.choice(VENDORS)

def mk_country():
    return random.choice(COUNTRIES)

def mk_money(base=1500.0, spread=30000.0):
    val = round(base + random.random() * spread, 2)
    usd = round(val * 1.05, 2)
    eur = round(val * 0.92, 2)
    return val, usd, eur

def status_desc_from_cd(cd):
    return SPEND_STATUS_DESC_MAP.get(cd, "PROCESSING")

def maybe_null(row):
    for c, p in NULL_RATES.items():
        if c in row and random.random() < p:
            row[c] = None
    return row

def hash_row(row):
    s = "|".join("" if row.get(c) is None else str(row.get(c, "")) for c in COLS)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

# ------------------------------
# Row construction
# ------------------------------
def make_row(spnd_id=None, exp_id=None, parent_id=None):
    spnd_id = spnd_id or mk_id("SPND")
    exp_id = exp_id or mk_id("EXP")
    parent_id = parent_id or mk_id("PEXP")
    spend_amt, usd_amt, eur_amt = mk_money()
    status_cd = random.choice(STATUS_CD_POOL)
    status_desc = status_desc_from_cd(status_cd)
    currency = random.choice(CURRENCY_POOL)
    channel_cd = random.choice(SPEND_CHANNEL_CD_POOL)
    channel_nm = random.choice(SPEND_CHANNEL_NM_POOL)
    act_type = random.choice(ACTIVITY_TYPES)
    act_cat = random.choice(ACT_CATS)
    region = random.choice(REGIONS)
    country_cd = mk_country()

    # Dates with logical ordering
    submit_dt = rand_date()
    start_dt_dt = datetime.strptime(rand_date(), "%Y-%m-%d")
    end_dt_dt = start_dt_dt + timedelta(days=random.randint(0, 10))
    payment_dt_dt = end_dt_dt + timedelta(days=random.randint(0, 10))

    row = {
        "SPND_ID": spnd_id,
        "EXPENSE_ID": exp_id,
        "PARENT_EXPENSE_ID": parent_id,
        "AZ_CUST_ID": mk_id("CUST"),
        "SRC_CUST_ID": mk_id("SRC"),
        "CUST_SRC_SYS": random.choice(SOURCES),
        "SOURCE_SYSTEM_NM": random.choice(["Event 2024","Training 2023","Conference 2021","Meeting 2019","Workshop 2025"]),
        "REC_SYS_CD": random.choice(["REC01","REC02","REC03"]),
        "RECORD_COMPANY_CD": random.choice(["TechCorp","BioInnovate","MediSoft"]),
        "SPEND_TXNMY_ID": random.randint(1000, 9999),
        "AZ_PROD_ID": random.randint(100000000, 999999999),
        "SRC_PROD_ID": random.randint(1000, 9999),
        "NDC11_PROD_ID": random.choice([str(random.randint(10000000000, 99999999999)),-1]),
        "BRAND_CD": random.choice(BRANDS),
        "SPEND_CHANNEL_CD": channel_cd,
        "SPEND_CHANNEL_NM": channel_nm,
        "SPEND_STATUS_CD": status_cd,
        "SPEND_STATUS_DESC": status_desc,
        "CURRENCY_CD": currency,
        "SPEND_DATE": rand_date(),

        "AUTHOR_ID": mk_id("AUTH"),
        "AUTHOR_EMAIL": mk_email(),
        "AUTHOR_CHANNEL": random.choice(["Direct","Hybrid","Partner","Digital","Traditional"]),
        "AUTHOR_FIRST_NM": random.choice(["Mike","Jane","Emma","John","Lisa","Robert","David","Taylor","Alex","Morgan","Riley","Casey","Avery","Jordan"]),
        "AUTHOR_LAST_NM": random.choice(["Smith","Jones","Brown","Williams","Garcia","Anderson","Martinez","Thompson","Clark","Wilson"]),
        "AUTHOR_MIDDLE_NM": random.choice(["A","B","C","D","", None]),
        "AUTHOR_FULL_NM": mk_name(),
        "AUTHOR_AZ_EMPL_ID": mk_id("EMP"),

        "FULFILMENT_VENDOR_ID": mk_id("VEND"),
        "FULFILMENT_VENDOR_NM": mk_vendor(),
        "FULFILMENT_VENDOR_PRID": mk_id("VPRID"),
        "FULFILMENT_VENDOR_ADDR_LINE_1": f"{random.randint(100,9999)} Oak St",
        "FULFILMENT_VENDOR_ADDR_LINE_2": random.choice(["Suite 215","Suite 619","", None]),
        "FULFILMENT_VENDOR_ADDR_LINE_3": random.choice(["", None]),
        "FULFILMENT_VENDOR_CITY": random.choice(["Phoenix","Los Angeles","Chicago","New York","Houston","Philadelphia"]),
        "FULFILMENT_VENDOR_STATE": random.choice(["AZ","CA","NY","TX","IL","PA","DE","FR","UK"]),
        "FULFILMENT_VENDOR_ZIP": str(random.randint(10000,99999)),
        "FULFILMENT_VENDOR_ZIP_EXTN": random.choice(["", str(random.randint(1000,9999))]),
        "FULFILMENT_VENDOR_COUNTRY": random.choice(["US","UK","DE","FR","CA","AU"]),

        "SUBMIT_DT": submit_dt,
        "START_DT": start_dt_dt.strftime("%Y-%m-%d"),
        "END_DT": end_dt_dt.strftime("%Y-%m-%d"),
        "PAYMENT_DT": payment_dt_dt.strftime("%Y-%m-%d"),

        "FIELD_ACTIVITY_CD": random.choice(["CONF","SEMI","WORK","MEET","TRAIN"]),
        "EVENT_DESC": random.choice(EVENT_DESCS),
        "SHORT_NOTES": random.choice(SHORT_NOTES_POOL),
        "LONG_NOTES": random.choice(LONG_NOTES_POOL),
        "PAYMENT_METHOD_SUPPLEMENT": random.choice(["Check","ACH","Wire Transfer","Credit Card","PO","Contract","Standard","Blanket"]),

        "ATENDEE_ROLE": random.choice(ROLES),
        "SPEAKER_NM": random.choice([mk_name(),"Dr. "+random.choice(["Alex","Taylor","Morgan","Riley","Casey","Avery"])+" "+random.choice(["Smith","Jones","Brown","Wilson","Clark","Martinez"]), None]),
        "EXHIBIT_NM": random.choice(["Booth "+str(random.randint(100,999)),"", None]),
        "EXPENSE_PAYEE_NM": mk_name(),
        "EXPENSE_REPORT_NUM": mk_id("RPT"),
        "PO_NUM": mk_id("PO"),
        "EXPENSE_AVG_PER_GUEST": round(random.random()*500,2),

        "PROMO_ITEM_QTY": random.randint(0,50000),
        "FMV": round(random.random()*500,2),
        "SPEND_AMOUNT": spend_amt,
        "SPD_AMT_PRE_ALLOC": spend_amt,
        "SPD_AMT_TOTAL_PARTICIPANTS": round(random.random()*20000,2),
        "SPD_AMT_TOTAL_LIC_HCPS": round(random.random()*20000,2),
        "SPD_AMT_TOTAL_PRESCRIBERS": round(random.random()*20000,2),

        "TOTAL_HCP": random.randint(0,50),
        "TOTAL_PARTICIPANTS": random.randint(0,100),
        "TOTAL_AZEMPLOYEES": random.randint(0,80),
        "TOTAL_HCPSTAFF": random.randint(0,30),
        "TOTAL_SPEAKERS": random.randint(0,15),
        "TOTAL_NO_OF_NOSHOWS": random.randint(0,80),
        "TOTAL_LICENCED_HCP": random.randint(0,50),

        "ROW_LOAD_DT": rand_date(),
        "ROW_UPDT_DT": rand_date(),
        "SRC_SYS_ACTIVITY_NUM": mk_id("ACT"),
        "TOTAL_PRESCRIBER": random.randint(0,100),

        "SHIPPED_QTY": random.randint(0,5000),
        "REQUESTED_QTY": random.randint(0,5000),

        "STD_GRP_NUM": random.randint(0,10000),
        "STD_LEGAL_COST": round(random.random()*5000,2),
        "ITEM_ID": mk_id("ITM"),
        "UNIT_OF_MEASURE": random.choice(["EA","KG","LB","BOX","CASE"]),

        "ACTIVITY_TYPE_CD": random.choice(["ACC","TRV","SPK","MAT","REG","INV","PO"]),
        "ACTIVITY_TYPE_NM": act_type,
        "ACT_CATEGORY_NM": act_cat,
        "ACT_SUBCATEGORY_NM": random.choice(["Skills Training","Continuing Education","Product Training","Leadership","Medical Education"]),

        "REGION_CD": region,
        "ITEM_SRC_SYS_NM": random.choice(ITEM_SRC_SYS_NM_POOL),
        "PROGRAM_ID": mk_id("PROG"),
        "PROJECT_NUMBER": mk_id("PROJ"),

        "SUPPLIER_INVOICE_NUMBER": mk_id("INV"),
        "ALLOCATION_NUMBER": mk_id("ALLOC"),
        "SITE_ID": mk_id("SITE"),
        "PI_ID": mk_id("PI"),
        "ENTITY_PAID_ID": mk_id("ENT"),

        "STUDY_CODE": mk_id("STUDY"),
        "STUDY_NAME": random.choice(["Clinical Study 803","Clinical Study 491","Clinical Study 631","Clinical Study 116","Clinical Study 317","Clinical Study 925"]),
        "COR_IND": random.choice(["Y","N"]),

        "ORG_EXPENSE_ID": mk_id("ORGEXP"),
        "PERSN_TYP_ID": str(random.randint(1,10)),
        "ORDERID": mk_id("ORD"),

        "SOURCE_SYSTEM_PROGRAM_ID": mk_id("SYSPROG"),
        "AZER_SHORT_NOTES": random.choice(["Event registration and related expenses","Training Fees","Materials","Professional Services"]),

        "FIELD_ACTIVITY_CODE": random.choice(["TRV","ACC","SPK","REG","MAT","INV","PO"]),
        "PREP_ID": mk_id("PREP"),
        "CREATEDONDATE_ORDERDATE": rand_date(),
        "CNCR_EXPENSE_REPORT_NUMBER": mk_id("CERPO"),

        "DER_SUPP_INVCE_NBR": mk_id("DSIN"),
        "DER_WBS_CODE": mk_id("WBS"),
        "DER_SRC_SYS_PROG_ID_NUM": str(random.randint(100000,999999)),
        "DER_SHIPPED_DATE": rand_date(),

        "ACTIVITY_CODE": random.choice(["AC001","AC002","AC003","AC004"]),
        "COMMENTS": random.choice(["Processing notes: Standard","Processing notes: Expedited","Processing notes: Priority",""]),
        "CONTRACT_NBR": mk_id("CNT"),
        "CONTROLLING_AREA": str(random.randint(1000,9999)),
        "DESC_OF_MEETING_EVENT": random.choice(["Meeting - Cardiology Focus","Workshop - Diabetes Focus","Seminar - Oncology Focus","Conference - Respiratory Focus"]),
        "DOC_TYPE_CODE": random.choice(["REC","PAY","INV","PO","SPK"]),
        "DOCUMENT_PAY_AMOUNT": round(random.random()*50000,2),
        "ENGAGEMENT_OWNER": mk_name(),

        "EUR_SPEND_AMOUNT": eur_amt,
        "FISCAL_YEAR": random.choice([2019,2020,2021,2022,2023,2024,2025]),
        "INTERNAL_ORDER": random.randint(1000,9999),
        "ORGNL_TXNMY_ID": random.randint(1000,9999),
        "POSTING_KEY": random.randint(1000,9999),

        "RECIPIENT_COUNTRY": mk_country(),
        "RPTBL_SPEND_AMOUNT": spend_amt,
        "RPTBL_SPEND_CURRENCY_CD": random.choice(["USD","EUR","GBP","CAD","AUD"]),
        "RPTBLTY_IND": random.choice(["Y","N"]),
        "SAP_DOC_NBR": mk_id("SAPDOC"),
        "SOURCE_CURRENCY": random.choice(["USD","EUR","GBP","CAD","AUD"]),
        "USD_SPEND_AMOUNT": usd_amt,

        "VENDOR_NUMBER": mk_id("VEN"),
        "VENDOR_TYPE": random.choice(ENTITY_TYPES),
        "SPD_CHN_CD": random.choice(["DIG","TRD","HYB","DIR"]),
        "SORT2": random.choice(["A","B","C","D"]),
        "EVENT_ID": mk_id("EVT"),
        "PAYMENT_TO": random.choice(["Institution","Vendor","Individual"]),
        "SERVICE_TYPE": random.choice(["Training","Workshop","Conference","Meeting","Materials","Consulting"]),
        "EVENT_TYPE": random.choice(["Seminar","Training","Conference","Workshop","Meeting"]),

        "ERR_MSG": random.choice(["","Data validation warning",""]),
        "SAP_DOC_NO": mk_id("SAPDOCNO"),
        "NATURE_OF_PAYMENT": random.choice(["Registration","Professional Services","Materials","Training Fees"]),
        "TRANSACTION_ID": str(uuid.uuid4()),
        "TEXT1": "Additional notes for training",
        "TEXT2": "Department: " + random.choice(["IT","HR","Sales","Operations","Finance","Research","Marketing"]),
        "TEXT3": "Therapeutic Area: " + random.choice(["Oncology","Cardiology","Respiratory","Neurology","Diabetes"]),

        "SAP_VENDOR_NAME1": random.choice(["International Partners","Global Services","Professional Partners","International Solutions","Professional Solutions"]),
        "SAP_VENDOR_NAME2": random.choice(["Inc","LLC","Global","Partners","Solutions"]),
        "SAP_VENDOR_COUNTRY_CODE": mk_country(),
        "BIOLOGICAL_IND": random.choice(["Y","N"]),

        "LAST_MODIFIED_DT": rand_date(),
        "EXCLUSION_IND": random.choice(["Y","N"]),
        "NL_ACTIVITY_TYPE": random.choice(["Training","Meeting","Conference","Workshop"]),
        "CONTRACT_DT": rand_date(),
        "DEPARTMENT": random.choice(["IT","HR","Sales","Operations","Finance","Research","Marketing"]),
        "THERAPEUTIC_AREA": random.choice(["Oncology","Cardiology","Respiratory","Neurology","Diabetes"]),
        "REGION_NAME": random.choice(["North America","Europe","Asia Pacific","Latin America","Middle East & Africa"]),

        "RECIPIENT_TYPE": random.choice(["Individual","Organization","Institution","Vendor"]),
        "RECIPIENT_IDENTIFIER": mk_id("RECIP"),
        "RECIPIENT_NAME": mk_name(),
        "RECIPIENT_COUNTRY_CD": country_cd
    }

    row = maybe_null(row)
    return row

def apply_update(r):
    r2 = dict(r)
    drift = 1 + random.uniform(-0.07, 0.07)

    # Monetary fields (guard if null)
    for amt_col in ["SPEND_AMOUNT","USD_SPEND_AMOUNT","EUR_SPEND_AMOUNT",
                    "RPTBL_SPEND_AMOUNT","DOCUMENT_PAY_AMOUNT",
                    "SPD_AMT_PRE_ALLOC","SPD_AMT_TOTAL_PARTICIPANTS","SPD_AMT_TOTAL_LIC_HCPS","SPD_AMT_TOTAL_PRESCRIBERS"]:
        if r2.get(amt_col) is not None:
            r2[amt_col] = round(float(r2[amt_col]) * drift, 2)

    # Status flip
    r2["SPEND_STATUS_CD"] = random.choice(STATUS_CD_POOL)
    r2["SPEND_STATUS_DESC"] = status_desc_from_cd(r2["SPEND_STATUS_CD"])

    # Shift dates only if present
    for dt_col in ["ROW_UPDT_DT","SPEND_DATE","SUBMIT_DT","START_DT","END_DT","PAYMENT_DT","LAST_MODIFIED_DT","CONTRACT_DT","DER_SHIPPED_DATE","CREATEDONDATE_ORDERDATE"]:
        if r2.get(dt_col):
            try:
                dt = datetime.strptime(r2[dt_col], "%Y-%m-%d")
                r2[dt_col] = (dt + timedelta(days=random.randint(1,30))).strftime("%Y-%m-%d")
            except Exception:
                # If parsing fails, leave as-is
                pass

    # Minor text append
    if r2.get("LONG_NOTES"):
        r2["LONG_NOTES"] = (r2["LONG_NOTES"] + " | Restated per policy").strip(" |")
    else:
        r2["LONG_NOTES"] = "Restated per policy"

    # Occasional categorical change
    r2["SPEND_CHANNEL_NM"] = random.choice(SPEND_CHANNEL_NM_POOL)
    r2["SPEND_CHANNEL_CD"] = random.choice(SPEND_CHANNEL_CD_POOL)
    r2["ACTIVITY_TYPE_NM"] = random.choice(ACTIVITY_TYPES)
    r2["ACT_CATEGORY_NM"] = random.choice(ACT_CATS)
    r2["REGION_CD"] = random.choice(REGIONS)

    r2 = maybe_null(r2)
    return r2

# ------------------------------
# Build BEFORE and AFTER
# ------------------------------
before_rows = []
carry_rows = []
for _ in range(N_CARRY):
    r = make_row()
    carry_rows.append(r)
    before_rows.append(r)

for _ in range(N_DELETE):
    before_rows.append(make_row())  # will be deleted (not carried)

# AFTER: unchanged + updated + inserts
after_rows = []

# Unchanged subset
unchanged = random.sample(carry_rows, N_UNCHANGED)
after_rows.extend(unchanged)

# Updated subset
remaining_for_update = [r for r in carry_rows if r not in unchanged]
updated = random.sample(remaining_for_update, N_UPDATE)
after_rows.extend(apply_update(r) for r in updated)

# Inserts
for _ in range(N_INSERT):
    after_rows.append(make_row())

# ------------------------------
# Changes ledger
# ------------------------------
def idx(rows):
    return {r["SPND_ID"]: r for r in rows}

b_idx = idx(before_rows)
a_idx = idx(after_rows)

changes = []
all_keys = set(b_idx.keys()) | set(a_idx.keys())
for k in all_keys:
    b = b_idx.get(k)
    a = a_idx.get(k)
    if b and not a:
        changes.append({"SPND_ID":k,"CHANGE_TYPE":"DELETE","BEFORE_HASH":hash_row(b),"AFTER_HASH":"","CHANGED_COLUMNS":""})
    elif a and not b:
        changes.append({"SPND_ID":k,"CHANGE_TYPE":"INSERT","BEFORE_HASH":"","AFTER_HASH":hash_row(a),"CHANGED_COLUMNS":""})
    else:
        hb, ha = hash_row(b), hash_row(a)
        if hb == ha:
            changes.append({"SPND_ID":k,"CHANGE_TYPE":"UNCHANGED","BEFORE_HASH":hb,"AFTER_HASH":ha,"CHANGED_COLUMNS":""})
        else:
            changed_cols = [c for c in COLS if ("" if b.get(c) is None else str(b.get(c,""))) != ("" if a.get(c) is None else str(a.get(c,"")))]
            changes.append({"SPND_ID":k,"CHANGE_TYPE":"UPDATE","BEFORE_HASH":hb,"AFTER_HASH":ha,"CHANGED_COLUMNS":",".join(changed_cols)})

# ------------------------------
# Write CSVs
# ------------------------------
with open("spend_before.csv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=COLS)
    w.writeheader()
    for r in before_rows:
        w.writerow(r)

with open("spend_after.csv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=COLS)
    w.writeheader()
    for r in after_rows:
        w.writerow(r)

with open("spend_changes.csv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["SPND_ID","CHANGE_TYPE","BEFORE_HASH","AFTER_HASH","CHANGED_COLUMNS"])
    w.writeheader()
    for c in changes:
        w.writerow(c)

print("Generated spend_before.csv (~{} rows), spend_after.csv ({} rows), spend_changes.csv (~{} rows)".format(
    len(before_rows), len(after_rows), len(changes)
))
