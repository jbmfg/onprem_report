import json
import trino
import xlsxwriter
from collections import defaultdict
from sqlite_connector import sqlite_db
from tesseract_connector import tesseract_connection
from datetime import datetime

class report_data(object):

    def __init__(self):
        self.customers = {}
        self.nulls = defaultdict(list)
        self.sfdb = tesseract_connection()
        self.db = sqlite_db("onprem_products.db")
        self.inst_ids = self.get_initial_list()
        self.act_dict = self.get_account_translation()

    def get_initial_list(self):
        query = f"""
        select i.installation_18_digit_id__c
        from edw_tesseract.sbu_ref_sbusfdc.installation__c i
        left join edw_tesseract.sbu_ref_sbusfdc.account a on i.account__c = a.id
        where a.cs_tier__c in ('Low', 'Medium', 'High', 'Holding')
        and i.product_group__c in ('Cb Protection', 'Cb Response', 'Cb Response Cloud')
        and i.installation_type__c in ('Perpetual', 'Subscription')
        and i.install_type__c in ('Partner', 'MSSP - Cb Protection', 'IR - Carbon Black', 'Other', 'General Availability', 'Bit9 Deployment', 'Initial purchase')
        """
        data = self.sfdb.execute(query)

        # Insert into db just the inst_ids
        fields = ["inst_id"]
        self.db.insert("installations", fields, data)

        # Return a string that can be used in subsequent queries
        data = [i[0] for i in data]
        data = "'" + "', '".join(data) + "'"
        return data

    def get_installation_info(self):
        query = f"""
        select i.installation_18_digit_id__c,
        i.licenses_purchased__c,
        i.normalized_host_count__c,
        i.last_contact__c,
        i.account__c,
        i.product_group__c
        from edw_tesseract.sbu_ref_sbusfdc.installation__c i
        where i.installation_18_digit_id__c in ({self.inst_ids})
        """
        data = self.sfdb.execute(query)
        fields = ("inst_id", "licenses_purchased", "normalized_host_count", "last_contact", "acct_id", "product")
        self.db.update("installations", fields, data)

    def get_account_translation(self):
        query = f"""
        select i.account__c, i.id from
        edw_tesseract.sbu_ref_sbusfdc.installation__c i
        where i.installation_18_digit_id__c in ({self.inst_ids})
        """
        data = self.sfdb.execute(query)
        act_dict = defaultdict(list)
        for act_id, inst_id in data:
            act_dict[act_id].append(inst_id)
        return act_dict

    def get_account_info(self):
        accts = "'" + "', '".join(self.act_dict.keys()) + "'"
        query = f"""
        select
        a.account_id_18_digits__c,
        a.cs_tier__c,
        a.arr__c,
        a.name,
        a.GS_CSM_Meter_Score__c,
        a.csm_meter_comments__c,
        a.GS_Overall_Score__c,
        a.gs_adoption_comments__c,
        csm.name,
        cse.name
        from edw_tesseract.sbu_ref_sbusfdc.account a
        left join edw_tesseract.sbu_ref_sbusfdc.user_sbu csm on a.Assigned_CP__c = csm.Id
        left join edw_tesseract.sbu_ref_sbusfdc.user_sbu cse on a.Customer_Success_Engineer__c = cse.Id
        where a.account_id_18_digits__c in ({accts})
        """
        data = self.sfdb.execute(query)
        fields = ["acct_id", "tier", "arr", "account_name", "csm_score", "csm_comments"]
        fields += ["gs_score", "adoption_comments", "csm", "cse"]
        self.db.insert("accounts", fields, data)

    def get_opportunity_info(self):
        accts = "'" + "', '".join(self.act_dict.keys()) + "'"
        query = f"""
        select o.id,
        o.accountid,
        o.acv_amount__c,
        o.cb_forecast__c,
        o.closeDate,
        o.product_family__c
        from edw_tesseract.sbu_ref_sbusfdc.opportunity o
        where o.accountid in ({accts})
        and o.closedate > CURRENT_DATE
        and o.type like '%Renewal%'
        """
        data = self.sfdb.execute(query)
        fields = ("opp_id", "acct_id", "acv", "forecast", "close_date", "type")
        self.db.insert("opportunities", fields, data)

    def get_subscription_info(self):
        accts = "'" + "', '".join(self.act_dict.keys()) + "'"
        query = f"""
        select account__c,
        arr__c,
        end_date__c,
        id,
        product_description__c,
        product__c,
        product_group__c,
        quantity__c,
        subscription_term__c,
        tcv__c
        from edw_tesseract.sbu_ref_sbusfdc.bit9_subscriptions__c s
        where active_subscription__c = true
        and account__c in ({accts})
        """
        data = self.sfdb.execute(query)
        fields = ["acct_id", "arr", "end_date", "sub_id"]
        fields += ["description", "product_id", "product"]
        fields += ["quantity", "sub_term", "tcv"]
        self.db.insert("subscriptions", fields, data)

    def renewal_quarter(self):
        def lookup_q(opp_date):
            formatstr = "%Y-%m-%d"
            if isinstance(opp_date, str):
                opp_date = datetime.strptime(opp_date, formatstr)
            opp_date = datetime(opp_date.year, opp_date.month, opp_date.day)
            qdict = {
                "2021": {
                    "Q1": ["2020-02-01", "2020-04-30"],
                    "Q2": ["2020-05-01", "2020-07-30"],
                    "Q3": ["2020-07-31", "2020-10-29"],
                    "Q4": ["2020-10-30", "2021-01-28"]
                },
                "2022": {
                    "Q1": ["2021-01-29", "2021-04-29"],
                    "Q2": ["2021-04-30", "2021-07-29"],
                    "Q3": ["2021-07-30", "2021-10-28"],
                    "Q4": ["2021-10-29", "2022-01-27"]
                },
                "2023": {
                    "Q1": ["2022-01-28", "2022-04-28"],
                    "Q2": ["2022-04-29", "2022-07-28"],
                    "Q3": ["2022-07-29", "2022-10-27"],
                    "Q4": ["2022-10-28", "2023-01-26"]
                },
                "2024": {
                    "Q1": ["2023-01-27", "2023-04-27"],
                    "Q2": ["2023-04-28", "2023-07-27"],
                    "Q3": ["2023-07-28", "2023-10-26"],
                    "Q4": ["2023-10-27", "2024-01-25"]
                },
                "2025": {
                    "Q1": ["2024-01-26", "2024-04-25"],
                    "Q2": ["2024-04-26", "2024-07-25"],
                    "Q3": ["2024-07-26", "2024-10-24"],
                    "Q4": ["2024-10-25", "2025-01-23"]
                },
                "2026": {
                    "Q1": ["2025-01-24", "2025-04-24"],
                    "Q2": ["2025-04-25", "2025-07-24"],
                    "Q3": ["2025-07-25", "2025-10-23"],
                    "Q4": ["2025-10-24", "2026-01-22"]}
                }
            opp_year = opp_date.year
            finding = "Unknown"
            for year in qdict:
                for q in qdict[year]:
                    start = datetime.strptime(qdict[year][q][0], formatstr)
                    end = datetime.strptime(qdict[year][q][1], formatstr)
                    if start <= opp_date <= end:
                        finding = f"{year} {q}"
            return finding

        query = "select opp_id, close_date from opportunities;"
        data = self.db.execute(query)
        data = [[i[0], lookup_q(i[1])] for i in data]
        fields = ("opp_id", "renewal_qt")
        self.db.update("opportunities", fields, data)

    def deployment_percentage(self):
        query = "select inst_id, normalized_host_count, licenses_purchased from installations;"
        data = self.db.execute(query)
        deployments = []
        for i in data:
            if i[1] == 0:
                deployment = "0%"
            elif i[1] is not None and i[2]:
                deployment = f"{round(i[1]/i[2] * 100, 2)}%"
            else:
                continue
            deployments.append([i[0], deployment])
        for i in deployments: print(i)
        fields = ("inst_id", "deployment")
        self.db.update("installations", fields, deployments)

    def product_family(self):
        query = "select distinct type from opportunities;"
        data = [i[0] for i in self.db.execute(query)]
        products = set([i for prods in data for i in prods.split(";")])
        for i in products: print(i)

def table_creations():
    db = sqlite_db("onprem_products.db")
    for table in ("installations", "accounts", "opportunities", "subscriptions"):
        db.execute(f"drop table if exists {table};")

    # Installations
    query = """
    CREATE table installations(
    inst_id TEXT PRIMARY KEY,
    licenses_purchased INTEGER DEFAULT Null CHECK (typeof(licenses_purchased) in ('integer', Null)),
    normalized_host_count INTEGER DEFAULT Null CHECK (typeof(normalized_host_count) in ('integer', Null)),
    deployment TEXT DEFAULT Null,
    last_contact STRING,
    acct_id STRING,
    product STRING
    );
    """
    db.execute(query)

    # Accounts
    query = """
    CREATE table accounts(
    acct_id TEXT PRIMARY KEY,
    tier TEXT,
    arr INTEGER DEFAULT 0 CHECK (typeof(arr) in ('integer', Null)),
    account_name TEXT,
    csm_score INTEGER DEFAULT 0 CHECK (typeof(csm_score) in ('integer', Null)),
    csm_comments TEXT,
    gs_score INTEGER DEFAULT 0 CHECK (typeof(gs_score) in ('integer', Null)),
    adoption_comments TEXT,
    csm TEXT,
    cse TEXT
    );
    """
    db.execute(query)

    # Opportunities
    query = """
    CREATE table opportunities(
    opp_id TEXT PRIMARY KEY,
    acct_id TEXT,
    acv INTEGER CHECK (typeof(acv) in ('integer', Null)),
    forecast TEXT,
    close_date TEXT,
    renewal_qt TEXT,
    type TEXT
    );
    """
    db.execute(query)

    # Subscriptions
    query = """
    CREATE table subscriptions(
    acct_id TEXT,
    arr REAL CHECK (typeof(arr) in ('real')),
    end_date TEXT,
    sub_id TEXT,
    description TEXT,
    product_id TEXT,
    product TEXT,
    quantity INTEGER CHECK (typeof(quantity) in ('integer')),
    sub_term INTEGER CHECK (typeof(sub_term) in ('integer')),
    tcv REAL CHECK (typeof(tcv) in ('real'))
    );
    """
    db.execute(query)

def writerows(wb, sheet, data, linkBool=False, setwid=True, col1url=False, bolder=False):
        bold = wb.add_format({"bold": True})
        # first get the length of the longest sting to set column widths
        numCols = len(data[0])
        widest = [10 for _ in range(numCols)]
        if setwid:
            try:
                for i in data:
                    for x in range(len(data[0])):
                        if type(i[x]) == int:
                            pass
                        elif i[x] is None:
                            pass
                        elif not isinstance(i[x], float) and widest[x] < len(i[x].encode("ascii", "ignore")):
                            if len(str(i[x])) > 50:
                                widest[x] = 50
                            else:
                                widest[x] = len(str(i[x])) #+ 4
            except IndexError:
                pass
                # print ("--INFO-- Index Error when setting column widths")
            except TypeError:
                print ("type error")
            except AttributeError:
                # Added check for floats above so this probably isnt needed any more
                print ("\n--INFO-- Can't encode a float\n")

        for x, i in enumerate(widest):
            sheet.set_column(x, x, i)

        # Then write the data
        for r in data:
            for i in r:
                if type(i) == str:
                    i = i.encode("ascii", "ignore")
        counter = 0
        for x, r in enumerate(data):
            counter += 1
            cell = "A" +str(counter)
            if bolder and (data[x-1] == "" or x==0):
                sheet.write_row(cell, r, bold)
            else:
                sheet.write_row(cell, r)
            if col1url:
                if x == 0:
                    pass
                else:
                    sheet_name = f"{x-1}. {r[0]}"[:31]
                    #sheet.write_url(cell, "internal:'{}'!A1".format("{}. {}".format(x,str(r[0]).replace("'","''"))[:31]), string=r[0])
                    sheet.write_url(cell, f"internal:'{sheet_name}'!A1", string=r[0])
            if linkBool:
                sheet.write_url(0, 6, "internal:Master!A1", string="Mastersheet")
        return True

def create_inst_master(db):
    def add_metric(metric):
        pass
    inst_ids = [i[0] for i in db.execute("select inst_id from installations;")]
    rows = {i:{} for i in inst_ids}

    # All of installations
    data = db.execute_dict("select * from installations;")
    for row in data:
        row_tup = list(zip(row.keys(), [i for i in row]))
        inst_id = row_tup.pop(0)[1]
        rows[inst_id].update(dict(row_tup))

    # All of accounts
    query = """
    select i.inst_id, a.*
    from installations i
    left join accounts a on i.acct_id = a.acct_id;
    """
    data = db.execute_dict(query)
    for row in data:
        row_tup = list(zip(row.keys(), [i for i in row]))
        inst_id = row_tup.pop(0)[1]
        rows[inst_id].update(dict(row_tup))

    # Those opportunities that apply *CBLO can be multiple so its omitted + wtf is other?
    lookup = {
        "Cb Cloud": ["CBWL", "CBVM", "CBWS", "CBD", "CBCO", "CBTS", "CBTH"],
        "Cb Response Cloud": ["CBRC"],
        "Cb Protection": ["CBP"],
        "Cb Response": ["CBR"]
    }
    for prod in lookup:
        query = f"""
        select i.inst_id,
        o.close_date,
        o.renewal_qt,
        o.forecast,
        o.acv as opp_acv,
        count(*) as opp_count
        from installations i
        left join opportunities o on i.acct_id = o.acct_id
        where i.product = '{prod}'
        and o.type like '%{", ".join(lookup[prod])}%'
        group by i.inst_id
        having o.ROWID = min(o.ROWID)
        order by o.close_date desc;
        """
        print(query)
        data = db.execute(query)
        for i in data:print(i)

    #print(json.dumps(rows, indent=2))
    return

def write_report():
    db = sqlite_db("onprem_products.db")
    wb = xlsxwriter.Workbook("On-Prem Products_Consumption Report.xlsx")
    lookup = {"Cb Response Cloud": "HEDR", "Cb Protection": "AC", "Cb Response": "EDR"}
    type_lookup = {"Cb Response Cloud": "cbrc", "Cb Protection": "cbp", "Cb Response": "cbr"}
    for product in [i[0] for i in db.execute("select distinct product from installations;")]:
        sheet = wb.add_worksheet(lookup[product])
        query = f"""
        select
        a.account_name as "Account Name",
        a.csm_score as "CSM Score",
        a.csm_comments as "CSM Comments",
        a.gs_score as "GS Score",
        a.tier as "Tier",
        a.arr as "ARR",
        a.csm as "CSM",
        a.cse as "CSE",
        i.licenses_purchased as "Licenses",
        i.normalized_host_count as "Normalized Host Count",
        i.deployment as "Deployment %",
        i.last_contact as "Last Connection",
        case when i.last_contact
        o.forecast as "Forecast",
        o.close_date as "Renewal Date",
        o.renewal_qt as "Renewal Quarter",
        i.inst_id
        from installations i
        left join accounts a on i.account_id = a.acct_id
        left join opportunities o on i.account_id = o.acct_id
        where i.product = '{product}'
        and o.type like '%{type_lookup[product]}%';
        """
        data = db.execute_dict(query)
        data = sorted(data, key=lambda x: x[0])
        header = data[0].keys()
        data.insert(0, header)
        writerows(wb, sheet, data)
    product_groups = [i[0] for i in db.execute("select distinct type from opportunities")]
    products = set([product for products in product_groups for product in products.split(";")])
    for i in products: print(i)
    wb.close()

if __name__ == "__main__":
    create_inst_master(sqlite_db("onprem_products.db"))
    import sys
    sys.exit(1)
    rd = report_data()
    table_creations()
    rd = report_data()
    rd.get_installation_info()
    rd.get_account_info()
    rd.get_opportunity_info()
    rd.get_subscription_info()
    rd.renewal_quarter()
    rd.deployment_percentage()
    rd.product_family()
    #write_report()
