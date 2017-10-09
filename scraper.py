#!/usr/bin/python

from datetime import datetime

import scraperwiki
from dateutil import relativedelta
import numpy as np
import os
import pandas as pd
import pandas_datareader.data as web
import time
from pytz import timezone
import urllib2
from pandas_datareader._utils import RemoteDataError
from pandas.core.indexes.base import InvalidIndexError

now = datetime.now(timezone('Australia/Melbourne'))

if int(os.environ['MORPH_RUN_DAILY']) > 0:

    # Bail if run on weekend
    if now.strftime("%A") in ["Saturday", "Sunday"]:
        print "Runs on weekend, bailing..."
        exit(0)

    # Sleep until after 17:00
    hour = int(now.strftime("%H"))
    if 17 > hour:
        time.sleep((17 - hour) * 60 * 60)

TO_DATE = now
TO_DATE -= relativedelta.relativedelta(days=int(os.environ['MORPH_DAYS_OFFSET']))
FROM_DATE = TO_DATE - relativedelta.relativedelta(months=12)

# # Load data for ASX securitues

response = urllib2.urlopen('http://www.asx.com.au/asx/research/ASXListedCompanies.csv')
contents = response.read()

csv_content = map(lambda x: x.replace("\r", ""), contents.split("\n"))[3:]

with open('ASXListedCompanies.csv', 'wb') as csvfile:
    csvfile.write("Company,Code,Industry group\n")
    for row in csv_content:
        csvfile.write(row + "\n")

df_asx300secs = pd.read_csv("ASXListedCompanies.csv")

pricing_panel = None

secs = df_asx300secs.Code.values

# Load from Yahoo
num = 10

for index in range(0, len(secs), num):

    codes = ["%s.AX" % x for x in secs[index:index + num]]

    print "Loading data for %s" % codes

    try:
        data = web.DataReader(codes, 'yahoo', FROM_DATE, TO_DATE)

        if pricing_panel is None:
            pricing_panel = data
        else:
            pricing_panel = pd.concat([pricing_panel, data], axis=2)
    except RemoteDataError:
        print "RemoteDataError"
    except ValueError:
        print "ValueError"
    except InvalidIndexError:
        print "InvalidIndexError"

pricing_panel = pricing_panel.dropna(axis=2, how="all")

print "Done"

pricing_data = {}
for sec_val in pricing_panel.minor_axis:
    sec = sec_val[0:-3]
    pricing_data[sec] = pricing_panel[:, :, sec_val]

# # Momentum calculations

MY_SHORT_MAV_TIME_PERIOD = int(os.environ['MORPH_MY_SHORT_MAV_TIME_PERIOD'])
MY_MAV_TIME_PERIOD = int(os.environ['MORPH_MY_MAV_TIME_PERIOD'])

for sec in pricing_data.keys():
    pricing_data[sec]["MY_MAV"] = pricing_data[sec]["Close"].rolling(window=MY_MAV_TIME_PERIOD, center=False).mean()
    pricing_data[sec]["MY_SHORT_MAV"] = pricing_data[sec]["Close"].rolling(window=MY_SHORT_MAV_TIME_PERIOD,
                                                                           center=False).mean()
    pricing_data[sec]["MY_RSI"] = pricing_data[sec]["MY_SHORT_MAV"] - pricing_data[sec]["MY_MAV"]
    pricing_data[sec]["MY_RSI_RANK"] = pricing_data[sec]["MY_RSI"].rank(pct=True, method='average').round(2) - 0.01
    pricing_data[sec]["Days_Over_Under"] = np.where(pricing_data[sec]["MY_SHORT_MAV"] > pricing_data[sec]["MY_MAV"], 1,
                                                    -1)
    y = pricing_data[sec]["Days_Over_Under"]
    pricing_data[sec]["Days"] = y * (y.groupby((y != y.shift(1)).cumsum()).cumcount() + 1)
    pricing_data[sec]["Days_x_Ratio"] = ((pricing_data[sec]["Days"] * pricing_data[sec]["MY_RSI_RANK"]) / 50).round(
        0) * 50
    pricing_data[sec]["Rounded_Days"] = (pricing_data[sec]["Days"] / 10).round(0) * 10

pd.set_option('display.max_colwidth', -1)

columns = []
columns.extend(df_asx300secs.columns)
columns.extend(["URL", "extraction_date", "extracted_on"])
columns.extend(pricing_data.itervalues().next().columns)

winners_vs_20 = pd.DataFrame(data=None, index=pricing_data.keys(), columns=columns)

for sec in pricing_data.keys():
    # Get the last row of the pricing data
    data = pricing_data[sec][-1:]
    winners_vs_20.loc[sec] = data.iloc[0]

    # Copy company details
    company = df_asx300secs.ix[df_asx300secs.Code == sec].to_dict("list")
    link = 'https://au.finance.yahoo.com/echarts?s={0}.AX'.format(sec)
    winners_vs_20.loc[sec]["URL"] = link
    winners_vs_20.loc[sec]["extraction_date"] = datetime.now().date().strftime("%d-%m-%Y")
    winners_vs_20.loc[sec]["extracted_on"] = datetime.now().strftime("%d-%m-%Y %H:%m:%S")
    for col in company.keys():
        winners_vs_20.loc[sec][col] = company[col][0]

sorted_winners1 = winners_vs_20.sort_values(by=["MY_RSI_RANK", "Days_x_Ratio"], ascending=False)


# Apply some filtering to remove noisy stocks
sorted_winners2 = sorted_winners1[
    (sorted_winners1["Volume"] > int(os.environ['MORPH_VOLUME_CUTOVER'])) &
    (sorted_winners1["Close"] > float(os.environ['MORPH_CLOSE_CUTOVER']))
]

sorted_winners = sorted_winners2[["extraction_date", "Code", "Company", "Industry group", "URL",
                                  "MY_RSI_RANK", "Days", "Days_x_Ratio", "Rounded_Days", "extracted_on", "Volume",
                                  "Close", "MY_MAV", "MY_SHORT_MAV"]]


# Save in the database
for index, row in sorted_winners.iterrows():
    scraperwiki.sqlite.save(unique_keys=['Code', 'extraction_date'], data=row.to_dict())
