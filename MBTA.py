#!/usr/bin/python
# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Created By  : Shubham Mishra
# Created Date: 27/10/2021
# Email       : smishra.shubhammishra@gmail.com
# Github      : https://www.github.com/shubhM13
# version     : '1.0'
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

""" Data Wrangling On MBTA V3 API"""

import requests
import pandas as pd
from datetime import datetime, timedelta, timezone


# ---------------------------------------------------------------------------

# Function to flatten out the JSON structure

def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """

    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude:
                    flatten(x[a], name + a + '_')
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out


def main():

    # Find the local time in UTC-4:00 time zone

    dt = datetime.now(timezone.utc) - timedelta(hours=4, minutes=0)

    # Predictions Endpoint with filter[stop] = Park Street and ordered by departure time

    url1 = \
        'https://api-v3.mbta.com/predictions?include=route&filter[stop]=place-pktrm&sort=departure_time'

    # Stops Endpoint

    url2 = 'https://api-v3.mbta.com/stops'

    # Route Endpoint to get the destination of routes in North and South direction

    url3 = 'https://api-v3.mbta.com/routes'

    # Get API responses

    # 1) Predictions

    try:
        resp = requests.get(url1).json()
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    # 2) Stops

    try:
        resp2 = requests.get(url2).json()
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    # 3) Routes

    try:
        resp3 = requests.get(url3).json()
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    # Select relevant columns from the response of the Predictions end point

    predictions_df = pd.DataFrame([flatten_json(x) for x in resp['data'
                                  ]]).filter(['attributes_departure_time'
            , 'attributes_direction_id', 'relationships_route_data_id',
            'relationships_stop_data_id'])
    predictions_df = \
        predictions_df[predictions_df['attributes_departure_time']
                       >= dt.strftime('%Y-%m-%dT%H:%M:%S'
                       )].sort_values('attributes_departure_time'
            ).head(10)

    # Select relevantcolumns from response of Stops end point and join it with the predictions data frame

    stops_df = pd.DataFrame([flatten_json(x) for x in resp2['data'
                            ]]).filter(['id', 'attributes_name'])
    merged_pred = pd.merge(predictions_df, stops_df,
                           left_on='relationships_stop_data_id',
                           right_on='id'
                           ).filter(['attributes_departure_time',
                                    'attributes_direction_id',
                                    'relationships_route_data_id',
                                    'attributes_name'])

    # Select relevant columns from reponse of Routes end point and join with the previous join result

    routes_df = pd.DataFrame([flatten_json(x) for x in resp3['data'
                             ]]).filter(['id',
            'attributes_direction_destinations'])
    merged_pred2 = pd.merge(merged_pred, routes_df, how='inner',
                            left_on='relationships_route_data_id',
                            right_on='id')

    merged_pred2 = merged_pred2.filter(['attributes_departure_time',
            'attributes_direction_id', 'relationships_route_data_id',
            'attributes_name', 'attributes_direction_destinations'
            ]).sort_values('attributes_departure_time', ascending=True)

    # All the routes on which next 10 trains will depart

    routes = sorted(merged_pred2['relationships_route_data_id'
                    ].unique())

    # Iterate over routes and final data frame to print the result

    print (merged_pred2['attributes_name'][0], ':', str(dt)[:-13])
    for route in routes:
        print ('-----', route, '-----')
        for (index, row) in merged_pred2.iterrows():
            if row['relationships_route_data_id'] == route:
                time_diff_seconds = \
                    (datetime.fromisoformat(row['attributes_departure_time'
                     ].replace('T', ' ')[:-6] + '+00:00')
                     - dt).total_seconds()
                time_diff_minutes = \
                    str(int(divmod(time_diff_seconds, 60)[0])) \
                    + ' Minutes ' + str(int(divmod(time_diff_seconds,
                        60)[1])) + ' Seconds'
                print (row['attributes_direction_destinations'
                       ][row['attributes_direction_id']], ':',
                       time_diff_minutes)


if __name__ == '__main__':
    main()
