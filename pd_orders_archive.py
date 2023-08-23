from __future__ import print_function
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools, GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI
import pandas as pd

# Tableau Archive copy (static)
SPREADSHEET_ID = '1trJgsYmiQTPL63Az-pYaL7Sjh2dpBzjIzWB5Rp0ofZw'
RANGE_NAME = 'PatientDirect-Tableau Orders Returns RMAs'

CLIENT_ID = "691787596771-0f01a2mpurami3k0tr3aaaf44572v9v5.apps.googleusercontent.com"
CLIENT_SECRET = "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"
REFRESH_TOKEN = "1//0dgFP9b5e8ZL5CgYIARAAGA0SNwF-L9Irnc6wqYpRwxrfEphgE7ihcxtzGdwqE0Wf8FsP_AA05y3yoHoE3qczKwzL4u_0oDvmdSQ"

def get_google_sheet(spreadsheet_id, range_name):
    """ Retrieve sheet data using OAuth credentials and Google Python API. """
    scopes = 'https://www.googleapis.com/auth/spreadsheets.readonly'
    # Setup the Sheets API
    store = file.Storage('credentials.json')
#    creds = store.get()
#    creds = None
    creds = client.OAuth2Credentials(
        access_token=None,  # set access_token to None since we use a refresh token
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        refresh_token=REFRESH_TOKEN,
        token_expiry=None,
        token_uri=GOOGLE_TOKEN_URI,
        user_agent=None,
        revoke_uri=GOOGLE_REVOKE_URI)
#    if not creds or creds.invalid:
#        flow = client.flow_from_clientsecrets('client_secret.json', scopes)
#        creds = tools.run_flow(flow, store)
    # optionally refresh the authorization token; otherwise the following line gets a new one
    creds.refresh(Http())
    service = build('sheets', 'v4', http=creds.authorize(Http()))
    # Call the Sheets API
    gsheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    return gsheet


def gsheet2df(gsheet):
    """ Converts Google sheet data to a Pandas DataFrame.
    Note: This script assumes that your data contains a header file on the first row!

    Also note that the Google API returns 'none' from empty cells - in order for the code
    below to work, you'll need to make sure your sheet doesn't contain empty cells,
    or update the code to account for such instances.

    """
    header = gsheet.get('values', [])[0]   # Assumes first line is header!
    values = gsheet.get('values', [])[1:]      # Everything else is data.
    n_cols = len(header)
    if not values:
        print('No data found.')
    else:
        all_data = []
        for col_id, col_name in enumerate(header):
            column_data = []
            for row in values:
                new_row = row
                n_row_cols = len(new_row)
                if n_row_cols < n_cols:
                    for i in range(n_cols - n_row_cols):
                        new_row.append('')
                column_data.append(new_row[col_id])
            ds = pd.Series(data=column_data, name=col_name)
            all_data.append(ds)
        df = pd.concat(all_data, axis=1)
        return df


gsheet = get_google_sheet(SPREADSHEET_ID, RANGE_NAME)
df = gsheet2df(gsheet)
print('Dataframe size = ', df.shape)
df.drop(columns=['Kit Scan','Date Order Number']).to_csv('Orders_archive_2023-07-07.csv',index=False)
