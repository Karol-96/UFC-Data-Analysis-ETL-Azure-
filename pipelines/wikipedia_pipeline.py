import json
import pandas as pd
import bs4
import requests




def get_ufc_page(url):
    print("Fetching Data From UFC")
    try:
        response = requests.get(url)
        response.raise_for_status() #Check if requests is succesful.
        html = response.text
        # print(html)
    except requests.RequestException as e:
        print(f"An Error occured while fetching data. {e}")
    return (response.text)

        
def get_ufc_data(**kwargs):
    url = kwargs["url"]
    html1 = get_ufc_page(url)
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html1,'html.parser')
    # Find the div with id "mw-content-text"
    content_div = soup.find("div", {"id": "mw-content-text"})

    if not content_div:
        print("No div with id 'mw-content-text' found on the page.")
        return
    # Find all tables within the content div
    tables = content_div.find_all("table", {"class": "wikitable sortable"})[7]
    if not tables:
        print("No tables found within the div.")
        return
    table_rows= tables.find_all('tr')
    # print(table_rows)
    data = []
    for row in table_rows:
        row_data = [cell.text.strip() for cell in row.find_all(['th', 'td'])]
        data.append(row_data)

    df = pd.DataFrame(data[1:], columns=data[0])
    print("The DF is",df)
    kwargs['ti'].xcom_push(key='rows', value=df)
    return "OK"


def ufc_data_cleaning(**kwargs):
    data = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='get_ufc_data', key='rows'))
    print(data.columns)
    data.to_csv('hh.csv')
    result_column = data['Result / next fight / status']
    # Check if 'Win' or 'Loss' is present in each row of the column
    win_condition = result_column.str.contains('Win')
    loss_condition = result_column.str.contains('Loss')
    print(win_condition)
    print(loss_condition)
    data['Result / next fight / status'] = data['Result / next fight / status'].str.strip()
    data['Result / next fight / status'] = data['Result / next fight / status'].str.replace(r'^(Win|Loss)\s*-\s*', '', regex=True)
    data = data.rename(columns= {"Result / next fight / status":"next fight / status"})
    invalid_rows = data[data['next fight / status'].apply(lambda x: len(str(x).split(' - '))) != 2]

    print(invalid_rows[['next fight / status']])
    #  'ext fight / status' is the column that contains values like 'UFC 300 (Las Vegas) - Bobby Green'
    non_empty_mask = data['next fight / status'].notnull()
    data.loc[non_empty_mask, ['Event/Location', 'Opponent']] = data.loc[non_empty_mask, 'next fight / status'].str.split(' - ', n=1, expand=True)


    # Process the 'Result / next fight / status' column
    data['Result / next fight / status'] = data['next fight / status'].str.strip()
    data['Result / next fight / status'] = data['Result / next fight / status'].str.replace('Win - ', "").str.replace('Loss - ', '')
    data = data.rename(columns={"Result / next fight / status": "next fight / status"})


    # Drop the original column
    data.drop('next fight / status', axis=1, inplace=True)
    kwargs['ti'].xcom_push(key='rows1', value=data)
    return(data)

def write_ufc_data(**kwargs):
    from datetime import datetime
    data = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='ufc_data_cleaning', key='rows1'))

    file_name = ('UFC_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')
    print('final output',data)

    #Loading into Azure container
    # data.to_csv('data/' + file_name, index=False)
    data.to_csv('abfs://raw@ufcdata.dfs.core.windows.net/data/' + file_name,
                storage_options={
                    'account_key': 'gQygp7fkkrwEgrN0dHOSwIs17y5uSGrKjkMb0l/6nlWTkZTVxkhWkjd7IluKg9Db3ibRYTPWMhEg+AStHWnIkg=='
                }, index=False)
