import requests
import pandas as pd
import json
import numpy as np
import concurrent.futures
from sqlalchemy import create_engine
import sqlalchemy
import re

param_dic = {
    "host"      : "localhost",
    "database"  : "hh",
    "user"      : "akv",
    "password"  : "akv"
}

connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    param_dic['user'],
    param_dic['password'],
    param_dic['host'],
    param_dic['database']
)


def _url(path):
    return 'https://api.hh.ru' + path

def get_vac_list(key_words, period):
    """ Returns the full list of vacancied searched by key_words and period (days from 1 to 30)"""
    data = pd.json_normalize(get_vac_list_by_page(key_words, period))
    pages = data.pages[0]
    data = pd.DataFrame.from_records(data['items'][0])
    print(data)
    for i in range(2,pages):
        try:
            new_page = pd.json_normalize(get_vac_list_by_page(key_words,period,page=i))['items'][0]
            new_page = pd.DataFrame.from_records(new_page)
            data = data.append(new_page)
        except:
            pass
    print(data)
    return data

def get_vac_list_by_page(key_words, period=1, per_page=10, page=1):
    """ Returns json with page of vacancies in accordance with request"""
    if period > 30:
        period = 30
    return requests.get(_url('/vacancies?text={}&period={}&page={}&per_page={}'.format(
        key_words,period,page,per_page))).json()

def get_vacancies(list_of_id):
    print('start')
    data = pd.DataFrame()
    for vac_id in list_of_id:
        try:
            data = pd.concat([data,pd.json_normalize(get_vacancy(vac_id))], axis = 0, ignore_index=True)
        except:
            pass
    print('stop')
    return data

def get_vacancies_concurrency(list_of_id):
    data = pd.DataFrame()
    print('start')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(lambda x: pd.json_normalize(get_vacancy(x)), list_of_id)
        for result in results:
            data = pd.concat([data,result], axis = 0, ignore_index=True)
    print('stop')
    return data


def get_vacancy(vacancy_id):
    return requests.get(_url('/vacancies/{}'.format(vacancy_id))).json()



def vacancies_to_sql(df):
    """
    Using a dummy table to test this call library
    """
    engine = create_engine(connect)
    dtypes = {'id': sqlalchemy.types.VARCHAR(length=8),
            'salary.to': sqlalchemy.types.INTEGER,
            'salary.from': sqlalchemy.types.INTEGER,
            'salary.gross': sqlalchemy.types.Boolean,
            'has_test': sqlalchemy.types.Boolean,
            'published_at': sqlalchemy.types.TIMESTAMP}
    df.to_sql(
        'vacancies_temp', 
        con=engine, 
        index=False,
        dtype = dtypes,
        if_exists='replace'
    )
    connection = engine.connect()
    connection.execute("INSERT INTO vacancies \
            SELECT t.* FROM vacancies_temp t \
            LEFT JOIN vacancies v ON (t.id = v.id) \
            WHERE v.id IS NULL")
    connection.execute('DROP TABLE vacancies_temp')
    print("to_sql() done (sqlalchemy)")

def key_skills_to_sql(df):
    """
    Using a dummy table to test this call library
    """
    engine = create_engine(connect)
    df.to_sql(
        'key_skills_temp', 
        con=engine, 
        index=False, 
        if_exists='append'
    )
    connection = engine.connect()
    connection.execute("INSERT INTO key_skills \
            SELECT t.* FROM key_skills_temp t \
            LEFT JOIN key_skills k ON (t.id = k.id and t.name = k.name) \
            WHERE k.id IS NULL AND NOT t.name IS NULL")
    connection.execute('DROP TABLE key_skills_temp')

    print("to_sql() done (sqlalchemy)")

def create_vac_table():
    engine = create_engine(connect)
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS vacancies( \
            id VARCHAR(8) PRIMARY KEY, \
            name VARCHAR(100) NOT NULL, \
            "salary.from" INT, \
            "salary.to" INT, \
            "salary.currency" VARCHAR(10), \
            "salary.gross" BOOLEAN, \
            has_test BOOLEAN, \
            published_at TIMESTAMP, \
            "experience.name" VARCHAR(100), \
            "address.city" VARCHAR(100), \
            "address.street" VARCHAR(100), \
            "address.building" VARCHAR(100), \
            "employment.name" VARCHAR(100),
            "description" TEXT, \
            "rate" FLOAT, \
            "salary_fromRUB" INTEGER, \
            "salary_toRUB" INTEGER
            )""")

def create_key_skills_table():
    engine = create_engine(connect)
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS key_skills( \
            id VARCHAR(8), \
            name VARCHAR(100),
            PRIMARY KEY(id,name))""")

def salaries_to_net(df):
    """Multiplies salaries to and from columns of vacancies dataframe if salary.gross is true"""
    df.loc[df['salary.gross']==True, 'salary.to'] *= 0.87
    df.loc[df['salary.gross']==True, 'salary.to'] *= 0.87
    return df

def convert_currencies(df):
    """Converts currecies to RUB using latest rate for the cleanup date and
    filters any records with BYR,BYN or UAH"""
    block_cur = ['BYR','BYN','UAH']
    df = df[df['salary.currency'].isin(block_cur)==False]
    df['salary.currency'] = data['salary.currency'].str.replace('RUR','RUB')    
    EXCHANGE_URL = "https://api.exchangerate-api.com/v4/latest/RUB"
    try:
        rates = requests.get(EXCHANGE_URL)
        rates = rates.json()['rates']
    except:
        raise AssertionError('Fail to get currency exchange rates')
    df['rate'] = df['salary.currency'].map(rates)
    df['salary_fromRUB'] = df['salary.from'] / df['rate']
    df['salary_toRUB'] = df['salary.to'] / df['rate']
    return df

def clean_description(df):
    """Cleans description column of vacancies dataframe from html tags"""
    df.description = df.description.apply(lambda x: re.sub('<[^<]+?>', '', x))
    return df



if __name__=="__main__":
    ## Create tables in sql database if not exists
    create_vac_table()
    create_key_skills_table()

    vac_cols = ['id','name','salary.from',
        'salary.to','salary.currency','salary.gross',
        'has_test','published_at',
        'experience.name',"experience.name", 
                "address.city", 
                "address.street",
                "address.building",
                "employment.name",
                'description'
                ]
    # Get list of vacancies and filter only relevan columns
    #data = get_vacancies(list(get_vac_list('data+engineer',1)['id']))
    data = get_vacancies_concurrency(list(get_vac_list('data+engineer',1)['id'][1:50])) #TODO remove [1:10] when before deployment
    vacancies = data[vac_cols]

    # Get list of key skills
    key_skills = data[['id','key_skills']]
    l = key_skills['key_skills'].str.len()
    df1 = pd.DataFrame(np.concatenate(key_skills['key_skills']).tolist(), index=np.repeat(key_skills.index, l))
    key_skills = key_skills.drop('key_skills', axis=1).join(df1).reset_index(drop=True)
    
    # Clean-up vacancies
    vacancies.drop_duplicates
    vacancies = salaries_to_net(vacancies)
    vacancies = clean_description(vacancies)
    vacancies = convert_currencies(vacancies)

    # Send data to sql database
    vacancies_to_sql(vacancies)
    key_skills_to_sql(key_skills)

