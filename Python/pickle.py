import db as db
import pandas as pd
import pymysql
import numpy as np


def redshift_download(sql):
    try:
        cur = db.con.cursor()
        cur.execute(sql)
        return cur.fetchall()
    except pymysql.Error as e:
        print((e.args[0], e.args[1]))


def redshift_upload(sql):
    cur = db.con.cursor()
    try:
        cur.execute(sql)
        db.con.commit()
    except pymysql.Error as e:
        print((e.args[0], e.args[1]))
    finally:
        cur.close()


def get_data():
    data_columns = ["month_date", "user_type", "membership_type", "users_id", "query"]
    sql_string = f"""insert query"""
    data = pd.DataFrame(redshift_download(sql_string), columns=data_columns)
    data.to_pickle("./pickle.pkl")
    return data


def pickle_stuff():
    pickle_columns = ["month_date", "user_type", "membership_type", "users_id", "query"]
    pickle_data = pd.read_pickle("./pickle.pkl")
    pickle_data.columns = pickle_columns
    return pickle_data


def get_dupes(data):
    data['duplicated'] = np.NaN
    data['duplicated'] = data['users_id'].duplicated()
    data = data.loc[data['duplicated'] == True]
    data.to_csv("./results.csv", index=False)


if __name__ == '__main__':
    # df = get_data()
    df = pickle_stuff()
    get_dupes(df)

