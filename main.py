import os
import pandas as pd


DATA_DIR = 'data'
PERSONS_SMALL_DATA_JSON_FILE_NAME = 'small_data_persons.json'
PERSONS_BIG_DATA_JSON_FILE_NAME = 'big_data_persons.json'
CONTACTS_SMALL_DATA_JSON_FILE_NAME = 'small_data_contracts.json'
CONTACTS_BIG_DATA_JSON_FILE_NAME = 'big_data_contracts.json'

# don't save additional columns added when transforming data
# to saved sheets
KEEP_ONLY_ORIGINAL_COLS_IN_SAVED = True

OUTPUT_DIR = 'output'
OUTPUT_FILE_NAME = 'output.xlsx'
if not os.path.exists(OUTPUT_DIR):
    os.mkdir(OUTPUT_DIR)
output_file_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME)


# the purpose is to avoid specifying "keep_original" in every function call
# and have global settings, and to easily switch by changing
# KEEP_ONLY_ORIGINAL_COLS_IN_SAVED variable
def only_original_cols_decorator(func):
    def wrapper(*args, **kwargs):
        kwargs["keep_original"] = KEEP_ONLY_ORIGINAL_COLS_IN_SAVED
        func(*args, **kwargs)
    return wrapper


def create_df_with_name_surname_cols(df):
    df[['name', 'surname']] = df['name_surname'].str.split(' ', n=1, expand=True)
    return df


def keep_df_only_original_cols(df):
    df = df[['ID','Name', 'Age']]
    return df


def prepare_data_for_persons():
    persons_small_data_json_path = os.path.join(DATA_DIR, PERSONS_SMALL_DATA_JSON_FILE_NAME)
    persons_big_data_json_path = os.path.join(DATA_DIR, PERSONS_BIG_DATA_JSON_FILE_NAME)
    
    # load data
    persons_small_df = pd.read_json(persons_small_data_json_path)
    persons_big_df = pd.read_json(persons_big_data_json_path)
        
    # prepare data
    persons_small_df['name_surname'] = persons_small_df['Name']
    persons_small_df['age'] = persons_small_df['Age']
    persons_big_df['name_surname'] = persons_big_df['Name']
    persons_big_df['age'] = persons_big_df['Age']
    
    # uncomment if don't want to preserve original cols
    #persons_small_df = persons_small_df.rename(columns={'Name': 'name_surname',
    #                                                    'Age': 'age'})
    #persons_big_df = persons_big_df.rename(columns={'Name': 'name_surname',
    #                                                 'Age': 'age'})
    
    persons_small_df = create_df_with_name_surname_cols(persons_small_df)
    persons_big_df = create_df_with_name_surname_cols(persons_big_df)
    return persons_small_df, persons_big_df
    

@only_original_cols_decorator
def task_1_3(persons_small_df, persons_big_df, writer, keep_original):
    persons_small_df = persons_small_df.sort_values(by='surname')
    persons_big_df = persons_big_df.sort_values(by='name')
    if keep_original:
        persons_small_df = keep_df_only_original_cols(persons_small_df)
    persons_small_df.to_excel(writer, sheet_name='small_data')
    if keep_original:
        persons_big_df = keep_df_only_original_cols(persons_big_df)
    persons_big_df.to_excel(writer, sheet_name='big_data')


@only_original_cols_decorator
def task_1_5(persons_small_df, persons_big_df, writer, keep_original):
    # this finds persons that are in big_data and not in small_data
    # the task says to find persons that are in small_data 
    # and not in big_data
    # I believe it was a mistake in task description, because there
    # are not such people
    # but there are persons that are in big_data and not in small_data
    df_joined = persons_big_df.merge(persons_small_df, on=['surname'], 
                       how='left', indicator=True)
    
    # you can uncomment to check there's no persons that are in small_data 
    # and not in big_data
    #df_joined = persons_small_df.merge(persons_big_df, on=['surname'], 
    #                   how='left', indicator=True)
    
    # only 'both' values (means all persons that are in small_data
    # are also in big_data)
    # print(df_joined['_merge'].unique())
    
    missing_persons = df_joined[df_joined['_merge'] == 'left_only']
    missing_persons = missing_persons.rename(columns={"ID_x": "ID",
                                                      "Name_x": "Name",
                                                      "Age_x": "Age"})
    if keep_original:
        missing_persons = keep_df_only_original_cols(missing_persons)
    missing_persons.to_excel(writer, sheet_name='1.5')


@only_original_cols_decorator
def task_1_6(persons_big_df, writer, keep_original):
    sorted_by_age = persons_big_df.sort_values(by='age', ascending=False)
    sorted_by_age['diff'] = sorted_by_age.groupby('surname')['age'].diff(-1)    
    with_diff_10 = sorted_by_age[sorted_by_age['diff'] == 10]
    if keep_original:
        with_diff_10 = keep_df_only_original_cols(with_diff_10)
    with_diff_10.to_excel(writer, sheet_name='1.6')


@only_original_cols_decorator
def task_1_7(persons_big_df, writer, keep_original):
    persons_big_df = persons_big_df.filter(regex='[a-zA-Z]')
    persons_big_df = persons_big_df[persons_big_df['Name'].str.contains(pat='[a-zA-Z]')] 
    if keep_original:
        persons_big_df = keep_df_only_original_cols(persons_big_df)
    persons_big_df.to_excel(writer, sheet_name='1.7')


def prepare_data_for_contacts(persons_big_df):
    contacts_small_data_json_path = os.path.join(DATA_DIR, CONTACTS_SMALL_DATA_JSON_FILE_NAME)
    contacts_big_data_json_path = os.path.join(DATA_DIR, CONTACTS_BIG_DATA_JSON_FILE_NAME)
    
    # load data
    contacts_small_df = pd.read_json(contacts_small_data_json_path)
    contacts_big_df = pd.read_json(contacts_big_data_json_path)
    
    contacts_df = pd.concat([contacts_small_df, contacts_big_df])
    contacts_df = contacts_df.drop_duplicates()
    contacts_df['To'] = pd.to_datetime(contacts_df['To'])
    contacts_df['From'] = pd.to_datetime(contacts_df['From'])
    contacts_df['duration'] = contacts_df['To'] - contacts_df['From']
    threshold_mins = 5*60
    # filter out contacts with duration of less than 5 minutes
    # because by conition they are not contacts
    contacts_df = contacts_df[contacts_df['duration'].dt.total_seconds() >= threshold_mins]
    
    df_merged = pd.concat([pd.merge(contacts_df, persons_big_df, left_on='Member1_ID', right_on='ID'),
                           pd.merge(contacts_df, persons_big_df, left_on='Member2_ID', right_on='ID')])
    return df_merged

@only_original_cols_decorator
def task_2_4(df, persons_big_df, writer, keep_original):
    df = df.groupby(['ID'])['name'].count().reset_index()
    df = df.rename(columns={'name': 'count'})
    df = df.sort_values(by='count', ascending=False)
    df = df.merge(persons_big_df, on=['ID'], how='inner')
    if keep_original:
        df = keep_df_only_original_cols(df)
    df.to_excel(writer, sheet_name='2.4')


@only_original_cols_decorator
def task_2_5(df, persons_big_df, writer, keep_original):
    df = df.groupby(['ID'])['duration'].sum().reset_index()
    df = df.sort_values(by='duration', ascending=False)
    df = df.merge(persons_big_df, on=['ID'], how='inner')
    if keep_original:
        df = keep_df_only_original_cols(df)
    df.to_excel(writer, sheet_name='2.5')


def task_2_6(df_merged):
    df_merged = df_merged.sort_values(by=['From', 'To'], ascending=True)
    df_merged['time_between_contacts'] =  df_merged.groupby('ID')['To'].shift(-1) - df_merged.groupby('ID')['From'].shift(0)
    df_merged['time_between_contacts'] = df_merged['time_between_contacts'].dt.total_seconds()

    df = df_merged.groupby(['age'])['time_between_contacts'].mean().reset_index()
    df = df.rename(columns={'time_between_contacts': 'avg_time_between_contacts'})
    df = df.sort_values(by=['avg_time_between_contacts', 'age'], ascending=True)
    
    print("Task 2.7")
    print(df.head(10))
    print("These are mostly people with age > 68")


def main():
    # part 1
        
    persons_small_df, persons_big_df = prepare_data_for_persons()
    writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
    
    # tasks 1.3-1.4
    task_1_3(persons_small_df, persons_big_df, writer)
    
    task_1_5(persons_small_df, persons_big_df, writer)
    
    # will use big_data further because it's not clear
    # from task which dataset should be used
    # and there are all values in big_data
    
    task_1_6(persons_big_df, writer)
    task_1_7(persons_big_df, writer)
    
    # part 2

    df_merged = prepare_data_for_contacts(persons_big_df)

    task_2_4(df_merged, persons_big_df, writer)
    task_2_5(df_merged, persons_big_df, writer)
    task_2_6(df_merged)
    
    writer.save()
    writer.close()


if __name__ == '__main__':
    main()
