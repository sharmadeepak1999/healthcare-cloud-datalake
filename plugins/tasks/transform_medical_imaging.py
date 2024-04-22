import pandas as pd
import os
def remove_outliers_and_impute(df, columns, threshold=1.5, impute_method='mean'):
    result_df = df.copy()
    
    for col in columns:
        # Calculate quartiles and IQR without null values
        Q1 = result_df[col].quantile(0.25)
        Q3 = result_df[col].quantile(0.75)
        IQR = Q3 - Q1

        # Calculate the lower and upper bounds for trimming
        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR

        # Remove outliers from non-null values
        result_df.loc[~result_df[col].isnull(), col] = result_df.loc[~result_df[col].isnull(), col].apply(
            lambda x: x if lower_bound <= x <= upper_bound else None
        )

        # Impute null values
        if impute_method == 'mean':
            impute_value = round(result_df[col].mean(), 2)
        elif impute_method == 'median':
            impute_value = result_df[col].median()
        else:
            impute_value = impute_method
        result_df[col].fillna(impute_value, inplace=True)

    return result_df

def one_hot_encode_and_drop(df, column):
    # Perform one-hot encoding
    encoded_df = pd.get_dummies(df[column], prefix=column)

    # Concatenate the encoded DataFrame with the original DataFrame
    df = pd.concat([df, encoded_df], axis=1)

    # Drop the original column
    df.drop(columns=[column], inplace=True)

    return df


def standardize_dates(date):
    date_formats_unique = [
        '%B %d, %Y',    # January 22, 2020
        '%Y',           # 2017
        '%b %d, %Y',    # Mar 3, 2020
        '%m/%d/%Y',     # 3/7/2020
        '%B %d',        # January 13
        '%B %Y',        # March 2003
    ]
    for date_format in date_formats_unique:
        try:
            standardized_date = pd.to_datetime(date, format=date_format)
            if not pd.isnull(standardized_date):
                return standardized_date.strftime('%Y-%m-%d')
        except:
            pass
    # If none of the formats match, return None
    return None

def replace_and_fill(df, columns, mapping):
    for col in columns:
        if col in mapping:
            df[col] = df[col].replace(mapping[col])
            df[col].fillna("Unknown", inplace=True)
    return df


def transform_medical_imaging(ti):
    df = ti.xcom_pull(task_ids='read_dataset', key='dataset')
    df.drop(columns=['extubated', 'in_icu', 'intubation_present', 'offset', 'doi', 'url', 'license', 'Unnamed: 29'], inplace=True)

    mode_sex = df.groupby("patientid")['sex'].first().mode()[0]
    df['sex'].fillna(mode_sex, inplace=True)

    columns = ['age', 'temperature', 'pO2_saturation', 'leukocyte_count', 'neutrophil_count', 'lymphocyte_count']
    df = remove_outliers_and_impute(df, columns)

    df['age'] = df['age'].round()

    df['clinical_notes'].fillna('Not specified', inplace=True)
    df['other_notes'].fillna('Not specified', inplace=True)
    df['location'].fillna('Not specified', inplace=True)

    df['finding'] = df['finding'].replace('todo', 'Unknown')
    
    mapping = {
        'RT_PCR_positive': {'Y': 'Yes', 'N': 'No'},
        'survival': {'Y': 'Yes', 'N': 'No'},
        'intubated': {'Y': 'Yes', 'N': 'No'},
        'went_icu': {'Y': 'Yes', 'N': 'No'},
        'needed_supplemental_O2': {'Y': 'Yes', 'N': 'No'}
    }
    columns_to_process = mapping.keys()
    df = replace_and_fill(df, columns_to_process, mapping)

    columns = ['finding', 'RT_PCR_positive', 'survival', 'intubated', 'went_icu', 'needed_supplemental_O2']
    for col in columns:
        df = one_hot_encode_and_drop(df, col)

    df['date_n'] = df['date'].apply(standardize_dates)
    df['year'] = pd.to_datetime(df['date_n']).dt.year
    df.drop(columns=['date_n', 'date'], inplace=True)
    df.to_csv(os.path.join(os.environ.get('AIRFLOW_HOME'), "datasets/medical-imaging/output/transformed_medical_imaging_dataset.csv"), index=False)
    ti.xcom_push(key='transformed_dataset', value=df)