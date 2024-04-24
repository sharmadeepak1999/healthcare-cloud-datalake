from sklearn.preprocessing import LabelEncoder
import os
def transform_wearable_dataset(ti):
    df = ti.xcom_pull(task_ids='read_dataset', key='dataset')

    df.drop(columns=['ref', 'study_id'], inplace=True)

    df.drop(columns=['appearance_in_first_grade_kinship'], inplace=True)



    df['disease_comment'] = df['disease_comment'].replace('-', 'Not Applicable')
    df['gender'] = df['gender'].replace('male', 'MALE')
    df['gender'] = df['gender'].replace('female', 'FEMALE')
    df['handedness'] = df['handedness'].replace('right', 'RIGHT')
    df['handedness'] = df['handedness'].replace('left', 'LEFT')
    df['appearance_in_kinship'] = df['appearance_in_kinship'].replace('true', 'TRUE')
    df['appearance_in_kinship'] = df['appearance_in_kinship'].replace('false', 'FALSE')


    le=LabelEncoder()
    df['gender']=le.fit_transform(df['gender'])
    df['condition']=le.fit_transform(df['condition'])
    df['disease_comment']=le.fit_transform(df['disease_comment'])
    df['handedness']=le.fit_transform(df['handedness'])
    df['effect_of_alcohol_on_tremor']=le.fit_transform(df['effect_of_alcohol_on_tremor'])


    df.to_csv(os.path.join(os.environ.get('AIRFLOW_HOME'), "datasets/wearable-dataset/output/final_wearable_dataset.csv"), index=False)
    ti.xcom_push(key='transformed_dataset', value=df)