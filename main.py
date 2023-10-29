"""
API usage examples
"""

import pandas as pd
from utils.byte_genie import ByteGenie

bg = ByteGenie(
    api_url='https://api.byte-genie.com/execute',
    secrets_file='secrets.json',
    task_mode='sync',
    calc_mode='async',
    verbose=1,
)
## generate meta-data
resp = bg.generate_metadata(
    data=[{'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '22.0', 'sibsp': '1', 'parch': '0', 'fare': '7.25', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '1', 'sex': 'female', 'age': '38.0', 'sibsp': '1', 'parch': '0', 'fare': '71.2833', 'embarked': 'C', 'class': 'First', 'who': 'woman', 'adult_male': 'False', 'deck': 'C', 'embark_town': 'Cherbourg', 'alive': 'yes', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': '26.0', 'sibsp': '0', 'parch': '0', 'fare': '7.925', 'embarked': 'S', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}, {'survived': '1', 'pclass': '1', 'sex': 'female', 'age': '35.0', 'sibsp': '1', 'parch': '0', 'fare': '53.1', 'embarked': 'S', 'class': 'First', 'who': 'woman', 'adult_male': 'False', 'deck': 'C', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '35.0', 'sibsp': '0', 'parch': '0', 'fare': '8.05', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '8.4583', 'embarked': 'Q', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Queenstown', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '1', 'sex': 'male', 'age': '54.0', 'sibsp': '0', 'parch': '0', 'fare': '51.8625', 'embarked': 'S', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'E', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '2.0', 'sibsp': '3', 'parch': '1', 'fare': '21.075', 'embarked': 'S', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': '27.0', 'sibsp': '0', 'parch': '2', 'fare': '11.1333', 'embarked': 'S', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'False'}, {'survived': '1', 'pclass': '2', 'sex': 'female', 'age': '14.0', 'sibsp': '1', 'parch': '0', 'fare': '30.0708', 'embarked': 'C', 'class': 'Second', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'yes', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': '4.0', 'sibsp': '1', 'parch': '1', 'fare': '16.7', 'embarked': 'S', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'G', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'False'}, {'survived': '1', 'pclass': '1', 'sex': 'female', 'age': '58.0', 'sibsp': '0', 'parch': '0', 'fare': '26.55', 'embarked': 'S', 'class': 'First', 'who': 'woman', 'adult_male': 'False', 'deck': 'C', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '20.0', 'sibsp': '0', 'parch': '0', 'fare': '8.05', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '39.0', 'sibsp': '1', 'parch': '5', 'fare': '31.275', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'female', 'age': '14.0', 'sibsp': '0', 'parch': '0', 'fare': '7.8542', 'embarked': 'S', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '1', 'pclass': '2', 'sex': 'female', 'age': '55.0', 'sibsp': '0', 'parch': '0', 'fare': '16.0', 'embarked': 'S', 'class': 'Second', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '2.0', 'sibsp': '4', 'parch': '1', 'fare': '29.125', 'embarked': 'Q', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Queenstown', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '2', 'sex': 'male', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '13.0', 'embarked': 'S', 'class': 'Second', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'female', 'age': '31.0', 'sibsp': '1', 'parch': '0', 'fare': '18.0', 'embarked': 'S', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '7.225', 'embarked': 'C', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '2', 'sex': 'male', 'age': '35.0', 'sibsp': '0', 'parch': '0', 'fare': '26.0', 'embarked': 'S', 'class': 'Second', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '1', 'pclass': '2', 'sex': 'male', 'age': '34.0', 'sibsp': '0', 'parch': '0', 'fare': '13.0', 'embarked': 'S', 'class': 'Second', 'who': 'man', 'adult_male': 'True', 'deck': 'D', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': '15.0', 'sibsp': '0', 'parch': '0', 'fare': '8.0292', 'embarked': 'Q', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Queenstown', 'alive': 'yes', 'alone': 'True'}, {'survived': '1', 'pclass': '1', 'sex': 'male', 'age': '28.0', 'sibsp': '0', 'parch': '0', 'fare': '35.5', 'embarked': 'S', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'A', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'female', 'age': '8.0', 'sibsp': '3', 'parch': '1', 'fare': '21.075', 'embarked': 'S', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': '38.0', 'sibsp': '1', 'parch': '5', 'fare': '31.3875', 'embarked': 'S', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '7.225', 'embarked': 'C', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '1', 'sex': 'male', 'age': '19.0', 'sibsp': '3', 'parch': '2', 'fare': '263.0', 'embarked': 'S', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'C', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '7.8792', 'embarked': 'Q', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Queenstown', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '7.8958', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '1', 'sex': 'male', 'age': '40.0', 'sibsp': '0', 'parch': '0', 'fare': '27.7208', 'embarked': 'C', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'True'}, {'survived': '1', 'pclass': '1', 'sex': 'female', 'age': 'nan', 'sibsp': '1', 'parch': '0', 'fare': '146.5208', 'embarked': 'C', 'class': 'First', 'who': 'woman', 'adult_male': 'False', 'deck': 'B', 'embark_town': 'Cherbourg', 'alive': 'yes', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '7.75', 'embarked': 'Q', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Queenstown', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '2', 'sex': 'male', 'age': '66.0', 'sibsp': '0', 'parch': '0', 'fare': '10.5', 'embarked': 'S', 'class': 'Second', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '1', 'sex': 'male', 'age': '28.0', 'sibsp': '1', 'parch': '0', 'fare': '82.1708', 'embarked': 'C', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '1', 'sex': 'male', 'age': '42.0', 'sibsp': '1', 'parch': '0', 'fare': '52.0', 'embarked': 'S', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'male', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '7.2292', 'embarked': 'C', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '21.0', 'sibsp': '0', 'parch': '0', 'fare': '8.05', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'female', 'age': '18.0', 'sibsp': '2', 'parch': '0', 'fare': '18.0', 'embarked': 'S', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': '14.0', 'sibsp': '1', 'parch': '0', 'fare': '11.2417', 'embarked': 'C', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'yes', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'female', 'age': '40.0', 'sibsp': '1', 'parch': '0', 'fare': '9.475', 'embarked': 'S', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '2', 'sex': 'female', 'age': '27.0', 'sibsp': '1', 'parch': '0', 'fare': '21.0', 'embarked': 'S', 'class': 'Second', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '7.8958', 'embarked': 'C', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'True'}, {'survived': '1', 'pclass': '2', 'sex': 'female', 'age': '3.0', 'sibsp': '1', 'parch': '2', 'fare': '41.5792', 'embarked': 'C', 'class': 'Second', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'yes', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': '19.0', 'sibsp': '0', 'parch': '0', 'fare': '7.8792', 'embarked': 'Q', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Queenstown', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '8.05', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': 'nan', 'sibsp': '1', 'parch': '0', 'fare': '15.5', 'embarked': 'Q', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Queenstown', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '7.75', 'embarked': 'Q', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Queenstown', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': 'nan', 'sibsp': '2', 'parch': '0', 'fare': '21.6792', 'embarked': 'C', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'female', 'age': '18.0', 'sibsp': '1', 'parch': '0', 'fare': '17.8', 'embarked': 'S', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '7.0', 'sibsp': '4', 'parch': '1', 'fare': '39.6875', 'embarked': 'S', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '21.0', 'sibsp': '0', 'parch': '0', 'fare': '7.8', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '1', 'pclass': '1', 'sex': 'female', 'age': '49.0', 'sibsp': '1', 'parch': '0', 'fare': '76.7292', 'embarked': 'C', 'class': 'First', 'who': 'woman', 'adult_male': 'False', 'deck': 'D', 'embark_town': 'Cherbourg', 'alive': 'yes', 'alone': 'False'}, {'survived': '1', 'pclass': '2', 'sex': 'female', 'age': '29.0', 'sibsp': '1', 'parch': '0', 'fare': '26.0', 'embarked': 'S', 'class': 'Second', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'False'}, {'survived': '0', 'pclass': '1', 'sex': 'male', 'age': '65.0', 'sibsp': '0', 'parch': '1', 'fare': '61.9792', 'embarked': 'C', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'B', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '1', 'sex': 'male', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '35.5', 'embarked': 'S', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'C', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}, {'survived': '1', 'pclass': '2', 'sex': 'female', 'age': '21.0', 'sibsp': '0', 'parch': '0', 'fare': '10.5', 'embarked': 'S', 'class': 'Second', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '28.5', 'sibsp': '0', 'parch': '0', 'fare': '7.2292', 'embarked': 'C', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'True'}, {'survived': '1', 'pclass': '2', 'sex': 'female', 'age': '5.0', 'sibsp': '1', 'parch': '2', 'fare': '27.75', 'embarked': 'S', 'class': 'Second', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '11.0', 'sibsp': '5', 'parch': '2', 'fare': '46.9', 'embarked': 'S', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '22.0', 'sibsp': '0', 'parch': '0', 'fare': '7.2292', 'embarked': 'C', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'True'}, {'survived': '1', 'pclass': '1', 'sex': 'female', 'age': '38.0', 'sibsp': '0', 'parch': '0', 'fare': '80.0', 'embarked': 'nan', 'class': 'First', 'who': 'woman', 'adult_male': 'False', 'deck': 'B', 'embark_town': 'nan', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '1', 'sex': 'male', 'age': '45.0', 'sibsp': '1', 'parch': '0', 'fare': '83.475', 'embarked': 'S', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'C', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '4.0', 'sibsp': '3', 'parch': '2', 'fare': '27.9', 'embarked': 'S', 'class': 'Third', 'who': 'child', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '1', 'sex': 'male', 'age': 'nan', 'sibsp': '0', 'parch': '0', 'fare': '27.7208', 'embarked': 'C', 'class': 'First', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'True'}, {'survived': '1', 'pclass': '3', 'sex': 'male', 'age': 'nan', 'sibsp': '1', 'parch': '1', 'fare': '15.2458', 'embarked': 'C', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'yes', 'alone': 'False'}, {'survived': '1', 'pclass': '2', 'sex': 'female', 'age': '29.0', 'sibsp': '0', 'parch': '0', 'fare': '10.5', 'embarked': 'S', 'class': 'Second', 'who': 'woman', 'adult_male': 'False', 'deck': 'F', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '19.0', 'sibsp': '0', 'parch': '0', 'fare': '8.1583', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '1', 'pclass': '3', 'sex': 'female', 'age': '17.0', 'sibsp': '4', 'parch': '2', 'fare': '7.925', 'embarked': 'S', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'False'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '26.0', 'sibsp': '2', 'parch': '0', 'fare': '8.6625', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '2', 'sex': 'male', 'age': '32.0', 'sibsp': '0', 'parch': '0', 'fare': '10.5', 'embarked': 'S', 'class': 'Second', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'female', 'age': '16.0', 'sibsp': '5', 'parch': '2', 'fare': '46.9', 'embarked': 'S', 'class': 'Third', 'who': 'woman', 'adult_male': 'False', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'False'}, {'survived': '0', 'pclass': '2', 'sex': 'male', 'age': '21.0', 'sibsp': '0', 'parch': '0', 'fare': '73.5', 'embarked': 'S', 'class': 'Second', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'no', 'alone': 'True'}, {'survived': '0', 'pclass': '3', 'sex': 'male', 'age': '26.0', 'sibsp': '1', 'parch': '0', 'fare': '14.4542', 'embarked': 'C', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Cherbourg', 'alive': 'no', 'alone': 'False'}, {'survived': '1', 'pclass': '3', 'sex': 'male', 'age': '32.0', 'sibsp': '0', 'parch': '0', 'fare': '56.4958', 'embarked': 'S', 'class': 'Third', 'who': 'man', 'adult_male': 'True', 'deck': 'nan', 'embark_town': 'Southampton', 'alive': 'yes', 'alone': 'True'}],
    data_context='this a dataset about titanic passengers and its survivors',
)
print('=============================')
print("generate_metadata response:")
print(resp['response']['task_1']['data'])
print('=============================')
## filter columns
resp = bg.filter_columns(
    metadata=[{'column_name': 'survived',
               'column_description': "The 'survived' column indicates whether a data entry represents a survival outcome, with unique values '0' for not survived and '1' for survived."},
              {'column_name': 'pclass',
               'column_description': "The 'pclass' column represents the passenger class, with possible values of 1, 2, or 3."},
              {'column_name': 'sex',
               'column_description': "The 'sex' column in the data has unique values of 'male' and 'female', providing information about the gender of individuals."},
              {'column_name': 'age',
               'column_description': "Column 'age' contains numerical values representing the ages of individuals, with some missing values ('nan') included."},
              {'column_name': 'sibsp',
               'column_description': "The 'sibsp' column contains the number of siblings/spouses aboard the Titanic, with unique values ranging from 0 to 5."},
              {'column_name': 'parch',
               'column_description': "The 'parch' column represents the number of parents/children aboard the Titanic. It contains values 0, 1, 2, or 5."},
              {'column_name': 'fare',
               'column_description': "The 'fare' column in the data table contains unique values representing the fares paid for a particular service or product."},
              {'column_name': 'embarked',
               'column_description': "This column represents the port of embarkation for passengers, with possible values being 'S' (Southampton), 'C' (Cherbourg), 'Q' (Queenstown), or 'nan' (missing data)."},
              {'column_name': 'class',
               'column_description': "This column indicates the class of the data, with unique values: 'Third', 'First', and 'Second'."},
              {'column_name': 'who',
               'column_description': "A column containing values denoting the person's demographic, with unique values for 'man', 'woman', and 'child'."},
              {'column_name': 'adult_male',
               'column_description': "This column indicates whether individuals are adult males, with unique values of 'True' and 'False'."},
              {'column_name': 'deck',
               'column_description': "The 'deck' column in the data table contains unique values representing deck classifications (nan, A, B, C, D, E, F, G)."},
              {'column_name': 'embark_town',
               'column_description': "Column 'embark_town' indicates the town where passengers boarded the ship, with unique values including 'Southampton', 'Cherbourg', 'Queenstown', and 'nan'."},
              {'column_name': 'alive',
               'column_description': "Column 'alive' indicates whether the data entry refers to an entity that is currently alive or not. Possible values are 'yes' for alive and 'no' for not alive."},
              {'column_name': 'alone',
               'column_description': "The 'alone' column indicates if an individual was alone ('True') or not ('False')."}],
    query='what is the difference in number of male and female survivors by class?',
)
print('=============================')
print("filter_columns response:")
print(resp['response']['task_1']['data'])
print('=============================')
## aggregate data
resp = bg.aggregate_data(
    data=[{'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'First'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'First'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'First'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Second'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'First'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'female', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Second'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'male', 'class': 'Second'},
          {'survived': '0', 'sex': 'female', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Second'},
          {'survived': '1', 'sex': 'male', 'class': 'Second'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '1', 'sex': 'male', 'class': 'First'},
          {'survived': '0', 'sex': 'female', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'First'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'First'},
          {'survived': '1', 'sex': 'female', 'class': 'First'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Second'},
          {'survived': '0', 'sex': 'male', 'class': 'First'},
          {'survived': '0', 'sex': 'male', 'class': 'First'},
          {'survived': '1', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'female', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'female', 'class': 'Second'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Second'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'First'},
          {'survived': '1', 'sex': 'female', 'class': 'Second'},
          {'survived': '0', 'sex': 'male', 'class': 'First'},
          {'survived': '1', 'sex': 'male', 'class': 'First'},
          {'survived': '1', 'sex': 'female', 'class': 'Second'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Second'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'First'},
          {'survived': '0', 'sex': 'male', 'class': 'First'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'First'},
          {'survived': '1', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Second'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Second'},
          {'survived': '0', 'sex': 'female', 'class': 'Third'},
          {'survived': '0', 'sex': 'male', 'class': 'Second'},
          {'survived': '0', 'sex': 'male', 'class': 'Third'},
          {'survived': '1', 'sex': 'male', 'class': 'Third'}],
    query='what is the difference in number of male and female survivors by class?',
)
print('=============================')
print("aggregate_data response:")
print(resp['response']['task_1']['data'])
print('=============================')
## standardise_data
resp = bg.standardise_data(
    data=[{'variable': 'Scope 1 Emissions', 'unit': 'tCO2/ton', 'value': '200', 'date': 'n/a',
           'variable description': 'Carbon emission intensity for scope 1',
           'value provided or estimate': 'Value provided in source', 'pagenum': '[1]',
           'doc_num': '[0]', 'row_id': 0},
          {'variable': 'Scope 2 Emissions', 'unit': 'tCO2/ton', 'value': '200', 'date': 'n/a',
           'variable description': 'Carbon emission intensity for scope 2',
           'value provided or estimate': 'Value provided in source', 'pagenum': '[1]',
           'doc_num': '[0]', 'row_id': 1},
          {'variable': 'Scope 3 Emissions', 'unit': 'tCO2/ton', 'value': '200', 'date': 'n/a',
           'variable description': 'Carbon emission intensity for scope 3',
           'value provided or estimate': 'Value provided in source', 'pagenum': '[1]',
           'doc_num': '[0]', 'row_id': 2}],
    groupby_cols=[],
    cols_to_std=['variable', 'unit', 'value', 'date', 'variable description', 'pagenum', 'doc_num'],
)
print('=============================')
print("standardise_data response:")
print(resp['response']['task_1']['data'])
print('=============================')
## standardise_names
resp = bg.standardise_names(
    data=[{'category': 'emissions', 'variable': 'Scope 1 Emissions.'},
          {'category': 'emissions', 'variable': 'GHG (scope 1)'},
          {'category': 'energy', 'variable': 'Electricity use'},
          {'category': 'emissions', 'variable': 'GHG scope 2'},
          {'category': 'energy', 'variable': 'Consumption of electricity'},
          {'category': 'energy', 'variable': 'Total energy use'},
          {'category': 'energy', 'variable': 'Fuel usage'},
          {'category': 'energy', 'variable': 'Total energy consumption'}],
    groupby_cols=['category'],
    text_col='variable',
)
print('=============================')
print("standardise_names response:")
print(resp['response']['task_1']['data'])
print('=============================')
## create_dataset
resp = bg.create_dataset(
    data=[{'score': 0.9,
           'row_id': 303,
           'category': 'GHG Emissions',
           'variable': 'Scope 2',
           'variable description': 'Indirect emissions',
           'unit': 'MT CO2e',
           'date': 'April 1, 2021-March 31, 2022',
           'value': '1,03,478',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries Limited',
           'pagenum': 56,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'},
          {'score': 0.89,
           'row_id': 262,
           'category': 'Emissions',
           'variable': 'Scope 2 Indirect',
           'variable description': 'GHG Emissions',
           'unit': 'tCO2 eq.',
           'date': 'FY 2021-22',
           'value': '1,03,478',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries',
           'pagenum': 54,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'},
          {'score': 0.87,
           'row_id': 103,
           'category': 'GHG Emissions',
           'variable': 'Scope 2',
           'variable description': 'Emissions from purchased electricity and purchased steam.',
           'unit': 'tCO2e.',
           'date': 'FY 2021-22',
           'value': '1,03,478',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries',
           'pagenum': 28,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'},
          {'score': 0.86,
           'row_id': 302,
           'category': 'GHG Emissions',
           'variable': 'Scope 1',
           'variable description': 'Direct emissions',
           'unit': 'MT CO2e',
           'date': 'April 1, 2021-March 31, 2022',
           'value': '5,99,553',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries Limited',
           'pagenum': 56,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'},
          {'score': 0.85,
           'row_id': 308,
           'category': 'GHG Emissions',
           'variable': 'Scope 3',
           'variable description': 'Waste generated in operations',
           'unit': 'MT CO2e',
           'date': 'April 1, 2021-March 31, 2022',
           'value': '11,108',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries Limited',
           'pagenum': 56,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'},
          {'score': 0.85,
           'row_id': 304,
           'category': 'GHG Emissions',
           'variable': 'Scope 3',
           'variable description': 'Purchased goods and services',
           'unit': 'MT CO2e',
           'date': 'April 1, 2021-March 31, 2022',
           'value': '11,89,713',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries Limited',
           'pagenum': 56,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'},
          {'score': 0.85,
           'row_id': 261,
           'category': 'Emissions',
           'variable': 'Scope 1 Direct Emission',
           'variable description': 'GHG Emissions',
           'unit': 'tCO2 eq.',
           'date': 'FY 2021-22',
           'value': '5,99,553',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries',
           'pagenum': 54,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'},
          {'score': 0.85,
           'row_id': 309,
           'category': 'GHG Emissions',
           'variable': 'Scope 3',
           'variable description': 'Employee commuting',
           'unit': 'MT CO2e',
           'date': 'April 1, 2021-March 31, 2022',
           'value': '15,653',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries Limited',
           'pagenum': 56,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'},
          {'score': 0.85,
           'row_id': 306,
           'category': 'GHG Emissions',
           'variable': 'Scope 3',
           'variable description': 'Fuel and energy related activities',
           'unit': 'MT CO2e',
           'date': 'April 1, 2021-March 31, 2022',
           'value': '1,20,572',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries Limited',
           'pagenum': 56,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'},
          {'score': 0.84,
           'row_id': 202,
           'category': 'Environment',
           'variable': 'Emissions',
           'variable description': 'Energy indirect (Scope 2) GHG emissions',
           'unit': '',
           'date': '2022',
           'value': 'Energy indirect (Scope 2) GHG emissions',
           'doc_org': 'Aarti Industries',
           'doc_year': 2022,
           'company name': 'Aarti Industries',
           'pagenum': 51,
           'doc_num': 0,
           'doc_name': 'httpswwwaarti-industriescomuploadpdfsustainability-report-2021-2022pdf'}],
    attrs=['emissions scope', 'emissions unit', 'emissions value', 'company name'],
    cols_to_use=['category', 'variable', 'date', 'value', 'variable description', 'company name', 'doc_org'],
)
print('=============================')
print("create_dataset response:")
print(resp['response']['task_1']['data'])
print('=============================')
"""
create_dataset returns data in a narrow/melted format, which can be converted into a wide format by pivoting the data 
"""
df = pd.DataFrame(resp['response']['task_1']['data'])
df = df.pivot(
    index=['row_num', 'context'],
    columns=['variable'],
    values='value',
).reset_index().drop(columns=['row_num', 'context'])
print('=============================')
print("create_dataset response (pivoted):")
print(df.to_dict('records'))
print('=============================')
