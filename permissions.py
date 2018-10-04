import pandas as pd


def generate_permissions(permissions_data):
    """
    !!! DON'T CHANGE THE NAME OF THIS FUNCTION !!!
    `permissions_data` is the result of the query defined in the `PERMISSIONS_DATA` block of the etl config.

    By default, `permissions_data` comes from the `user_groups_permissions` domain
    and is a list of dictionaries like this
    {'group': 'Paris', 'country': 'France', 'region': 'Ile-de-France', 'department': 75, 'domain': 'user_groups_permissions'}
    """
    permissions = []

    permissions_df = pd.DataFrame(permissions_data).drop(columns='domain')
    """
default_report  entityGroup        entityName        user_group
          NaN       manager              None     FranceManager
            1          None     reportFrance1     FranceManager
          NaN          None     reportFrance2     FranceManager
            1         paris     reportFramce1      ParisManager
          NaN       manager              None  AllemagneManager
            1          None  reportAllemagne1  AllemagneManager
    """
    for group, df_group in permissions_df.groupby('user_group'):
        possibilities = []  # all possible combinations
        default = None  # default report
        for idx, row in df_group.iterrows():
            row_dict = row.drop('user_group').to_dict()  # row_dict = {'default': 1, 'entityGroup': None, 'entityName': 'reportFrance1'}
            row_default = row_dict.pop('default_report', None)  # row_default = 1, row_dict = {'entityGroup': None, 'entityName': 'reportFrance1'}
            query = {k: v for k, v in row_dict.items() if v is not None}  # query = {'entityName': 'reportFrance1'}
            if row_default == 1:
                default = query  # default = {'entityName': 'reportFrance1'}
            possibilities.append(query)
        permission = {
            'group': group,
            'reports': {'$or': possibilities},
        }
        if default is not None:
            permission['default'] = default
        permissions.append(permission)

    """
    permissions = [
        {
            'group': 'AllemagneManager',
            'reports': {'$or': [
                {'entityGroup': 'manager'},
                {'entityName': 'reportAllemagne1'}
            ]},
            'default': {'entityName': 'reportAllemagne1'}
        },
        {
            'group': 'FranceManager',
            'reports': {'$or': [
                {'entityGroup': 'manager'},
                {'entityName': 'reportFrance1'},
                {'entityName': 'reportFrance2'}
            ]},
            'default': {'entityName': 'reportFrance1'}}
        },
        {
            'group': 'ParisManager',
            'reports': {'$or': [
                {'entityGroup': 'paris', 'entityName': 'reportFrance1'}
            ]},
            'default': {'entityGroup': 'paris', 'entityName': 'reportFrance1'}
        }
    ]
    """
    return permissions
