if __name__ == '__main__':
    """
    PLEASE NOTE: There are definitely things that could be better about this.
    It's a one-off for a relatively-obscure use-case. Get over it.

    All data needed to generate a SQL file goes into the program_data variable.
    Hierarchically, it looks like this:
    {
        'categories': [
            {
                'category_name': 'some_name',
                'search_columns_override': [
                    'some_column_name_1',
                    'some_column_name_2',
                ],
                'search_terms': [
                    'search_term_1',
                    'search_term_2',
                ],
            }
        ],
        'search_columns': [
            'some_column_name_1',
            'some_column_name_2',
        ],
        'naked_columns': [
            'some_column_name_1',
            'some_column_name_2',
        ],
        'from': {
            'from_clause': 'dbo.MyTable1 AS a LEFT OUTER JOIN dbo.MyTable2 AS b ON a.Key = b.Key',
            'from_table': 'dbo.MyTable1',
        },
        'where': 'WHERE a.MyColumn = "something"',
    }

    Definitions:
        categories: One record per search category.
            category_name: This is the name that will be used in the column names; for example: column_name_contains_category_name.
            search_columns_override: A list of columns to search. If present, overrides the list of search_columns (see below) for this category.
            search_terms: A list of search terms to look for in the column text.
        search_columns: The default columns to search for all categories. Overridden at a category level by search_columns_override.
        naked_columns: These columns will be included directly in the query -- so if you need columns such as an ID column to pull through, put them here.
        from: Supply either a from_clause (a COMPLETE SQL FROM clause) or a from_table (the name of a table). If you supply a from_clause,
            keep in mind you MUST prefix all of your column names with a table alias from your FROM clause to avoid name clashes.
        where: A complete WHERE clause. Can safely be omitted.

    A note on column names: The column name can either just be a string, or it can be an object:
    {
        'col_name': 'col_name_1',
        'col_alias': 'col_alias',
    }

    If it's an object, its alias will be used in the outputted column names rather than its full name.
    This can be useful if you have very long column names.
    """
    program_data = {
        'categories': [
            {
                'category_name': 'messaging',
                'search_terms': [
                    'chatbot',
                    'social',
                    'messaging'
                ],
            },
            {
                'category_name': 'platform',
                'search_terms': [
                    'integration',
                    'apps',
                    'third-party',
                    'third party',
                    '3rd party',
                    'extensibility',
                    'objects',
                    'system',
                    'systems',
                ],
            },
            {
                'category_name': 'reliability',
                'search_terms': [
                    'outage',
                    'reliability',
                    'stability',
                    'uptime',
                    'downtime',
                    'security',
                    'breach',
                ],
            },
            {
                'category_name': 'crmification',
                'search_terms': [
                    'sfa',
                    'sales automation',
                ],
            },
            {
                'category_name': 'enterprise',
                'search_terms': [
                    'architecture',
                    'scalability',
                    'itsm',
                    'itil',
                    'sandbox',
                    'ola',
                    'sla',
                ],
            },
            {
                'category_name': 'other',
                'search_terms': [
                    'social listening',
                    'social media management',
                    'social media monitor',
                ],
            }
        ],
        'search_columns': [
            {'col_name': 'account_health_risk_type__c', 'col_alias': 'ahrt'},
            {'col_name': 'description_of_risk__c', 'col_alias': 'dor'},
            {'col_name': 'x2014_renewal_status_notes__c', 'col_alias': 'rsn'},
            {'col_name': 'churn_reason__c', 'col_alias': 'cr'},
            {'col_name': 'competitive_notes__c', 'col_alias': 'cn'},
        ],
        'naked_columns': [
            'id',
            'account_health_risk_type__c',
            'description_of_risk__c',
            'x2014_renewal_status_notes__c',
            'churn_reason__c',
            'competitive_notes__c',
        ],
        'from': {
            'from_table': 'sfdc.account_scd2'
        },
        'where': "WHERE dw_eff_end = '9999-12-31'"
    }

    def get_search_columns(category):
        return category['search_columns_override'] if 'search_columns_override' in category else program_data['search_columns']

    def get_column_alias(column):
        return column if isinstance(column, str) else column['col_alias']
    
    def get_column_name(column):
        return column if isinstance(column, str) else column['col_name']

    def generate_category_variables(categories):
        variables = ''
        for category in categories:
            category_name = category['category_name']
            variables += f"DECLARE {category_name} STRING;\n"
        return variables + '\n'

    def generate_category_variable_values(categories):
        variable_values = ''
        for category in categories:
            category_name = category['category_name']
            search_terms = category['search_terms']
            variable_values += f"SET {category_name} = r'\\b({'|'.join(search_terms)})\\b';\n"
        return variable_values + '\n'

    def generate_columns_select_list(columns):
        return '\n'.join([f'        {col_name},' for col_name in columns])

    def generate_category_search_columns(categories):
        columns = ''
        for category in categories:
            category_name = category['category_name']
            search_columns = get_search_columns(category)
            columns += f"\n\n        -- Autogenerated columns for category: {category_name}\n"
            for col in search_columns:
                col_alias = get_column_alias(col)
                col_name = get_column_name(col)
                columns += f"        REGEXP_CONTAINS(COALESCE(LOWER({col_name}), ''), {category_name}) AS {col_alias}_contains_{category_name},\n"
                columns += '        NULLIF('
                columns += '\n             ARRAY_TO_STRING('
                columns += f"\n                REGEXP_EXTRACT_ALL(COALESCE(LOWER({col_name}), ''), {category_name}),"
                columns += "\n                ', '"
                columns += "\n            ),"
                columns += "\n            ''"
                columns += f'\n        ) AS {col_alias}_{category_name}_matches,\n'
        return columns[:-2] + '\n'

    def get_from_clause(data):
        if 'from_clause' in data['from']:
            return f"\n    {data['from']['from_clause']}\n"
        else:
            return f"    FROM {data['from']['from_table']}\n"

    def generate_initial_flags_cte(data):
        cte = ''
        cte += 'WITH initial_flags AS\n'
        cte += '(\n'
        cte += '    SELECT\n'
        cte += generate_columns_select_list(data['naked_columns'])
        cte += generate_category_search_columns(data['categories'])
        cte += get_from_clause(data)
        cte += f"    {data['where']}\n" if 'where' in data else ''
        cte += '),\n\n'
        return cte

    def generate_category_top_level_columns(categories):
        columns = '\n'
        for category in categories:
            category_name = category['category_name']
            search_columns = get_search_columns(category)
            columns += f"\n        -- Autogenerated top-level columns for category: {category_name}\n"
            columns += '        ' + \
                '\n            OR '.join(
                    [f"{get_column_alias(col)}_contains_{category_name}" for col in search_columns])
            columns += f'\n                AS any_contains_{category_name},\n'
            columns += '        NULLIF(\n'
            columns += '            CONCAT(\n'
            for col in search_columns:
                col_alias = get_column_alias(col)
                col_name = get_column_name(col)
                columns += '                COALESCE(\n'
                columns += f"                    CONCAT('{col_name}: [', {col_alias}_{category_name}_matches, ']\\n'),\n"
                columns += "                    ''\n"
                columns += '                ),\n'
            columns = columns[:-2] + '\n'
            columns += '            ),\n'
            columns += "            ''\n"
            columns += f'        ) AS all_{category_name}_matches,\n'
        return columns

    def generate_initial_flags_column_list(categories):
        columns = ''
        for category in categories:
            category_name = category['category_name']
            search_columns = get_search_columns(category)
            for col in search_columns:
                col_alias = get_column_alias(col)
                columns += f'        {col_alias}_contains_{category_name},\n'
                columns += f'        {col_alias}_{category_name}_matches,\n'
        return columns[:-2] + '\n'

    def generate_aggregate_flags_cte(data):
        cte = ''
        cte += 'aggregate_flags AS\n'
        cte += '(\n'
        cte += '    SELECT\n'
        cte += generate_columns_select_list(data['naked_columns'])
        cte += generate_category_top_level_columns(data['categories'])
        cte += '\n        -- All flag columns from previous CTE\n'
        cte += generate_initial_flags_column_list(data['categories'])
        cte += '    FROM initial_flags\n'
        cte += ')\n\n'
        return cte

    def generate_final_select_statement(data):
        statement = ''
        statement += 'SELECT * FROM aggregate_flags\n'
        statement += 'WHERE\n    '
        statement += '\n    OR '.join([f"any_contains_{category['category_name']}" for category in data['categories']])
        return statement

    def write_sql_to_file(sql):
        with open('output.sql', 'w') as file:
                file.write(sql)

    final_sql = ''
    final_sql += generate_category_variables(program_data['categories'])
    final_sql += generate_category_variable_values(program_data['categories'])
    final_sql += generate_initial_flags_cte(program_data)
    final_sql += generate_aggregate_flags_cte(program_data)
    final_sql += generate_final_select_statement(program_data)
    write_sql_to_file(final_sql)
