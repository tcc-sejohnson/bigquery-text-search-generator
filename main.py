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

    final_sql = ''

    for category in program_data['categories']:
        category_name = category['category_name']
        search_terms = category['search_terms']
        final_sql += f"DECLARE {category_name} STRING;\n"

    final_sql += '\n'

    for category in program_data['categories']:
        category_name = category['category_name']
        search_terms = category['search_terms']
        final_sql += f"SET {category_name} = r'\\b({'|'.join(search_terms)})\\b';\n"

    final_sql += '\n'
    final_sql += 'WITH initial_flags AS\n'
    final_sql += '(\n'
    final_sql += '    SELECT\n'
    final_sql += '\n'.join(
        [f'        {col_name},' for col_name in program_data['naked_columns']])

    for category in program_data['categories']:
        category_name = category['category_name']
        search_columns = category['search_columns_override'] if 'search_columns_override' in category else program_data['search_columns']
        final_sql += f"\n\n        -- Autogenerated columns for category {category_name}\n"
        for col in search_columns:
            col_alias = col if isinstance(col, str) else col['col_alias']
            col_name = col if isinstance(col, str) else col['col_name']
            final_sql += f"        REGEXP_CONTAINS(COALESCE(LOWER({col_name}), ''), {category_name}) AS {col_alias}_contains_{category_name},\n"
            # This next line is super ugly but it's that way so that it prints prettily
            final_sql += f"""        NULLIF(
            ARRAY_TO_STRING(
                REGEXP_EXTRACT_ALL(COALESCE(LOWER({col_name}), ''), {category_name}),
                ', '
            ),
            ''
        ) AS {col_alias}_{category_name}_matches,\n"""

    final_sql = final_sql[:-2] + '\n'
    if 'from_clause' in program_data['from']:
        final_sql += '\n    ' + program_data['from']['from_clause'] + '\n'
    else:
        final_sql += f"    FROM {program_data['from']['from_table']}\n"

    if 'where' in program_data:
        final_sql += f"    {program_data['where']}\n"

    final_sql += '),\n\n'
    final_sql += 'aggregate_flags AS\n'
    final_sql += '(\n'
    final_sql += '    SELECT\n'
    final_sql += '\n'.join(
        [f'        {col_name},' for col_name in program_data['naked_columns']])
    final_sql += '\n'

    for category in program_data['categories']:
        category_name = category['category_name']
        search_columns = category['search_columns_override'] if 'search_columns_override' in category else program_data['search_columns']
        final_sql += f"\n        -- Autogenerated top-level columns for category {category_name}\n"
        final_sql += '        ' + \
            '\n            OR '.join(
                [f"{col if isinstance(col, str) else col['col_alias']}_contains_{category_name}" for col in search_columns])
        final_sql += f'\n                AS any_contains_{category_name},\n'
        final_sql += '        NULLIF(\n'
        final_sql += '            CONCAT(\n'
        for col in search_columns:
            col_alias = col if isinstance(col, str) else col['col_alias']
            col_name = col if isinstance(col, str) else col['col_name']
            final_sql += '                COALESCE(\n'
            final_sql += f"                    CONCAT('{col_name}: [', {col_alias}_{category_name}_matches, ']\\n'),\n"
            final_sql += "                    ''\n"
            final_sql += '                ),\n'
        final_sql = final_sql[:-2] + '\n'
        final_sql += '            ),\n'
        final_sql += "            ''\n"
        final_sql += f'        ) AS all_{category_name}_matches,\n'

    final_sql += '\n        -- All flag columns from previous CTE\n'
    for category in program_data['categories']:
        category_name = category['category_name']
        search_columns = category['search_columns_override'] if 'search_columns_override' in category else program_data['search_columns']
        for col in search_columns:
            col_alias = col if isinstance(col, str) else col['col_alias']
            final_sql += f'        {col_alias}_contains_{category_name},\n'
            final_sql += f'        {col_alias}_{category_name}_matches,\n'

    final_sql = final_sql[:-2] + '\n'
    final_sql += '    FROM initial_flags\n'
    final_sql += ')\n\n'
    final_sql += 'SELECT * FROM aggregate_flags\n'
    final_sql += 'WHERE\n    '
    final_sql += '\n    OR '.join(
        [f"any_contains_{category['category_name']}" for category in program_data['categories']])

    with open('output.sql', 'w') as file:
        file.write(final_sql)
