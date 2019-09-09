# -*- coding: utf-8 -*-

import pandas as pd
from ddf_utils.str import to_concept_id

source = '../source/Table_Model-based_estimates_Countries_Run20180220.xlsx'


def d(c, m):
    return {'indicator': c, 'method': m}


def create_datapoints(data, indicator_mapping, method_mapping):
    data_df = data[['Country or area', 'Indicator', 'Median estimate and uncertainty intervals', 'DataValue', 'Year']]
    data_df = data_df[data_df['Median estimate and uncertainty intervals'] == 'MEDIAN ESTIMATE (adjusted)']
    data_df = data_df[['Country or area', 'Year', 'Indicator', 'DataValue']].copy()
    data_df['concept'] = data_df['Indicator'].map(lambda x: indicator_mapping[x]['indicator'])
    data_df['method'] = data_df['Indicator'].map(lambda x: indicator_mapping[x]['method'])

    data_df.columns = ['country', 'year', 'i', 'val', 'concept', 'method']

    data_df = data_df[['country', 'year', 'concept', 'method', 'val']]

    gs = data_df.groupby('concept')

    for c, df_ in gs:
        c_id = to_concept_id(c)
        df = df_.copy()
        df = df.drop('concept', axis=1)
        df['country'] = df['country'].map(to_concept_id)
        df['method'] = df['method'].map(method_mapping)
        df.columns = ['country', 'year', 'method', c_id]
        df = df[['country', 'method', 'year', c_id]]

        if df['method'].dropna().empty:
            df = df.drop('method', axis=1)
            df.to_csv('../../ddf--datapoints--{}--by--country--year.csv'.format(c_id), index=False)
        else:
            df.to_csv('../../ddf--datapoints--{}--by--country--method--year.csv'.format(c_id), index=False)


def main():
    # generate mappings
    c1 = 'Contraceptive prevalence (Percentage)'
    c2 = 'Unmet need (Percentage)'
    c3 = 'Total demand (Percentage)'
    c4 = 'Demand satisfied (Percentage)'
    c5 = 'Contraceptive prevalence (Number)'
    c6 = 'Unmet need (Number)'

    m1 = 'Any method'
    m2 = 'traditional method'
    m3 = 'modern method'

    method_mapping = {m1: 'any', m2: 'traditional', m3: 'modern'}  # method name -> method entity

    # indicator name used in source file -> indicator concept name and method
    # because we will create multidimensional indicators
    indicator_mapping = {
        "Contraceptive prevalence: Any method (Percentage)" : d(c1, m1),
        "Contraceptive prevalence: Any modern method (Percentage)" : d(c1, m3),
        "Contraceptive prevalence: Any traditional method (Percentage)" : d(c1, m2),
        "Unmet need for family planning: Any method (Percentage)" : d(c2, m1),
        "Unmet need for family planning: Any modern method (Percentage)" : d(c2, m3),
        "Total demand for family planning (Percentage)" : d(c3, None),
        "Demand for family planning satisfied by any method (Percentage)" : d(c4, m1),
        "Demand for family planning satisfied by any modern method (Percentage)" : d(c4, m3),
        "Contraceptive prevalence: Any method (Number)" : d(c5, m1),
        "Contraceptive prevalence: Any modern method (Number)" : d(c5, m3),
        "Unmet need for family planning: Any method (Number)" : d(c6, m1),
        "Unmet need for family planning: Any modern method (Number)" : d(c6, m3)
    }

    # processing FP Indicators (Percentage) sheet
    data1 = pd.read_excel(source, sheet_name='FP Indicators (Percentage)', skiprows=3)
    create_datapoints(data1, indicator_mapping, method_mapping)
    # processing FP Indicators (Number) sheet
    data2 = pd.read_excel(source, sheet_name='FP Indicators (Number)', skiprows=3)
    create_datapoints(data2, indicator_mapping, method_mapping)

    # entities
    methods = pd.DataFrame({'method': list(method_mapping.values()), 'name': list(method_mapping.keys())})
    methods.to_csv('../../ddf--entities--method.csv', index=False)

    countries = data1['Country or area'].unique()
    countries = pd.DataFrame({'country': list(map(to_concept_id, countries)), 'name': countries})
    countries.to_csv('../../ddf--entities--country.csv', index=False)

    # concepts
    measures = [c1, c2, c3, c4, c5, c6]
    measures = pd.DataFrame({'concept': list(map(to_concept_id, measures)), 'name': measures})
    measures['concept_type'] = 'measure'
    discrete = pd.DataFrame([
        ['year', 'Year', 'time'],
        ['name', 'Name', 'string'],
        ['method', 'Contraception method', 'entity_domain'],
        ['country', 'Country', 'entity_domain']
    ], columns=['concept', 'name', 'concept_type'])
    cdf = pd.concat([measures, discrete], ignore_index=True)
    cdf.to_csv('../../ddf--concepts.csv', index=False)

    print('Done.')


if __name__ == '__main__':
    main()
