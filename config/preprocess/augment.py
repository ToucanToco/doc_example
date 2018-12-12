import re
import pandas as pd
from toucan_data_sdk.utils.generic import (
    compute_cumsum, compute_evolution_by_frequency
)

# columns
BRAND = 'brand'
BRAND_CLASS = 'brand_class'
COUNTRY = 'country'
PRODUCT_LINE = 'product_line'
DATE = 'date'
YEAR = 'year'
MONTH = 'month'
PERIOD_TYPE = 'period_type'
VALUE = 'value'
EVOLUTION = 'evolution'


def clean_mapping(mapping):
    # Replace brand names
    columns = {
        'brand': BRAND,
        'brand_class': BRAND_CLASS
    }
    mapping = mapping[list(columns.keys())]
    mapping = mapping.rename(columns=columns)

    to_replace = {
        'Fake Lux.': "Fake Luxury",
        'Golden Fragrances': 'Golden Fragrance',
        'Luxury Cpy': 'Luxury Company'
    }
    mapping[BRAND] = mapping[BRAND].replace(to_replace)

    # Convert brand names to uppercase
    mapping[BRAND] = mapping[BRAND].str.upper()

    return mapping


def clean_data(data):
    columns = {
        'brand': BRAND,
        'product_line': PRODUCT_LINE,
        'Country': COUNTRY
    }
    data = data.rename(columns=columns)

    # Convert multiple date columns to one column
    col_dates = [col for col in data.columns if
                 re.match(r'^\w{3}\ \d{4}$', col)]
    data = pd.melt(
        data,
        id_vars=[BRAND, PRODUCT_LINE, COUNTRY],
        value_vars=col_dates,
        var_name=DATE
    )
    data[DATE] = pd.to_datetime(data[DATE], format='%b %Y')
    data[YEAR] = data[DATE].dt.year
    data[MONTH] = data[DATE].dt.month
    return data[[BRAND, PRODUCT_LINE, COUNTRY, DATE, YEAR, MONTH, VALUE]]


def prepare_data(data, mapping):
    mapping = clean_mapping(mapping)
    data = clean_data(data)

    data = data.merge(mapping, on=BRAND, how='left')

    # Add cumul by year
    data_cum = compute_cumsum(
        data,
        id_cols=[YEAR, BRAND, BRAND_CLASS, PRODUCT_LINE, COUNTRY],
        reference_cols=[DATE, MONTH],
        value_cols=[VALUE]
    )
    data_cum['period_type'] = 'cumul'
    data['period_type'] = 'current'
    data = pd.concat([data_cum, data], sort=False).reset_index(drop=True)
    return data


def compute_data_by_country(data):
    # Group by country
    df = data.groupby(
        [COUNTRY, DATE, YEAR, MONTH, PERIOD_TYPE]
    )[VALUE].sum().reset_index()

    # add evolution (vs previous year)
    df[EVOLUTION] = compute_evolution_by_frequency(
        df,
        id_cols=[COUNTRY, PERIOD_TYPE, MONTH],
        date_col=YEAR,
        value_col=VALUE,
        freq=1,
        method='pct',
        format='column',
    )

    return df


def compute_data_perfume_by_brand(data):
    # Keep rows where product_line is PERFUME
    df = data[data[PRODUCT_LINE] == 'PERFUME']

    # Group by brand
    df = df.groupby(
        [BRAND, BRAND_CLASS, DATE, YEAR, MONTH, PERIOD_TYPE]
    )[VALUE].sum().reset_index()

    # Add evolution (vs previous year)
    df[EVOLUTION] = compute_evolution_by_frequency(
        df,
        id_cols=[BRAND, BRAND_CLASS, PERIOD_TYPE, MONTH],
        date_col=YEAR,
        value_col=VALUE,
        freq=1,
        method='pct',
        format='column',
    )
    return df
