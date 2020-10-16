import operator
from io import StringIO

import numpy as np
import pandas as pd

import db


def count_model_quality(bf_results):
    win_percentages = []
    merge_columns = ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8', 'p9', 'p10',
                     'reg_rev_expired_time', 'accuracy_filter', 'accuracy_level',
                     'accuracy_field', 'volatility_tpsl_filter', 'volatility_tp_level',
                     'volatility_sl_level', 'volatility_tpsl_field']

    for i, result in enumerate(bf_results):
        df_result = pd.read_table(StringIO(result), sep=',')
        wins_amount = len(df_result[df_result['pnl'] > 0])
        loses_amount = len(df_result[df_result['pnl'] < 0])
        win_percentage = wins_amount / (wins_amount + loses_amount)
        win_percentages.append(win_percentage)

    quality = np.prod(win_percentages)

    def merge_intervals(results):/*
------------- UNDER NDA -----------------------
*/

        return df

    df_result = merge_intervals(bf_results)
    df_result
    /*
------------- UNDER NDA -----------------------
*/

    df_result = df_result.sort_values(by=['quality_r'], ascending=False)
    df_result.index = df_result.index + 1
    res = (quality, df_result)

    return res


def select_best_strategies(results, top_oracles_amount=3, base_oracles_amount=4,
                           top_strat_threshold=0.60, base_strat_threshold=0.55):
    df = pd.DataFrame()
    results.sort(key=operator.itemgetter(1), reverse=True)

    base_results = results[top_oracles_amount:][:base_oracles_amount]
    for res in base_results:
/*
------------- UNDER NDA -----------------------
*/
            df = df.append(strategy)

    top_results = results[:top_oracles_amount]
/*
------------- UNDER NDA -----------------------
*/

    if df.empty:
        print('ZERO strats with Quality R >', top_strat_threshold)
    else:
        print('Best strats:')
        print(df[['model_name', 'quality_r']])

    return df


def start(model_names, experiment_name):
    results = []
    for model_name in model_names:
        try:
            bf_results = db.get_brute_results(model_name, experiment_name)
            res = count_model_quality(bf_results)
            res = (model_name,) + res
            results.append(res)
        except Exception as e:
            print('FAILED getting brute results')
            print(e)

    best_strats = select_best_strategies(results)

    return best_strats

