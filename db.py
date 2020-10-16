import ast
import json
import os
import subprocess
import time

import requests
from clickpanda import read_clickhouse

CREDENTIALS = {
    'server': os.environ['DB_HOST'],
    'port': os.environ['DB_PORT'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD']
}


def get_config(model_name, experiment_name):
    model_name = model_name.split('_')[0] + '_' + '30403'  # temporary fix
    url = 'http://{server}:{port}/?user={user}&password={password}'.format(**CREDENTIALS)
    query = open('queries/get_brute_config.sql').read().format(oracle_name=model_name,
                                                               experiment_name=experiment_name)

    config = requests.get(url, params='query=' + query).json()
    return config


def save_bf_points(oracle_name, experiment_name, points):
    print('---- Saving points for BF to DB ----')
    url = 'http://{server}:{port}/?user={user}&password={password}'.format(**CREDENTIALS)

    def is_unique_oracle_experiment():
        query = """SELECT * FROM db_bruteforce.tbl_points
                   WHERE oracle_name = '{oracle_name}'
                     AND experiment_name = '{experiment_name}'""".format(oracle_name=oracle_name,
                                                                         experiment_name=experiment_name)

        res = requests.get(url, params='query=' + query).text
        is_unique = True if (res == '') else False
        return is_unique

    if is_unique_oracle_experiment():
        values = "('{oracle_name}', '{experiment_name}', '{points}')"
        bash_cmd = 'echo """INSERT INTO db_bruteforce.tbl_points (oracle_name, experiment_name, points) ' \
                   'VALUES' + values.format(oracle_name=oracle_name,
                                            experiment_name=experiment_name,
                                            points=json.dumps(points).replace('"', '\\"')) + \
                   '""" | curl """' + url + '""" --data-binary @-'
        # print(bash_cmd)
        subprocess.run(bash_cmd, shell=True)
    else:
        raise Exception('Pair {oracle_name}, {experiment_name} is NOT unique!'.format(oracle_name=oracle_name,
                                                                                      experiment_name=experiment_name))


def get_oracle_predictions(date_start, date_end, model_name):
    url = 'http://{server}:{port}/?user={user}&password={password}'.format(**CREDENTIALS)
    ts_start = int(date_start)
    ts_end = int(date_end)

    query = open('queries/get_oracle_preds.sql').read().format(database='db_binance',
                                                               model_name=model_name,
                                                               ts_start=ts_start,
                                                               ts_finish=ts_end)
    try:
        preds = read_clickhouse(query, connection={'host': url})
        preds = preds.set_index('date').rename(columns={'price_del': 'y_pred'})
    except Exception as e:
        print("No DB with this oracle!")
        raise e

    return preds


def get_brute_results(model_name, experiment_name):
    url = 'http://{server}:{port}/?user={user}&password={password}'.format(**CREDENTIALS)
    query = open('queries/get_brute_results.sql').read().format(oracle_name=model_name,
                                                                experiment_name=experiment_name)

    bf_results = requests.get(url, params='query=' + query)
    bf_results = ast.literal_eval(bf_results.text)

    return bf_results


def save_strategies(starts, config):
    print('---- Saving best strats to DB ----')
    url = 'http://{server}:{port}/?user={user}&password={password}'.format(**CREDENTIALS)

    for index, strat in starts.iterrows():
        values = "('{oracle_name}', ' ', {ID}, {p1}, {p2}, {p3}, {p4}, {p5}, {p6}, {p7}, {p8}, {p9}, {p10}, {k}, " \
                 "{dyn_rev_regr}, {hist_features}, {date_start_test}, {date_end_test}, {ranges}, '{description}')"
        ranges = [config['INTERVALS']['1'], config['INTERVALS']['2'], config['INTERVALS']['3'],
                  config['INTERVALS']['work']]
        bash_cmd = 'echo "INSERT INTO db_strat_params.strategies VALUES ' + \
                   values.format(oracle_name=strat['model_name'],
                                 description=config['BASE'] + '_' + config['QUOTE'] + '_' + 'pipeline',
                                 ID=index if index != 0 else 1000,
                                 p1=strat['p1'], p2=strat['p2'], p3=strat['p3'], p4=strat['p4'], p5=strat['p5'],
                                 p6=strat['p6'], p7=strat['p7'], p8=strat['p8'], p9=strat['p9'], p10=strat['p10'],
                                 date_start_test=config['INTERVALS']['1'][0],
                                 date_end_test=config['INTERVALS']['3'][1],
                                 ranges=ranges,
                                 k=1, dyn_rev_regr=1, hist_features=0) + \
                   '" | curl "' + url + '" --data-binary @-'
        # print(bash_cmd)
        subprocess.run(bash_cmd, shell=True)

        values = "('{oracle_name}', {ID}, {rev_regr_exp_time}, {acc_filter}, {acc_lvl}, '{acc_field}', {dyn_tpsl}, " \
                 "{dyn_tpsl_tp}, {dyn_tpsl_sl}, '{dyn_tpsl_field}', {ld_lim}, {ld_lim_red}, {ld_lim_green}, " \
                 "{cncl_delay_maker_min}, {cncl_delay_taker_min}, '{close_cmd}', {close_cmd_time}, " \
                 "{close_cmd_tp_exceeding}, {close_cmd_tq_lvl}, {close_cmd_tq}, {close_time_prolong}, '{oracle_zero}')"

        bash_cmd = 'echo "INSERT INTO db_strat_params.envs VALUES ' + \
                   values.format(oracle_name=strat['model_name'],
                                 ID=index,
                                 rev_regr_exp_time=strat['reg_rev_expired_time'],
                                 acc_filter=strat['accuracy_filter'],
                                 acc_lvl=strat['accuracy_level'],
                                 acc_field=strat['accuracy_field'],
                                 dyn_tpsl=strat['volatility_tpsl_filter'],
                                 dyn_tpsl_tp=strat['volatility_tp_level'],
                                 dyn_tpsl_sl=strat['volatility_sl_level'],
                                 dyn_tpsl_field=strat['volatility_tpsl_field'],
                                 ld_lim=config.get('LEARN_DISTANCE_LIMIT', 5),
                                 ld_lim_red=config.get('LEARN_DISTANCE_LIMIT_RED', 5),
                                 ld_lim_green=config.get('LEARN_DISTANCE_LIMIT_GREEN', 5),
                                 cncl_delay_maker_min=config.get('CANCEL_DELAY_MAKER_MIN', 1),
                                 cncl_delay_taker_min=config.get('CANCEL_DELAY_TAKER_MIN', 1),
                                 close_cmd=config.get('CLOSE_COMMAND', ' '),
                                 close_cmd_time=config.get('CLOSE_COMMAND_TIME', 28800),
                                 close_cmd_tp_exceeding=config.get('CLOSE_COMMAND_TP_EXCEEDING', 0),
                                 close_cmd_tq_lvl=config.get('CLOSE_COMMAND_TQ_LEVEL', 0.1),
                                 close_cmd_tq=config.get('CLOSE_COMMAND_TQ', 0),
                                 close_time_prolong=config.get('CLOSE_TIME_PROLONG_ENABLE', 1),
                                 oracle_zero=config.get('ORACLE_ZERO', '2500_0')) + \
                   '" | curl "' + url + '" --data-binary @-'
        # print(bash_cmd)
        subprocess.run(bash_cmd, shell=True)
    print('------------- DONE ---------------')


def add_zero_oracle(model_name, config):
    print('---- Adding zero oracle to DB ----')
    url = 'http://{server}:{port}/?user={user}&password={password}'.format(**CREDENTIALS)

    def is_unique_zero_oracle():
        is_unique = False
        query_strats = """SELECT * FROM db_strat_params.strategies
                   WHERE oracle_name = '{oracle_name}'
                     AND ID = 0""".format(oracle_name=model_name)

        res = requests.get(url, params='query=' + query_strats).text
        is_unique_strats = True if (res == '') else False

        query_envs = """SELECT * FROM db_strat_params.envs
                           WHERE oracle_name = '{oracle_name}'
                             AND ID = 0""".format(oracle_name=model_name)
        res = requests.get(url, params='query=' + query_envs).text
        is_unique_envs = True if (res == '') else False

        if is_unique_strats and is_unique_envs:
            is_unique = True
        return is_unique

    if is_unique_zero_oracle():
        values = "('{oracle_name}', ' ', {ID}, {p1}, {p2}, {p3}, {p4}, {p5}, {p6}, {p7}, {p8}, {p9}, {p10}, {k}, " \
                 "{dyn_rev_regr}, {hist_features}, {date_start_test}, {date_end_test}, {ranges}, '{description}')"
        ranges = [config['INTERVALS']['1'], config['INTERVALS']['2'], config['INTERVALS']['3'],
                  config['INTERVALS']['work']]
        bash_cmd =/*
------------- UNDER NDA -----------------------
*/

                   '" | curl "' + url + '" --data-binary @-'
        # print(bash_cmd)
        subprocess.run(bash_cmd, shell=True)

        values = /*
------------- UNDER NDA -----------------------
*/

        bash_cmd = 'echo "INSERT INTO db_strat_params.envs VALUES ' + \
                  /*
------------- UNDER NDA -----------------------
*/

        # print(bash_cmd)
        subprocess.run(bash_cmd, shell=True)
    else:
        print('---- Zero oracle is already added ----')


def save_pnl_container_config(container_config):
    url = 'http://{server}:{port}/?user={user}&password={password}'.format(**CREDENTIALS)
    values = "({ts}, '{namespace}', '{config}')"

    bash_cmd = 'echo """INSERT INTO db_deploy_configs.configs (ts, namespace, config) ' \
               'VALUES' + values.format(ts=int(time.time()),
                                        namespace='thecabal-pnl2',
                                        config=json.dumps(container_config).replace('"', '\\"')) + \
               '""" | curl """' + url + '""" --data-binary @-'
    # print(bash_cmd)
    subprocess.run(bash_cmd, shell=True)
